using System.Threading.Channels;
using Grpc.Core;
using Microsoft.Extensions.Options;
using StackExchange.Redis;
using Tracking.Contracts;
using Tracking.Contracts.V1;
using Tracking.Core.Abstractions;
using Tracking.Core.Devices;

namespace Tracking.Ingest.Services;

/// <summary>
/// gRPC endpoint for device location streams.
/// </summary>
/// <remarks>
/// Each call gets a bounded outbox channel with a single pump task writing to the response stream,
/// because gRPC does not allow concurrent writes and both acks (from the read loop) and alerts
/// (from the Redis subscription) need to reach the same stream.
/// </remarks>
public sealed partial class LocationIngestService(
    ILocationLog log,
    PingValidator validator,
    DeviceSessionRegistry sessions,
    IngestMetrics metrics,
    TimeProvider clock,
    IOptions<IngestOptions> options,
    ILogger<LocationIngestService> logger) : LocationIngest.LocationIngestBase
{
    public override async Task StreamLocations(
        IAsyncStreamReader<Contracts.V1.LocationPing> requestStream,
        IServerStreamWriter<ServerMessage> responseStream,
        ServerCallContext context)
    {
        ArgumentNullException.ThrowIfNull(requestStream);
        ArgumentNullException.ThrowIfNull(responseStream);
        ArgumentNullException.ThrowIfNull(context);

        var deviceId = context.RequestHeaders.GetValue(ContractMapping.DeviceIdHeader);
        if (!DeviceId.IsValid(deviceId))
        {
            throw new RpcException(new Status(
                StatusCode.InvalidArgument,
                $"The '{ContractMapping.DeviceIdHeader}' header must be 1-{DeviceId.MaxLength} characters of letters, digits, '.', ':', '_' or '-'."));
        }

        var cancellationToken = context.CancellationToken;
        var outbox = Channel.CreateBounded<ServerMessage>(new BoundedChannelOptions(options.Value.OutboxCapacity)
        {
            SingleReader = true,
            FullMode = BoundedChannelFullMode.Wait,
        });

        LogStreamOpened(deviceId);
        metrics.StreamOpened();
        var pump = PumpAsync(outbox.Reader, responseStream, cancellationToken);
        var session = sessions.Register(deviceId, outbox.Writer);
        long received = 0;
        try
        {
            await foreach (var message in requestStream.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            {
                received++;
                var ack = await HandlePingAsync(deviceId, message, cancellationToken).ConfigureAwait(false);
                await outbox.Writer.WriteAsync(new ServerMessage { Ack = ack }, cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            session.Dispose();
            outbox.Writer.TryComplete();
            await pump.ConfigureAwait(false);
            metrics.StreamClosed();
            LogStreamClosed(deviceId, received);
        }
    }

    private async Task<PingAck> HandlePingAsync(string deviceId, Contracts.V1.LocationPing message, CancellationToken cancellationToken)
    {
        var ping = message.ToDomain(deviceId, clock.GetUtcNow());
        var rejection = validator.Validate(ping);
        if (rejection is not null)
        {
            metrics.Rejected();
            LogPingRejected(deviceId, message.Sequence, rejection);
            return new PingAck { Sequence = message.Sequence, Accepted = false, Reason = rejection };
        }

        try
        {
            var entryId = await log.AppendAsync(ping, cancellationToken).ConfigureAwait(false);
            metrics.Accepted();
            return new PingAck { Sequence = message.Sequence, Accepted = true, EntryId = entryId };
        }
        catch (Exception ex) when (ex is RedisException or TimeoutException)
        {
            metrics.Unavailable();
            LogAppendFailed(ex, deviceId, message.Sequence);
            return new PingAck
            {
                Sequence = message.Sequence,
                Accepted = false,
                Retryable = true,
                Reason = "location log unavailable",
            };
        }
    }

    private static async Task PumpAsync(ChannelReader<ServerMessage> outbox, IServerStreamWriter<ServerMessage> responseStream, CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var message in outbox.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            {
                await responseStream.WriteAsync(message, cancellationToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // The client went away; nothing left to deliver to.
        }
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Location stream opened for {DeviceId}")]
    private partial void LogStreamOpened(string deviceId);

    [LoggerMessage(Level = LogLevel.Information, Message = "Location stream closed for {DeviceId} after {Received} pings")]
    private partial void LogStreamClosed(string deviceId, long received);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Rejected ping {Sequence} from {DeviceId}: {Reason}")]
    private partial void LogPingRejected(string deviceId, ulong sequence, string reason);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Could not append ping {Sequence} from {DeviceId}")]
    private partial void LogAppendFailed(Exception exception, string deviceId, ulong sequence);
}
