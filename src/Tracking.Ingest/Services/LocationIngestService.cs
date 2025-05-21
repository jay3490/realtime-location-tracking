using System.Threading.Channels;
using Grpc.Core;
using Microsoft.Extensions.Options;
using Tracking.Contracts;
using Tracking.Contracts.V1;
using Tracking.Core.Devices;

namespace Tracking.Ingest.Services;

/// <summary>
/// gRPC endpoint for device location streams.
/// </summary>
/// <remarks>
/// <para>
/// Each call runs three loops connected by bounded channels:
/// a reader moves pings from the request stream into the inbound channel; the call itself takes
/// whatever pings are waiting, validates them, appends the valid ones through
/// <see cref="PingAppender"/> and queues one ack per ping, in order; and a pump writes the outbox to
/// the response stream, because gRPC does not allow concurrent writes and alerts from the Redis
/// subscription share that stream with the acks.
/// </para>
/// <para>
/// Every channel is bounded, so a slow log or a slow device stops the reader rather than growing
/// a buffer: the device is held back by HTTP/2 flow control.
/// </para>
/// </remarks>
public sealed partial class LocationIngestService(
    PingAppender appender,
    PingValidator validator,
    DeviceSessionRegistry sessions,
    IngestMetrics metrics,
    TimeProvider clock,
    IOptions<IngestOptions> options,
    ILogger<LocationIngestService> logger) : LocationIngest.LocationIngestBase
{
    private readonly IngestOptions _options = options.Value;

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
        var outbox = Channel.CreateBounded<ServerMessage>(new BoundedChannelOptions(_options.OutboxCapacity)
        {
            SingleReader = true,
            FullMode = BoundedChannelFullMode.Wait,
        });
        var inbound = Channel.CreateBounded<Contracts.V1.LocationPing>(new BoundedChannelOptions(_options.InboundCapacity)
        {
            SingleReader = true,
            SingleWriter = true,
            FullMode = BoundedChannelFullMode.Wait,
        });

        LogStreamOpened(deviceId);
        metrics.StreamOpened();
        var pump = PumpAsync(outbox.Reader, responseStream, cancellationToken);
        var session = sessions.Register(deviceId, outbox.Writer);
        var reader = ReadRequestsAsync(requestStream, inbound.Writer, cancellationToken);
        long received = 0;
        try
        {
            received = await AcknowledgeAsync(deviceId, inbound.Reader, outbox.Writer, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            session.Dispose();
            outbox.Writer.TryComplete();
            await pump.ConfigureAwait(false);
            await reader.ConfigureAwait(false);
            metrics.StreamClosed();
            LogStreamClosed(deviceId, received);
        }
    }

    /// <summary>
    /// Copies the request stream into <paramref name="inbound"/>, waiting whenever it is full. Errors
    /// complete the channel so the acknowledging loop sees them; this task itself never faults.
    /// </summary>
    private static async Task ReadRequestsAsync(
        IAsyncStreamReader<Contracts.V1.LocationPing> requestStream,
        ChannelWriter<Contracts.V1.LocationPing> inbound,
        CancellationToken cancellationToken)
    {
        try
        {
            while (await requestStream.MoveNext(cancellationToken).ConfigureAwait(false))
            {
                await inbound.WriteAsync(requestStream.Current, cancellationToken).ConfigureAwait(false);
            }

            inbound.TryComplete();
        }
        catch (Exception ex)
        {
            inbound.TryComplete(ex);
        }
    }

    /// <summary>Appends and acknowledges pings as they arrive. Returns how many pings were handled.</summary>
    private async Task<long> AcknowledgeAsync(
        string deviceId,
        ChannelReader<Contracts.V1.LocationPing> inbound,
        ChannelWriter<ServerMessage> outbox,
        CancellationToken cancellationToken)
    {
        long handled = 0;
        var batch = new List<Contracts.V1.LocationPing>(_options.InboundCapacity);
        while (await inbound.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
        {
            batch.Clear();
            while (batch.Count < _options.InboundCapacity && inbound.TryRead(out var message))
            {
                batch.Add(message);
            }

            foreach (var ack in await HandleBatchAsync(deviceId, batch, cancellationToken).ConfigureAwait(false))
            {
                await outbox.WriteAsync(new ServerMessage { Ack = ack }, cancellationToken).ConfigureAwait(false);
            }

            handled += batch.Count;
        }

        return handled;
    }

    private async Task<PingAck[]> HandleBatchAsync(string deviceId, List<Contracts.V1.LocationPing> messages, CancellationToken cancellationToken)
    {
        var receivedAt = clock.GetUtcNow();
        var acks = new PingAck[messages.Count];
        var accepted = new List<(int Index, Core.Devices.LocationPing Ping)>(messages.Count);

        for (var i = 0; i < messages.Count; i++)
        {
            var message = messages[i];
            var ping = message.ToDomain(deviceId, receivedAt);
            if (validator.Validate(ping) is { } rejection)
            {
                metrics.Rejected();
                LogPingRejected(deviceId, message.Sequence, rejection);
                acks[i] = new PingAck { Sequence = message.Sequence, Accepted = false, Reason = rejection };
            }
            else
            {
                accepted.Add((i, ping));
            }
        }

        if (accepted.Count == 0)
        {
            return acks;
        }

        var ids = await appender.AppendAsync(accepted.ConvertAll(a => a.Ping), cancellationToken).ConfigureAwait(false);
        for (var j = 0; j < accepted.Count; j++)
        {
            var index = accepted[j].Index;
            var sequence = messages[index].Sequence;
            if (ids[j] is { } entryId)
            {
                metrics.Accepted();
                acks[index] = new PingAck { Sequence = sequence, Accepted = true, EntryId = entryId };
            }
            else
            {
                metrics.Unavailable();
                LogAppendFailed(deviceId, sequence);
                acks[index] = new PingAck
                {
                    Sequence = sequence,
                    Accepted = false,
                    Retryable = true,
                    Reason = "location log unavailable",
                };
            }
        }

        return acks;
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

    [LoggerMessage(Level = LogLevel.Warning, Message = "Could not append ping {Sequence} from {DeviceId}; acknowledged as retryable")]
    private partial void LogAppendFailed(string deviceId, ulong sequence);
}
