using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Tracking.Contracts;
using Tracking.Contracts.V1;

namespace Tracking.Simulator;

/// <summary>
/// Drives one simulated device over its own StreamLocations call: one task writes pings on a timer,
/// another reads acks and alerts. The call is reopened after failures.
/// </summary>
public sealed partial class DeviceStreamer(
    LocationIngest.LocationIngestClient client,
    SimulatedDevice device,
    TimeSpan interval,
    SimulatorStats stats,
    ILogger<DeviceStreamer> logger)
{
    private static readonly TimeSpan ReconnectDelay = TimeSpan.FromSeconds(2);
    private static readonly TimeSpan DrainTimeout = TimeSpan.FromSeconds(5);

    // An explicit field, because the logging source generator for .NET 8 does not see primary constructor parameters.
    private readonly ILogger<DeviceStreamer> _logger = logger;

    private ulong _sequence;

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await StreamOnceAsync(cancellationToken).ConfigureAwait(false);
                return;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (RpcException ex) when (ex.StatusCode == StatusCode.Cancelled && cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (RpcException ex)
            {
                stats.Reconnected();
                LogStreamFailed(device.Id, ex.StatusCode, ReconnectDelay);
                await Task.Delay(ReconnectDelay, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private async Task StreamOnceAsync(CancellationToken cancellationToken)
    {
        var headers = new Metadata { { ContractMapping.DeviceIdHeader, device.Id } };

        // The call has its own token so that stopping ends the writes gracefully and the acks for
        // pings already sent can still arrive; it is only cancelled if draining takes too long.
        using var drain = new CancellationTokenSource();
        using var call = client.StreamLocations(headers, cancellationToken: drain.Token);

        var reader = ReadResponsesAsync(call.ResponseStream, drain.Token);
        try
        {
            using var timer = new PeriodicTimer(interval);
            var lastTick = DateTimeOffset.UtcNow;
            do
            {
                var now = DateTimeOffset.UtcNow;
                var position = device.Advance(now - lastTick);
                lastTick = now;

                await call.RequestStream.WriteAsync(new LocationPing
                {
                    Sequence = ++_sequence,
                    Latitude = position.Latitude,
                    Longitude = position.Longitude,
                    RecordedAt = Timestamp.FromDateTimeOffset(now),
                    AccuracyMeters = 5,
                }, cancellationToken).ConfigureAwait(false);
                stats.PingSent();
            }
            while (await timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false));
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Stopping: close the request side and wait for the remaining acks.
        }

        await call.RequestStream.CompleteAsync().ConfigureAwait(false);
        drain.CancelAfter(DrainTimeout);
        await reader.ConfigureAwait(false);
    }

    private async Task ReadResponsesAsync(IAsyncStreamReader<ServerMessage> responses, CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var message in responses.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            {
                switch (message.PayloadCase)
                {
                    case ServerMessage.PayloadOneofCase.Ack:
                        stats.AckReceived(message.Ack.Accepted);
                        if (!message.Ack.Accepted)
                        {
                            LogRejected(device.Id, message.Ack.Sequence, message.Ack.Reason);
                        }

                        break;
                    case ServerMessage.PayloadOneofCase.Alert:
                        stats.AlertReceived();
                        LogAlert(device.Id, message.Alert.Transition, message.Alert.GeofenceName);
                        break;
                }
            }
        }
        catch (RpcException ex) when (ex.StatusCode == StatusCode.Cancelled && cancellationToken.IsCancellationRequested)
        {
            // Normal shutdown.
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Normal shutdown.
        }
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "{DeviceId}: stream failed with {Status}; reconnecting in {Delay}")]
    private partial void LogStreamFailed(string deviceId, StatusCode status, TimeSpan delay);

    [LoggerMessage(Level = LogLevel.Warning, Message = "{DeviceId}: ping {Sequence} rejected: {Reason}")]
    private partial void LogRejected(string deviceId, ulong sequence, string reason);

    [LoggerMessage(Level = LogLevel.Information, Message = "{DeviceId}: {Transition} {Geofence}")]
    private partial void LogAlert(string deviceId, GeofenceTransition transition, string geofence);
}
