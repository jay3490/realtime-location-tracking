using StackExchange.Redis;
using Tracking.Core.Abstractions;
using Tracking.Core.Geofencing;

namespace Tracking.Ingest.Services;

/// <summary>
/// Subscribes to the live alert feed and forwards each alert to the device's stream if that
/// device is connected to this instance. Every ingest replica receives every alert and simply
/// ignores the ones for devices it is not holding.
/// </summary>
public sealed partial class AlertRelay(IAlertFeed feed, DeviceSessionRegistry sessions, ILogger<AlertRelay> logger) : BackgroundService
{
    private static readonly TimeSpan RetryDelay = TimeSpan.FromSeconds(2);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        IAsyncDisposable? subscription = null;
        while (subscription is null && !stoppingToken.IsCancellationRequested)
        {
            try
            {
                subscription = await feed.SubscribeAsync(DeliverAsync, stoppingToken).ConfigureAwait(false);
                LogSubscribed();
            }
            catch (Exception ex) when (ex is RedisException or TimeoutException)
            {
                LogSubscribeFailed(ex, RetryDelay);
                await Task.Delay(RetryDelay, stoppingToken).ConfigureAwait(false);
            }
        }

        if (subscription is null)
        {
            return;
        }

        try
        {
            await Task.Delay(Timeout.Infinite, stoppingToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Shutting down.
        }
        finally
        {
            await subscription.DisposeAsync().ConfigureAwait(false);
        }
    }

    private Task DeliverAsync(GeofenceAlert alert)
    {
        if (sessions.TryDeliver(alert))
        {
            LogDelivered(alert.DeviceId, alert.GeofenceId, alert.Transition);
        }

        return Task.CompletedTask;
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Subscribed to the geofence alert feed")]
    private partial void LogSubscribed();

    [LoggerMessage(Level = LogLevel.Warning, Message = "Could not subscribe to the alert feed; retrying in {Delay}")]
    private partial void LogSubscribeFailed(Exception exception, TimeSpan delay);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Delivered {Transition} of {GeofenceId} to {DeviceId}")]
    private partial void LogDelivered(string deviceId, string geofenceId, GeofenceTransition transition);
}
