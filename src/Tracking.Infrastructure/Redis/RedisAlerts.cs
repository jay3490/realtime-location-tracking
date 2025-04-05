using System.Text.Json;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using Tracking.Core;
using Tracking.Core.Abstractions;
using Tracking.Core.Geofencing;

namespace Tracking.Infrastructure.Redis;

/// <summary>
/// Live alerts over Redis pub/sub. Alerts are published by <see cref="RedisDeviceStateStore"/> inside
/// the commit transaction; this class only subscribes.
/// </summary>
public sealed partial class RedisAlertFeed(IConnectionMultiplexer redis, RedisKeys keys, ILogger<RedisAlertFeed> logger) : IAlertFeed
{
    public async Task<IAsyncDisposable> SubscribeAsync(Func<GeofenceAlert, Task> handler, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(handler);

        // A ChannelMessageQueue delivers messages sequentially, in the order Redis sent them.
        var queue = await redis.GetSubscriber().SubscribeAsync(keys.AlertChannel).WaitAsync(cancellationToken).ConfigureAwait(false);
        queue.OnMessage(async message =>
        {
            GeofenceAlert? alert;
            try
            {
                alert = JsonSerializer.Deserialize<GeofenceAlert>(message.Message.ToString(), TrackingJson.Options);
            }
            catch (JsonException ex)
            {
                LogMalformedAlert(ex);
                return;
            }

            if (alert is not null)
            {
                await handler(alert).ConfigureAwait(false);
            }
        });

        return new Subscription(queue);
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "Ignoring an alert message that is not valid JSON")]
    private partial void LogMalformedAlert(Exception exception);

    private sealed class Subscription(ChannelMessageQueue queue) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync() => await queue.UnsubscribeAsync().ConfigureAwait(false);
    }
}

/// <summary>Reads the capped alert stream newest first with <c>XREVRANGE</c>.</summary>
public sealed class RedisAlertLog(IConnectionMultiplexer redis, RedisKeys keys) : IAlertLog
{
    public async Task<IReadOnlyList<GeofenceAlert>> GetRecentAsync(int count, CancellationToken cancellationToken = default)
    {
        var entries = await redis.GetDatabase()
            .StreamRangeAsync(keys.AlertLog, "-", "+", count, Order.Descending)
            .WaitAsync(cancellationToken)
            .ConfigureAwait(false);

        return entries
            .Select(e => e["alert"])
            .Where(v => !v.IsNullOrEmpty)
            .Select(v => JsonSerializer.Deserialize<GeofenceAlert>(v.ToString(), TrackingJson.Options))
            .OfType<GeofenceAlert>()
            .ToArray();
    }
}
