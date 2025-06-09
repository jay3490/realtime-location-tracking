using Microsoft.Extensions.Options;
using StackExchange.Redis;
using Tracking.Core.Abstractions;

namespace Tracking.Infrastructure.Redis;

/// <summary>The analytics outbox stream read through its own consumer group (the relays).</summary>
public sealed class RedisAnalyticsOutbox(IConnectionMultiplexer redis, RedisKeys keys, IOptions<RedisOptions> options) : IAnalyticsOutbox
{
    private readonly string _group = options.Value.AnalyticsRelayGroup;

    private IDatabase Db => redis.GetDatabase();

    public async Task EnsureConsumerGroupAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            await Db.StreamCreateConsumerGroupAsync(keys.AnalyticsOutbox, _group, StreamPosition.Beginning, createStream: true)
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);
        }
        catch (RedisServerException ex) when (ex.Message.StartsWith("BUSYGROUP", StringComparison.Ordinal))
        {
            // Another relay created it first.
        }
    }

    public async Task<IReadOnlyList<LogEntry>> ReadNewAsync(string consumer, int count, CancellationToken cancellationToken = default) =>
        RedisLocationConsumerGroup.Convert(await Db.StreamReadGroupAsync(keys.AnalyticsOutbox, _group, consumer, StreamPosition.NewMessages, count)
            .WaitAsync(cancellationToken)
            .ConfigureAwait(false));

    public async Task<IReadOnlyList<LogEntry>> ReadOwnPendingAsync(string consumer, string afterId, int count, CancellationToken cancellationToken = default) =>
        RedisLocationConsumerGroup.Convert(await Db.StreamReadGroupAsync(keys.AnalyticsOutbox, _group, consumer, afterId, count)
            .WaitAsync(cancellationToken)
            .ConfigureAwait(false));

    public async Task<ClaimResult> ClaimAbandonedAsync(string consumer, TimeSpan minIdle, string startId, int count, CancellationToken cancellationToken = default)
    {
        var result = await Db.StreamAutoClaimAsync(keys.AnalyticsOutbox, _group, consumer, (long)minIdle.TotalMilliseconds, startId, count)
            .WaitAsync(cancellationToken)
            .ConfigureAwait(false);

        if (result.DeletedIds.Length > 0)
        {
            // Trimmed before they were relayed; nothing left to publish.
            await Db.StreamAcknowledgeAsync(keys.AnalyticsOutbox, _group, result.DeletedIds).WaitAsync(cancellationToken).ConfigureAwait(false);
        }

        return new ClaimResult(result.NextStartId.ToString(), RedisLocationConsumerGroup.Convert(result.ClaimedEntries));
    }

    public Task AcknowledgeAsync(IReadOnlyCollection<string> entryIds, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(entryIds);
        if (entryIds.Count == 0)
        {
            return Task.CompletedTask;
        }

        return Db.StreamAcknowledgeAsync(keys.AnalyticsOutbox, _group, entryIds.Select(id => (RedisValue)id).ToArray())
            .WaitAsync(cancellationToken);
    }
}
