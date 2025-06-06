using Microsoft.Extensions.Options;
using StackExchange.Redis;
using Tracking.Core.Abstractions;

namespace Tracking.Infrastructure.Redis;

/// <summary>
/// Consumer-group reads over the ping stream: <c>XREADGROUP</c>, <c>XACK</c> and <c>XAUTOCLAIM</c>.
/// </summary>
/// <remarks>
/// StackExchange.Redis multiplexes every caller over one connection, so it does not issue blocking
/// commands; <c>XREADGROUP</c> is sent without <c>BLOCK</c> and the caller polls with a back-off when
/// a read comes back empty.
/// </remarks>
public sealed class RedisLocationConsumerGroup(IConnectionMultiplexer redis, RedisKeys keys, IOptions<RedisOptions> options)
    : ILocationConsumerGroup
{
    private readonly string _group = options.Value.ConsumerGroup;

    private IDatabase Db => redis.GetDatabase();

    public async Task EnsureConsumerGroupAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            // Start from the beginning so pings appended before the first processor started are not skipped.
            await Db.StreamCreateConsumerGroupAsync(keys.Pings, _group, StreamPosition.Beginning, createStream: true)
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);
        }
        catch (RedisServerException ex) when (ex.Message.StartsWith("BUSYGROUP", StringComparison.Ordinal))
        {
            // Another processor created it first.
        }
    }

    public async Task<IReadOnlyList<LogEntry>> ReadNewAsync(string consumer, int count, CancellationToken cancellationToken = default)
    {
        var entries = await Db.StreamReadGroupAsync(keys.Pings, _group, consumer, StreamPosition.NewMessages, count)
            .WaitAsync(cancellationToken)
            .ConfigureAwait(false);
        return Convert(entries);
    }

    public async Task<IReadOnlyList<LogEntry>> ReadOwnPendingAsync(string consumer, string afterId, int count, CancellationToken cancellationToken = default)
    {
        var entries = await Db.StreamReadGroupAsync(keys.Pings, _group, consumer, afterId, count)
            .WaitAsync(cancellationToken)
            .ConfigureAwait(false);
        return await WithDeliveryCountsAsync(consumer, Convert(entries), cancellationToken).ConfigureAwait(false);
    }

    public async Task<ClaimResult> ClaimAbandonedAsync(string consumer, TimeSpan minIdle, string startId, int count, CancellationToken cancellationToken = default)
    {
        var result = await Db.StreamAutoClaimAsync(keys.Pings, _group, consumer, (long)minIdle.TotalMilliseconds, startId, count)
            .WaitAsync(cancellationToken)
            .ConfigureAwait(false);

        if (result.DeletedIds.Length > 0)
        {
            // XAUTOCLAIM drops pending references to entries trimmed from the stream; nothing to process.
            await Db.StreamAcknowledgeAsync(keys.Pings, _group, result.DeletedIds).WaitAsync(cancellationToken).ConfigureAwait(false);
        }

        var claimed = await WithDeliveryCountsAsync(consumer, Convert(result.ClaimedEntries), cancellationToken).ConfigureAwait(false);
        return new ClaimResult(result.NextStartId.ToString(), claimed);
    }

    public Task AcknowledgeAsync(IReadOnlyCollection<string> entryIds, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(entryIds);
        if (entryIds.Count == 0)
        {
            return Task.CompletedTask;
        }

        var ids = entryIds.Select(id => (RedisValue)id).ToArray();
        return Db.StreamAcknowledgeAsync(keys.Pings, _group, ids).WaitAsync(cancellationToken);
    }

    public async Task<ConsumerGroupStatus> GetStatusAsync(CancellationToken cancellationToken = default)
    {
        var db = Db;
        if (!await db.KeyExistsAsync(keys.Pings).WaitAsync(cancellationToken).ConfigureAwait(false))
        {
            return ConsumerGroupStatus.Empty;
        }

        var length = db.StreamLengthAsync(keys.Pings);
        var groups = db.StreamGroupInfoAsync(keys.Pings);
        await Task.WhenAll(length, groups).WaitAsync(cancellationToken).ConfigureAwait(false);

        var group = groups.Result.FirstOrDefault(g => g.Name == _group);
        return group.Name is null
            ? new ConsumerGroupStatus(length.Result, length.Result, 0)
            : new ConsumerGroupStatus(length.Result, group.Lag, group.PendingMessageCount);
    }

    public async Task DeadLetterAsync(LogEntry entry, string reason, string consumer, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(entry);

        var fields = entry.Fields
            .Select(kv => new NameValueEntry(kv.Key, kv.Value))
            .Append(new NameValueEntry(DeadLetterFields.SourceId, entry.Id))
            .Append(new NameValueEntry(DeadLetterFields.Reason, reason))
            .Append(new NameValueEntry(DeadLetterFields.Deliveries, entry.DeliveryCount))
            .Append(new NameValueEntry(DeadLetterFields.Consumer, consumer))
            .ToArray();

        var transaction = Db.CreateTransaction();
        _ = transaction.StreamAddAsync(keys.DeadLetters, fields, maxLength: options.Value.DeadLetterMaxLength, useApproximateMaxLength: true);
        _ = transaction.StreamAcknowledgeAsync(keys.Pings, _group, entry.Id);
        await transaction.ExecuteAsync().WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Re-reading pending entries and claiming them both bump their delivery count, which only
    /// <c>XPENDING</c> reports; one extended <c>XPENDING</c> over the id range fills it in. This runs on
    /// the recovery paths only, never for new entries (whose count is 1 by definition).
    /// </summary>
    private async Task<IReadOnlyList<LogEntry>> WithDeliveryCountsAsync(string consumer, LogEntry[] entries, CancellationToken cancellationToken)
    {
        if (entries.Length == 0)
        {
            return entries;
        }

        var pending = await Db.StreamPendingMessagesAsync(keys.Pings, _group, entries.Length, consumer, entries[0].Id, entries[^1].Id)
            .WaitAsync(cancellationToken)
            .ConfigureAwait(false);
        var counts = pending.ToDictionary(p => p.MessageId.ToString(), p => p.DeliveryCount, StringComparer.Ordinal);

        return entries
            .Select(e => counts.TryGetValue(e.Id, out var count) ? e with { DeliveryCount = count } : e)
            .ToArray();
    }

    private static LogEntry[] Convert(IEnumerable<StackExchange.Redis.StreamEntry> entries) =>
        entries
            .Where(e => !e.IsNull)
            .Select(e => new LogEntry(
                e.Id.ToString(),
                e.Values.ToDictionary(v => v.Name.ToString(), v => v.Value.ToString(), StringComparer.Ordinal)))
            .ToArray();
}

/// <summary>Fields added to an entry when it is copied to the dead-letter stream.</summary>
public static class DeadLetterFields
{
    public const string SourceId = "dead.sourceId";
    public const string Reason = "dead.reason";
    public const string Deliveries = "dead.deliveries";
    public const string Consumer = "dead.consumer";
}
