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
        return Convert(entries);
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

        return new ClaimResult(result.NextStartId.ToString(), Convert(result.ClaimedEntries));
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

    public async Task DeadLetterAsync(LogEntry entry, string reason, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(entry);

        var fields = entry.Fields
            .Select(kv => new NameValueEntry(kv.Key, kv.Value))
            .Append(new NameValueEntry("dead.sourceId", entry.Id))
            .Append(new NameValueEntry("dead.reason", reason))
            .ToArray();

        var transaction = Db.CreateTransaction();
        _ = transaction.StreamAddAsync(keys.DeadLetters, fields, maxLength: options.Value.AlertLogMaxLength, useApproximateMaxLength: true);
        _ = transaction.StreamAcknowledgeAsync(keys.Pings, _group, entry.Id);
        await transaction.ExecuteAsync().WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    private static LogEntry[] Convert(IEnumerable<StackExchange.Redis.StreamEntry> entries) =>
        entries
            .Where(e => !e.IsNull)
            .Select(e => new LogEntry(
                e.Id.ToString(),
                e.Values.ToDictionary(v => v.Name.ToString(), v => v.Value.ToString(), StringComparer.Ordinal)))
            .ToArray();
}
