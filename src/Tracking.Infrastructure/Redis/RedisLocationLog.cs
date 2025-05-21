using Microsoft.Extensions.Options;
using StackExchange.Redis;
using Tracking.Core.Abstractions;
using Tracking.Core.Devices;

namespace Tracking.Infrastructure.Redis;

/// <summary>Appends pings to the Redis stream with <c>XADD</c>, trimming it with <c>MAXLEN</c>.</summary>
/// <remarks>
/// A batch is queued on one <see cref="IBatch"/>, so all of its <c>XADD</c>s leave in a single write
/// and come back in a single round trip, in order. Only the last command of a batch carries the
/// <c>MAXLEN</c> threshold: trimming once per batch keeps the stream at the same bound for a fraction
/// of the work.
/// </remarks>
public sealed class RedisLocationLog(IConnectionMultiplexer redis, RedisKeys keys, IOptions<RedisOptions> options) : ILocationLog
{
    private readonly int _maxLength = options.Value.PingStreamMaxLength;
    private readonly bool _approximate = options.Value.PingStreamTrimApproximate;

    public async Task<string> AppendAsync(LocationPing ping, CancellationToken cancellationToken = default)
    {
        var id = await redis.GetDatabase()
            .StreamAddAsync(keys.Pings, Encode(ping), maxLength: _maxLength, useApproximateMaxLength: _approximate)
            .WaitAsync(cancellationToken)
            .ConfigureAwait(false);
        return id.ToString();
    }

    public async Task<IReadOnlyList<string?>> AppendBatchAsync(IReadOnlyList<LocationPing> pings, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(pings);
        if (pings.Count == 0)
        {
            return [];
        }

        var batch = redis.GetDatabase().CreateBatch();
        var appends = new Task<RedisValue>[pings.Count];
        for (var i = 0; i < pings.Count; i++)
        {
            var last = i == pings.Count - 1;
            appends[i] = batch.StreamAddAsync(
                keys.Pings,
                Encode(pings[i]),
                maxLength: last ? _maxLength : null,
                useApproximateMaxLength: _approximate);
        }

        batch.Execute();

        try
        {
            await Task.WhenAll(appends).WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is RedisException or TimeoutException && appends.Any(a => a.IsCompletedSuccessfully))
        {
            // Some appends made it before the failure; report them individually below. When none did,
            // the filter does not match and the error itself propagates.
        }

        return appends.Select(a => a.IsCompletedSuccessfully ? a.Result.ToString() : null).ToArray();
    }

    private static NameValueEntry[] Encode(LocationPing ping) =>
        LocationPingCodec.Encode(ping)
            .Select(kv => new NameValueEntry(kv.Key, kv.Value))
            .ToArray();
}
