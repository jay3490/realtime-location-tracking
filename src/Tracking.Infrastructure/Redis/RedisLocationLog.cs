using Microsoft.Extensions.Options;
using StackExchange.Redis;
using Tracking.Core.Abstractions;
using Tracking.Core.Devices;

namespace Tracking.Infrastructure.Redis;

/// <summary>Appends pings to the Redis stream with <c>XADD ... MAXLEN ~</c>.</summary>
public sealed class RedisLocationLog(IConnectionMultiplexer redis, RedisKeys keys, IOptions<RedisOptions> options) : ILocationLog
{
    private readonly int _maxLength = options.Value.PingStreamMaxLength;

    public async Task<string> AppendAsync(LocationPing ping, CancellationToken cancellationToken = default)
    {
        var fields = LocationPingCodec.Encode(ping)
            .Select(kv => new NameValueEntry(kv.Key, kv.Value))
            .ToArray();

        var id = await redis.GetDatabase()
            .StreamAddAsync(keys.Pings, fields, maxLength: _maxLength, useApproximateMaxLength: true)
            .ConfigureAwait(false);
        return id.ToString();
    }
}
