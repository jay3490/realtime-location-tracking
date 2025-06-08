using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace Tracking.Infrastructure.Redis;

/// <summary>
/// The Redis key layout, in one place.
/// </summary>
public sealed class RedisKeys
{
    public RedisKeys(IOptions<RedisOptions> options)
        : this(options?.Value.KeyPrefix ?? throw new ArgumentNullException(nameof(options)))
    {
    }

    public RedisKeys(string prefix)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(prefix);
        Prefix = prefix;
        Pings = $"{prefix}:pings";
        DeadLetters = $"{prefix}:pings:dead";
        Positions = $"{prefix}:positions";
        Geofences = $"{prefix}:geofences";
        GeofencesVersion = $"{prefix}:geofences:version";
        AlertLog = $"{prefix}:alerts";
        AlertChannel = RedisChannel.Literal($"{prefix}:alerts");
        AnalyticsOutbox = $"{prefix}:analytics";
    }

    public string Prefix { get; }

    /// <summary>Stream of accepted pings (ingest writes, processors consume).</summary>
    public RedisKey Pings { get; }

    /// <summary>Stream of entries the processor could not decode.</summary>
    public RedisKey DeadLetters { get; }

    /// <summary>Geo sorted set of the latest position per device.</summary>
    public RedisKey Positions { get; }

    /// <summary>Hash of geofence id to JSON definition.</summary>
    public RedisKey Geofences { get; }

    public RedisKey GeofencesVersion { get; }

    /// <summary>Capped stream of committed alerts.</summary>
    public RedisKey AlertLog { get; }

    /// <summary>Pub/sub channel that live alerts are published on.</summary>
    public RedisChannel AlertChannel { get; }

    /// <summary>
    /// Stream of analytics events written in the same transaction as the state they describe, and
    /// relayed to Kafka from there.
    /// </summary>
    public RedisKey AnalyticsOutbox { get; }

    /// <summary>Hash holding the latest state of one device.</summary>
    public RedisKey Device(string deviceId) => $"{Prefix}:device:{deviceId}";
}
