using System.Text.Json;
using StackExchange.Redis;
using Tracking.Core;
using Tracking.Core.Abstractions;
using Tracking.Core.Geofencing;

namespace Tracking.Infrastructure.Redis;

/// <summary>
/// Geofences as JSON values in one hash, with a version counter bumped in the same transaction
/// as every change.
/// </summary>
public sealed class RedisGeofenceStore(IConnectionMultiplexer redis, RedisKeys keys) : IGeofenceStore
{
    private IDatabase Db => redis.GetDatabase();

    public async Task<IReadOnlyList<Geofence>> GetAllAsync(CancellationToken cancellationToken = default)
    {
        var entries = await Db.HashGetAllAsync(keys.Geofences).WaitAsync(cancellationToken).ConfigureAwait(false);
        return entries
            .Select(e => Deserialize(e.Value))
            .OrderBy(g => g.Id, StringComparer.Ordinal)
            .ToArray();
    }

    public async Task<Geofence?> GetAsync(string id, CancellationToken cancellationToken = default)
    {
        var value = await Db.HashGetAsync(keys.Geofences, id).WaitAsync(cancellationToken).ConfigureAwait(false);
        return value.IsNull ? null : Deserialize(value);
    }

    public async Task<bool> UpsertAsync(Geofence geofence, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(geofence);

        var transaction = Db.CreateTransaction();
        var created = transaction.HashSetAsync(keys.Geofences, geofence.Id, Serialize(geofence));
        _ = transaction.StringIncrementAsync(keys.GeofencesVersion);
        await transaction.ExecuteAsync().WaitAsync(cancellationToken).ConfigureAwait(false);
        return await created.ConfigureAwait(false);
    }

    public async Task<bool> TryCreateAsync(Geofence geofence, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(geofence);

        var transaction = Db.CreateTransaction();
        transaction.AddCondition(Condition.HashNotExists(keys.Geofences, geofence.Id));
        _ = transaction.HashSetAsync(keys.Geofences, geofence.Id, Serialize(geofence));
        _ = transaction.StringIncrementAsync(keys.GeofencesVersion);
        return await transaction.ExecuteAsync().WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<bool> DeleteAsync(string id, CancellationToken cancellationToken = default)
    {
        var transaction = Db.CreateTransaction();
        transaction.AddCondition(Condition.HashExists(keys.Geofences, id));
        _ = transaction.HashDeleteAsync(keys.Geofences, id);
        _ = transaction.StringIncrementAsync(keys.GeofencesVersion);
        return await transaction.ExecuteAsync().WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<long> GetVersionAsync(CancellationToken cancellationToken = default)
    {
        var value = await Db.StringGetAsync(keys.GeofencesVersion).WaitAsync(cancellationToken).ConfigureAwait(false);
        return value.IsNull ? 0 : (long)value;
    }

    private static string Serialize(Geofence geofence) => JsonSerializer.Serialize(geofence, TrackingJson.Options);

    private static Geofence Deserialize(RedisValue value) =>
        JsonSerializer.Deserialize<Geofence>(value.ToString(), TrackingJson.Options)
        ?? throw new InvalidOperationException("Stored geofence JSON was null.");
}
