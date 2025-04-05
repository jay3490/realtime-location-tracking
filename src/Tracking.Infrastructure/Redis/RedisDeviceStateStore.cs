using System.Globalization;
using System.Text.Json;
using Microsoft.Extensions.Options;
using StackExchange.Redis;
using Tracking.Core;
using Tracking.Core.Abstractions;
using Tracking.Core.Devices;
using Tracking.Core.Geo;
using Tracking.Core.Geofencing;

namespace Tracking.Infrastructure.Redis;

/// <summary>
/// Device state in one hash per device plus a geo set over the latest positions.
/// </summary>
/// <remarks>
/// A commit is a single <c>MULTI</c>/<c>EXEC</c> guarded by a <c>WATCH</c>-style condition on the stored
/// timestamp: the hash, the <c>GEOADD</c>, the alert log entries and the alert <c>PUBLISH</c> all land
/// together or not at all. Two processors racing on one device therefore cannot interleave, and an
/// alert is never published for a state that was not stored.
/// </remarks>
public sealed class RedisDeviceStateStore(IConnectionMultiplexer redis, RedisKeys keys, IOptions<RedisOptions> options)
    : IDeviceStateStore
{
    private const string LatitudeField = "lat";
    private const string LongitudeField = "lon";
    private const string RecordedAtField = "ts";
    private const string SpeedField = "speed";
    private const string HeadingField = "heading";
    private const string GeofencesField = "geofences";

    private static readonly RedisValue[] StateFields =
        [LatitudeField, LongitudeField, RecordedAtField, SpeedField, HeadingField, GeofencesField];

    private IDatabase Db => redis.GetDatabase();

    public async Task<DeviceState?> GetAsync(string deviceId, CancellationToken cancellationToken = default)
    {
        var values = await Db.HashGetAsync(keys.Device(deviceId), StateFields).WaitAsync(cancellationToken).ConfigureAwait(false);
        if (values[0].IsNull || values[1].IsNull || values[2].IsNull)
        {
            return null;
        }

        var geofences = values[5].IsNullOrEmpty
            ? Array.Empty<string>()
            : values[5].ToString().Split(',', StringSplitOptions.RemoveEmptyEntries);

        return new DeviceState(
            deviceId,
            new GeoPoint(ParseDouble(values[0]), ParseDouble(values[1])),
            DateTimeOffset.FromUnixTimeMilliseconds((long)values[2]),
            values[3].IsNullOrEmpty ? null : ParseDouble(values[3]),
            values[4].IsNullOrEmpty ? null : ParseDouble(values[4]),
            geofences);
    }

    public async Task<bool> TryCommitAsync(
        DeviceState? expected,
        DeviceState updated,
        IReadOnlyList<GeofenceAlert> alerts,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(updated);
        ArgumentNullException.ThrowIfNull(alerts);

        var deviceKey = keys.Device(updated.DeviceId);
        var transaction = Db.CreateTransaction();
        transaction.AddCondition(expected is null
            ? Condition.HashNotExists(deviceKey, RecordedAtField)
            : Condition.HashEqual(deviceKey, RecordedAtField, expected.RecordedAt.ToUnixTimeMilliseconds()));

        _ = transaction.HashSetAsync(deviceKey,
        [
            new HashEntry(LatitudeField, Format(updated.Position.Latitude)),
            new HashEntry(LongitudeField, Format(updated.Position.Longitude)),
            new HashEntry(RecordedAtField, updated.RecordedAt.ToUnixTimeMilliseconds()),
            new HashEntry(SpeedField, updated.SpeedMetersPerSecond is { } speed ? Format(speed) : RedisValue.EmptyString),
            new HashEntry(HeadingField, updated.HeadingDegrees is { } heading ? Format(heading) : RedisValue.EmptyString),
            new HashEntry(GeofencesField, string.Join(',', updated.InsideGeofenceIds)),
        ]);
        _ = transaction.GeoAddAsync(keys.Positions, updated.Position.Longitude, updated.Position.Latitude, updated.DeviceId);

        foreach (var alert in alerts)
        {
            var json = JsonSerializer.Serialize(alert, TrackingJson.Options);
            _ = transaction.StreamAddAsync(keys.AlertLog, "alert", json, maxLength: options.Value.AlertLogMaxLength, useApproximateMaxLength: true);
            _ = transaction.PublishAsync(keys.AlertChannel, json);
        }

        return await transaction.ExecuteAsync().WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<IReadOnlyList<NearbyDevice>> FindNearbyAsync(GeoPoint center, double radiusMeters, int limit, CancellationToken cancellationToken = default)
    {
        var db = Db;
        var results = await db.GeoSearchAsync(
                keys.Positions,
                center.Longitude,
                center.Latitude,
                new GeoSearchCircle(radiusMeters, GeoUnit.Meters),
                count: limit,
                demandClosest: true,
                order: Order.Ascending,
                options: GeoRadiusOptions.WithCoordinates | GeoRadiusOptions.WithDistance)
            .WaitAsync(cancellationToken)
            .ConfigureAwait(false);

        if (results.Length == 0)
        {
            return [];
        }

        var batch = db.CreateBatch();
        var timestamps = results
            .Select(r => batch.HashGetAsync(keys.Device(r.Member.ToString()), RecordedAtField))
            .ToArray();
        batch.Execute();
        await Task.WhenAll(timestamps).WaitAsync(cancellationToken).ConfigureAwait(false);

        return results
            .Select((r, i) =>
            {
                var ts = timestamps[i].Result;
                return new NearbyDevice(
                    r.Member.ToString(),
                    new GeoPoint(r.Position!.Value.Latitude, r.Position.Value.Longitude),
                    r.Distance ?? 0,
                    ts.IsNull ? null : DateTimeOffset.FromUnixTimeMilliseconds((long)ts));
            })
            .ToArray();
    }

    private static string Format(double value) => value.ToString("R", CultureInfo.InvariantCulture);

    private static double ParseDouble(RedisValue value) => double.Parse(value.ToString(), NumberStyles.Float, CultureInfo.InvariantCulture);
}
