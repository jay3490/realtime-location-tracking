using Tracking.Core.Devices;
using Tracking.Core.Geo;
using Tracking.Core.Geofencing;

namespace Tracking.Core.Abstractions;

public sealed record NearbyDevice(string DeviceId, GeoPoint Position, double DistanceMeters, DateTimeOffset? RecordedAt);

/// <summary>
/// Latest device state, the spatial index over it, and the alerts produced by each update.
/// </summary>
public interface IDeviceStateStore
{
    Task<DeviceState?> GetAsync(string deviceId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Atomically stores <paramref name="updated"/>, updates the spatial index and records and publishes
    /// <paramref name="alerts"/>, but only if the stored state still matches <paramref name="expected"/>
    /// (null meaning "no state yet"). Returns false, writing nothing, when another writer got there first.
    /// </summary>
    Task<bool> TryCommitAsync(DeviceState? expected, DeviceState updated, IReadOnlyList<GeofenceAlert> alerts, CancellationToken cancellationToken = default);

    /// <summary>Devices whose latest position is within <paramref name="radiusMeters"/>, nearest first.</summary>
    Task<IReadOnlyList<NearbyDevice>> FindNearbyAsync(GeoPoint center, double radiusMeters, int limit, CancellationToken cancellationToken = default);
}
