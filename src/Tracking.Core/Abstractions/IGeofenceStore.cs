using Tracking.Core.Geofencing;

namespace Tracking.Core.Abstractions;

public interface IGeofenceStore
{
    Task<IReadOnlyList<Geofence>> GetAllAsync(CancellationToken cancellationToken = default);

    Task<Geofence?> GetAsync(string id, CancellationToken cancellationToken = default);

    /// <summary>Creates or replaces a geofence. Returns true when it did not exist before.</summary>
    Task<bool> UpsertAsync(Geofence geofence, CancellationToken cancellationToken = default);

    /// <summary>Creates a geofence only if the id is free. Returns false when it already exists.</summary>
    Task<bool> TryCreateAsync(Geofence geofence, CancellationToken cancellationToken = default);

    Task<bool> DeleteAsync(string id, CancellationToken cancellationToken = default);

    /// <summary>
    /// A counter that changes whenever the geofence set changes, so readers can cache the set
    /// and reload it only when this value moves.
    /// </summary>
    Task<long> GetVersionAsync(CancellationToken cancellationToken = default);
}
