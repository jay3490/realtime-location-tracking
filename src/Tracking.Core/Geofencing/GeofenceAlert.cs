using Tracking.Core.Geo;

namespace Tracking.Core.Geofencing;

public enum GeofenceTransition
{
    Enter = 1,
    Exit = 2,
}

/// <summary>
/// A device crossed a geofence boundary. <see cref="OccurredAt"/> is the device's timestamp of the
/// first ping on the new side of the boundary.
/// </summary>
public sealed record GeofenceAlert(
    string DeviceId,
    string GeofenceId,
    string GeofenceName,
    GeofenceTransition Transition,
    GeoPoint Position,
    DateTimeOffset OccurredAt);
