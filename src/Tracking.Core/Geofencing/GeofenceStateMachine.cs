using Tracking.Core.Geo;

namespace Tracking.Core.Geofencing;

/// <summary>
/// The result of evaluating one position against the geofence set.
/// </summary>
/// <param name="InsideGeofenceIds">Membership after the position, sorted ordinally.</param>
/// <param name="Alerts">Exits first, then enters, each ordered by geofence id.</param>
public sealed record GeofenceEvaluation(IReadOnlyList<string> InsideGeofenceIds, IReadOnlyList<GeofenceAlert> Alerts);

/// <summary>
/// Tracks, per device, which geofences it is inside and emits an alert on every change.
/// </summary>
/// <remarks>
/// Each geofence is a two-state machine (outside, inside). A device with no history starts outside
/// every geofence, so its first fix inside one produces an enter alert. When a geofence is deleted
/// while a device is inside it, the membership is dropped silently: there is no boundary left to
/// exit, so no exit alert is produced.
/// </remarks>
public static class GeofenceStateMachine
{
    public static GeofenceEvaluation Evaluate(
        string deviceId,
        IReadOnlyCollection<string> previouslyInside,
        GeoPoint position,
        DateTimeOffset recordedAt,
        IReadOnlyCollection<Geofence> geofences)
    {
        ArgumentNullException.ThrowIfNull(previouslyInside);
        ArgumentNullException.ThrowIfNull(geofences);

        var wasInside = previouslyInside as IReadOnlySet<string> ?? new HashSet<string>(previouslyInside, StringComparer.Ordinal);
        var nowInside = new List<string>();
        var exits = new List<GeofenceAlert>();
        var enters = new List<GeofenceAlert>();

        foreach (var fence in geofences.OrderBy(f => f.Id, StringComparer.Ordinal))
        {
            var inside = fence.Contains(position);
            var before = wasInside.Contains(fence.Id);

            if (inside)
            {
                nowInside.Add(fence.Id);
            }

            if (inside == before)
            {
                continue;
            }

            var alert = new GeofenceAlert(
                deviceId,
                fence.Id,
                fence.Name,
                inside ? GeofenceTransition.Enter : GeofenceTransition.Exit,
                position,
                recordedAt);
            (inside ? enters : exits).Add(alert);
        }

        return new GeofenceEvaluation(nowInside, [.. exits, .. enters]);
    }
}
