using Tracking.Core.Geo;
using Tracking.Core.Geofencing;

namespace Tracking.Core.Devices;

/// <summary>
/// What applying a ping to a device's state produced.
/// </summary>
public sealed record TrackingOutcome
{
    private TrackingOutcome(DeviceState? state, IReadOnlyList<GeofenceAlert> alerts)
    {
        State = state;
        Alerts = alerts;
    }

    /// <summary>The ping was not newer than the stored state and was ignored.</summary>
    public static TrackingOutcome Stale { get; } = new(null, []);

    public DeviceState? State { get; }

    public IReadOnlyList<GeofenceAlert> Alerts { get; }

    [System.Diagnostics.CodeAnalysis.MemberNotNullWhen(false, nameof(State))]
    public bool IsStale => State is null;

    public static TrackingOutcome Updated(DeviceState state, IReadOnlyList<GeofenceAlert> alerts) =>
        new(state ?? throw new ArgumentNullException(nameof(state)), alerts);
}

/// <summary>
/// Pure state transition: previous device state + ping + geofences = next state and alerts.
/// </summary>
public static class DeviceTracker
{
    /// <summary>
    /// Below this displacement the bearing between two fixes is dominated by GPS noise,
    /// so the previous heading is kept instead.
    /// </summary>
    public const double MinHeadingDisplacementMeters = 2.0;

    /// <summary>
    /// Applies <paramref name="ping"/> to <paramref name="current"/>. Pings whose timestamp is not
    /// strictly after the stored one are stale: they come from redelivery or out-of-order arrival
    /// and must not move the device backwards or replay geofence alerts.
    /// </summary>
    public static TrackingOutcome Apply(DeviceState? current, LocationPing ping, IReadOnlyCollection<Geofence> geofences)
    {
        ArgumentNullException.ThrowIfNull(ping);

        if (current is not null && ping.RecordedAt <= current.RecordedAt)
        {
            return TrackingOutcome.Stale;
        }

        double? speed = null;
        double? heading = null;
        if (current is not null)
        {
            speed = GeoMath.SpeedMetersPerSecond(current.Position, current.RecordedAt, ping.Position, ping.RecordedAt);
            heading = GeoMath.HaversineMeters(current.Position, ping.Position) >= MinHeadingDisplacementMeters
                ? GeoMath.InitialBearingDegrees(current.Position, ping.Position)
                : current.HeadingDegrees;
        }

        var evaluation = GeofenceStateMachine.Evaluate(
            ping.DeviceId,
            current?.InsideGeofenceIds ?? [],
            ping.Position,
            ping.RecordedAt,
            geofences);

        var next = new DeviceState(ping.DeviceId, ping.Position, ping.RecordedAt, speed, heading, evaluation.InsideGeofenceIds);
        return TrackingOutcome.Updated(next, evaluation.Alerts);
    }
}
