using System.Globalization;
using System.Text.Json.Serialization;
using Tracking.Core.Devices;
using Tracking.Core.Geo;
using Tracking.Core.Geofencing;

namespace Tracking.Core.Analytics;

public enum AnalyticsEventKind
{
    Position = 1,
    Geofence = 2,
}

/// <summary>
/// Something the processor committed that downstream analytics needs. Events are delivered at least
/// once, so every event carries an id that is stable across redeliveries: consumers deduplicate on it.
/// </summary>
public abstract record AnalyticsEvent(string EventId, string DeviceId)
{
    [JsonIgnore]
    public abstract AnalyticsEventKind Kind { get; }
}

/// <summary>
/// A committed position, enriched with what the processor derived from the previous one. The
/// distance and previous timestamp make each event self-contained: summing distances does not depend
/// on the order in which events are consumed.
/// </summary>
/// <param name="DistanceMeters">Haversine distance from the previous committed position; 0 for the first.</param>
/// <param name="PreviousRecordedAt">Timestamp of the previous committed position; null for the first.</param>
public sealed record PositionEvent(
    string EventId,
    string DeviceId,
    double Latitude,
    double Longitude,
    DateTimeOffset RecordedAt,
    double? AccuracyMeters,
    double? SpeedMetersPerSecond,
    double? HeadingDegrees,
    double DistanceMeters,
    DateTimeOffset? PreviousRecordedAt,
    IReadOnlyList<string> InsideGeofenceIds) : AnalyticsEvent(EventId, DeviceId)
{
    [JsonIgnore]
    public override AnalyticsEventKind Kind => AnalyticsEventKind.Position;
}

/// <summary>A device entered or left a geofence.</summary>
public sealed record GeofenceEvent(
    string EventId,
    string DeviceId,
    string GeofenceId,
    string GeofenceName,
    GeofenceTransition Transition,
    double Latitude,
    double Longitude,
    DateTimeOffset OccurredAt) : AnalyticsEvent(EventId, DeviceId)
{
    [JsonIgnore]
    public override AnalyticsEventKind Kind => AnalyticsEventKind.Geofence;
}

public static class AnalyticsEvents
{
    /// <summary>
    /// The events for one committed transition: one position event, then one geofence event per alert.
    /// </summary>
    /// <remarks>
    /// Committed timestamps strictly increase per device (stale pings never commit), so a device id and
    /// a timestamp identify a position event, and adding the geofence and transition identifies an alert.
    /// </remarks>
    public static IReadOnlyList<AnalyticsEvent> For(DeviceState? previous, DeviceState next, LocationPing ping, IReadOnlyList<GeofenceAlert> alerts)
    {
        ArgumentNullException.ThrowIfNull(next);
        ArgumentNullException.ThrowIfNull(ping);
        ArgumentNullException.ThrowIfNull(alerts);

        var events = new List<AnalyticsEvent>(1 + alerts.Count)
        {
            new PositionEvent(
                PositionEventId(next.DeviceId, next.RecordedAt),
                next.DeviceId,
                next.Position.Latitude,
                next.Position.Longitude,
                next.RecordedAt,
                ping.AccuracyMeters,
                next.SpeedMetersPerSecond,
                next.HeadingDegrees,
                previous is null ? 0 : GeoMath.HaversineMeters(previous.Position, next.Position),
                previous?.RecordedAt,
                next.InsideGeofenceIds),
        };

        foreach (var alert in alerts)
        {
            events.Add(new GeofenceEvent(
                GeofenceEventId(alert),
                alert.DeviceId,
                alert.GeofenceId,
                alert.GeofenceName,
                alert.Transition,
                alert.Position.Latitude,
                alert.Position.Longitude,
                alert.OccurredAt));
        }

        return events;
    }

    public static string PositionEventId(string deviceId, DateTimeOffset recordedAt) =>
        string.Create(CultureInfo.InvariantCulture, $"{deviceId}/{recordedAt.ToUnixTimeMilliseconds()}");

    public static string GeofenceEventId(GeofenceAlert alert)
    {
        ArgumentNullException.ThrowIfNull(alert);
        var transition = alert.Transition == GeofenceTransition.Enter ? "enter" : "exit";
        return string.Create(
            CultureInfo.InvariantCulture,
            $"{alert.DeviceId}/{alert.GeofenceId}/{transition}/{alert.OccurredAt.ToUnixTimeMilliseconds()}");
    }
}
