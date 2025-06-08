using System.Text.Json;
using Tracking.Core.Analytics;
using Tracking.Core.Devices;
using Tracking.Core.Geo;
using Tracking.Core.Geofencing;

namespace Tracking.Core.Tests.Analytics;

public sealed class AnalyticsEventsTests
{
    private static readonly GeoPoint Start = new(52.3731, 4.8926);
    private static readonly DateTimeOffset T0 = new(2026, 3, 9, 8, 0, 0, TimeSpan.Zero);

    [Fact]
    public void The_first_position_has_no_distance_and_no_predecessor()
    {
        var next = new DeviceState("van", Start, T0, null, null, []);

        var position = Assert.IsType<PositionEvent>(Assert.Single(AnalyticsEvents.For(null, next, new LocationPing("van", Start, T0, 4), [])));

        Assert.Equal(0, position.DistanceMeters);
        Assert.Null(position.PreviousRecordedAt);
        Assert.Equal(4, position.AccuracyMeters);
        Assert.Equal($"van/{T0.ToUnixTimeMilliseconds()}", position.EventId);
    }

    [Fact]
    public void Later_positions_carry_the_distance_from_the_previous_one()
    {
        var previous = new DeviceState("van", Start, T0, null, null, []);
        var moved = GeoMath.Destination(Start, 90, 250);
        var next = new DeviceState("van", moved, T0.AddSeconds(25), 10, 90, ["dam"]);

        var position = (PositionEvent)AnalyticsEvents.For(previous, next, new LocationPing("van", moved, T0.AddSeconds(25)), [])[0];

        Assert.Equal(250, position.DistanceMeters, precision: 3);
        Assert.Equal(T0, position.PreviousRecordedAt);
        Assert.Equal(["dam"], position.InsideGeofenceIds);
        Assert.Equal(10, position.SpeedMetersPerSecond);
    }

    [Fact]
    public void Each_alert_becomes_a_geofence_event_after_the_position()
    {
        var next = new DeviceState("van", Start, T0, null, null, ["dam"]);
        var enter = new GeofenceAlert("van", "dam", "Dam", GeofenceTransition.Enter, Start, T0);
        var exit = new GeofenceAlert("van", "depot", "Depot", GeofenceTransition.Exit, Start, T0);

        var events = AnalyticsEvents.For(null, next, new LocationPing("van", Start, T0), [enter, exit]);

        Assert.Equal([AnalyticsEventKind.Position, AnalyticsEventKind.Geofence, AnalyticsEventKind.Geofence], events.Select(e => e.Kind));
        var first = Assert.IsType<GeofenceEvent>(events[1]);
        Assert.Equal(GeofenceTransition.Enter, first.Transition);
        Assert.Equal($"van/dam/enter/{T0.ToUnixTimeMilliseconds()}", first.EventId);
        Assert.Equal($"van/depot/exit/{T0.ToUnixTimeMilliseconds()}", events[2].EventId);
    }

    [Fact]
    public void Events_round_trip_through_the_shared_json_shape()
    {
        var original = new GeofenceEvent("van/dam/enter/1", "van", "dam", "Dam", GeofenceTransition.Enter, 52.1, 4.2, T0);

        var json = JsonSerializer.Serialize(original, TrackingJson.Options);
        var copy = JsonSerializer.Deserialize<GeofenceEvent>(json, TrackingJson.Options);

        Assert.Contains("\"transition\":\"enter\"", json, StringComparison.Ordinal);
        Assert.DoesNotContain("kind", json, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(original, copy);
    }
}
