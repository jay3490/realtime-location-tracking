using Tracking.Core.Devices;
using Tracking.Core.Geo;
using Tracking.Core.Geofencing;

namespace Tracking.Core.Tests.Devices;

public class DeviceTrackerTests
{
    private static readonly GeoPoint Start = new(40.7128, -74.0060);
    private static readonly DateTimeOffset T0 = new(2026, 5, 4, 9, 30, 0, TimeSpan.Zero);
    private static readonly CircleGeofence StartFence = new("start", "Start", Start, 100);

    [Fact]
    public void First_ping_creates_state_without_speed_or_heading()
    {
        var outcome = DeviceTracker.Apply(null, new LocationPing("d1", Start, T0), [StartFence]);

        Assert.False(outcome.IsStale);
        Assert.Equal(Start, outcome.State.Position);
        Assert.Equal(T0, outcome.State.RecordedAt);
        Assert.Null(outcome.State.SpeedMetersPerSecond);
        Assert.Null(outcome.State.HeadingDegrees);
        Assert.Equal(["start"], outcome.State.InsideGeofenceIds);
        Assert.Equal(GeofenceTransition.Enter, Assert.Single(outcome.Alerts).Transition);
    }

    [Fact]
    public void Next_ping_derives_speed_and_heading()
    {
        var first = DeviceTracker.Apply(null, new LocationPing("d1", Start, T0), []).State!;
        var east = GeoMath.Destination(Start, 90, 300);

        var outcome = DeviceTracker.Apply(first, new LocationPing("d1", east, T0.AddSeconds(30)), []);

        Assert.Equal(10, outcome.State!.SpeedMetersPerSecond!.Value, precision: 3);
        Assert.Equal(90, outcome.State.HeadingDegrees!.Value, precision: 1);
    }

    [Fact]
    public void Heading_is_kept_when_the_device_barely_moves()
    {
        var moving = new DeviceState("d1", Start, T0, 5, 45, []);
        var jitter = GeoMath.Destination(Start, 200, 0.5);

        var outcome = DeviceTracker.Apply(moving, new LocationPing("d1", jitter, T0.AddSeconds(1)), []);

        Assert.Equal(45, outcome.State!.HeadingDegrees);
        Assert.Equal(0.5, outcome.State.SpeedMetersPerSecond!.Value, precision: 3);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-10)]
    public void Pings_not_newer_than_the_state_are_stale(int secondsAfterState)
    {
        var state = new DeviceState("d1", Start, T0, null, null, ["start"]);
        var farAway = GeoMath.Destination(Start, 0, 10_000);

        var outcome = DeviceTracker.Apply(state, new LocationPing("d1", farAway, T0.AddSeconds(secondsAfterState)), [StartFence]);

        Assert.True(outcome.IsStale);
        Assert.Null(outcome.State);
        Assert.Empty(outcome.Alerts);
    }

    [Fact]
    public void Leaving_a_fence_reports_an_exit_with_the_ping_timestamp()
    {
        var state = new DeviceState("d1", Start, T0, null, null, ["start"]);
        var away = GeoMath.Destination(Start, 0, 1_000);

        var outcome = DeviceTracker.Apply(state, new LocationPing("d1", away, T0.AddMinutes(2)), [StartFence]);

        Assert.Empty(outcome.State!.InsideGeofenceIds);
        var exit = Assert.Single(outcome.Alerts);
        Assert.Equal(GeofenceTransition.Exit, exit.Transition);
        Assert.Equal(T0.AddMinutes(2), exit.OccurredAt);
        Assert.Equal(away, exit.Position);
    }
}
