using Tracking.Core.Geo;
using Tracking.Core.Geofencing;

namespace Tracking.Core.Tests.Geofencing;

public class GeofenceStateMachineTests
{
    private static readonly GeoPoint Depot = new(52.3676, 4.9041);
    private static readonly DateTimeOffset T0 = new(2026, 3, 1, 8, 0, 0, TimeSpan.Zero);

    private static readonly CircleGeofence DepotFence = new("depot", "Depot", Depot, 200);
    private static readonly CircleGeofence WideFence = new("zone", "Delivery zone", Depot, 2_000);

    private static readonly GeoPoint InsideBoth = GeoMath.Destination(Depot, 0, 50);
    private static readonly GeoPoint InsideZoneOnly = GeoMath.Destination(Depot, 0, 1_000);
    private static readonly GeoPoint OutsideBoth = GeoMath.Destination(Depot, 0, 5_000);

    [Fact]
    public void First_fix_inside_a_fence_enters_it()
    {
        var result = GeofenceStateMachine.Evaluate("d1", [], InsideZoneOnly, T0, [DepotFence, WideFence]);

        Assert.Equal(["zone"], result.InsideGeofenceIds);
        var alert = Assert.Single(result.Alerts);
        Assert.Equal(GeofenceTransition.Enter, alert.Transition);
        Assert.Equal("zone", alert.GeofenceId);
        Assert.Equal("Delivery zone", alert.GeofenceName);
        Assert.Equal("d1", alert.DeviceId);
        Assert.Equal(T0, alert.OccurredAt);
    }

    [Fact]
    public void Staying_inside_produces_no_alerts()
    {
        var result = GeofenceStateMachine.Evaluate("d1", ["zone"], InsideZoneOnly, T0, [DepotFence, WideFence]);

        Assert.Equal(["zone"], result.InsideGeofenceIds);
        Assert.Empty(result.Alerts);
    }

    [Fact]
    public void Staying_outside_produces_no_alerts()
    {
        var result = GeofenceStateMachine.Evaluate("d1", [], OutsideBoth, T0, [DepotFence, WideFence]);

        Assert.Empty(result.InsideGeofenceIds);
        Assert.Empty(result.Alerts);
    }

    [Fact]
    public void Leaving_a_fence_exits_it()
    {
        var result = GeofenceStateMachine.Evaluate("d1", ["depot", "zone"], InsideZoneOnly, T0, [DepotFence, WideFence]);

        Assert.Equal(["zone"], result.InsideGeofenceIds);
        var alert = Assert.Single(result.Alerts);
        Assert.Equal(GeofenceTransition.Exit, alert.Transition);
        Assert.Equal("depot", alert.GeofenceId);
    }

    [Fact]
    public void Crossing_several_boundaries_lists_exits_before_enters()
    {
        var other = new CircleGeofence("a-far", "Far away", OutsideBoth, 100);

        var result = GeofenceStateMachine.Evaluate("d1", ["depot", "zone"], OutsideBoth, T0, [WideFence, other, DepotFence]);

        Assert.Equal(["a-far"], result.InsideGeofenceIds);
        Assert.Collection(
            result.Alerts,
            a => Assert.Equal(("depot", GeofenceTransition.Exit), (a.GeofenceId, a.Transition)),
            a => Assert.Equal(("zone", GeofenceTransition.Exit), (a.GeofenceId, a.Transition)),
            a => Assert.Equal(("a-far", GeofenceTransition.Enter), (a.GeofenceId, a.Transition)));
    }

    [Fact]
    public void Membership_of_a_deleted_fence_is_dropped_without_an_exit()
    {
        var result = GeofenceStateMachine.Evaluate("d1", ["zone", "deleted"], InsideZoneOnly, T0, [WideFence]);

        Assert.Equal(["zone"], result.InsideGeofenceIds);
        Assert.Empty(result.Alerts);
    }

    [Fact]
    public void A_full_trip_enters_and_exits_in_sequence()
    {
        GeoPoint[] trip = [OutsideBoth, InsideZoneOnly, InsideBoth, InsideZoneOnly, OutsideBoth];
        IReadOnlyCollection<string> inside = [];
        var alerts = new List<GeofenceAlert>();

        for (var i = 0; i < trip.Length; i++)
        {
            var result = GeofenceStateMachine.Evaluate("d1", inside, trip[i], T0.AddMinutes(i), [DepotFence, WideFence]);
            inside = result.InsideGeofenceIds;
            alerts.AddRange(result.Alerts);
        }

        Assert.Empty(inside);
        Assert.Equal(
            [
                ("zone", GeofenceTransition.Enter, 1),
                ("depot", GeofenceTransition.Enter, 2),
                ("depot", GeofenceTransition.Exit, 3),
                ("zone", GeofenceTransition.Exit, 4),
            ],
            alerts.Select(a => (a.GeofenceId, a.Transition, (int)(a.OccurredAt - T0).TotalMinutes)));
    }
}
