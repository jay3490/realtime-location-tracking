using System.Text.Json;
using Tracking.Core.Geo;
using Tracking.Core.Geofencing;

namespace Tracking.Core.Tests.Geofencing;

public class GeofenceTests
{
    private static readonly GeoPoint Center = new(52.3676, 4.9041);

    // A 0.01 x 0.01 degree square.
    private static readonly PolygonGeofence Square = new("square", "Square", new[]
    {
        new GeoPoint(0, 0),
        new GeoPoint(0, 0.01),
        new GeoPoint(0.01, 0.01),
        new GeoPoint(0.01, 0),
    });

    // An L shape whose notch (the upper right quadrant) is outside.
    private static readonly PolygonGeofence LShape = new("l-shape", "L", new[]
    {
        new GeoPoint(0, 0),
        new GeoPoint(0, 2),
        new GeoPoint(1, 2),
        new GeoPoint(1, 1),
        new GeoPoint(2, 1),
        new GeoPoint(2, 0),
    });

    [Theory]
    [InlineData(0, true)]
    [InlineData(99, true)]
    [InlineData(101, false)]
    [InlineData(5_000, false)]
    public void Circle_contains_points_within_its_radius(double distanceFromCenter, bool expected)
    {
        var fence = new CircleGeofence("c", "Circle", Center, RadiusMeters: 100);
        var point = GeoMath.Destination(Center, 37, distanceFromCenter);

        Assert.Equal(expected, fence.Contains(point));
    }

    [Theory]
    [InlineData(0.005, 0.005, true)]
    [InlineData(0.0001, 0.0099, true)]
    [InlineData(-0.0001, 0.005, false)]
    [InlineData(0.005, 0.0101, false)]
    [InlineData(0.02, 0.02, false)]
    public void Polygon_uses_even_odd_containment(double lat, double lon, bool expected)
    {
        Assert.Equal(expected, Square.Contains(new GeoPoint(lat, lon)));
    }

    [Theory]
    [InlineData(0, 0.005)]
    [InlineData(0.01, 0.01)]
    [InlineData(0.005, 0)]
    public void Polygon_counts_edges_and_vertices_as_inside(double lat, double lon)
    {
        Assert.True(Square.Contains(new GeoPoint(lat, lon)));
    }

    [Theory]
    [InlineData(0.5, 0.5, true)]
    [InlineData(0.5, 1.5, true)]
    [InlineData(1.5, 0.5, true)]
    [InlineData(1.5, 1.5, false)]
    public void Polygon_handles_concave_shapes(double lat, double lon, bool expected)
    {
        Assert.Equal(expected, LShape.Contains(new GeoPoint(lat, lon)));
    }

    [Fact]
    public void Polygon_containment_does_not_depend_on_winding_order()
    {
        var reversed = Square with { Vertices = Square.Vertices.Reverse().ToArray() };

        Assert.True(reversed.Contains(new GeoPoint(0.005, 0.005)));
        Assert.False(reversed.Contains(new GeoPoint(0.02, 0.005)));
    }

    [Fact]
    public void Valid_definitions_have_no_errors()
    {
        Assert.Empty(Square.Validate());
        Assert.Empty(new CircleGeofence("depot_1", "Depot", Center, 250).Validate());
    }

    [Theory]
    [InlineData("")]
    [InlineData("has space")]
    [InlineData("comma,separated")]
    public void Ids_are_restricted_to_a_safe_alphabet(string id)
    {
        var errors = new CircleGeofence(id, "Name", Center, 10).Validate();

        Assert.Contains(errors, e => e.StartsWith("id", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-5)]
    [InlineData(double.PositiveInfinity)]
    [InlineData(2_000_000)]
    public void Circle_radius_must_be_positive_and_bounded(double radius)
    {
        var errors = new CircleGeofence("c", "Circle", Center, radius).Validate();

        Assert.Contains(errors, e => e.StartsWith("radiusMeters", StringComparison.Ordinal));
    }

    [Fact]
    public void Polygon_needs_three_distinct_vertices()
    {
        var degenerate = new PolygonGeofence("p", "P", new[] { new GeoPoint(0, 0), new GeoPoint(0, 0), new GeoPoint(1, 1) });

        Assert.Contains(degenerate.Validate(), e => e.Contains("distinct", StringComparison.Ordinal));
        Assert.False(new PolygonGeofence("p", "P", new[] { new GeoPoint(0, 0), new GeoPoint(1, 1) }).Contains(new GeoPoint(0.5, 0.5)));
    }

    [Fact]
    public void Polygon_rejects_antimeridian_spans()
    {
        var wide = new PolygonGeofence("p", "P", new[] { new GeoPoint(0, -179), new GeoPoint(0, 179), new GeoPoint(1, 0) });

        Assert.Contains(wide.Validate(), e => e.Contains("antimeridian", StringComparison.Ordinal));
    }

    [Fact]
    public void Geofences_round_trip_through_json_with_a_type_discriminator()
    {
        Geofence[] fences = [new CircleGeofence("c", "Circle", Center, 120.5), Square];

        var json = JsonSerializer.Serialize(fences, TrackingJson.Options);
        var restored = JsonSerializer.Deserialize<Geofence[]>(json, TrackingJson.Options)!;

        Assert.Contains("\"type\":\"circle\"", json, StringComparison.Ordinal);
        Assert.Contains("\"type\":\"polygon\"", json, StringComparison.Ordinal);
        var circle = Assert.IsType<CircleGeofence>(restored[0]);
        Assert.Equal(120.5, circle.RadiusMeters);
        Assert.Equal(Center, circle.Center);
        var polygon = Assert.IsType<PolygonGeofence>(restored[1]);
        Assert.Equal(Square.Vertices, polygon.Vertices);
    }

    [Fact]
    public void Geofence_json_accepts_api_style_input()
    {
        const string json = """
            {"type":"circle","id":"hq","name":"Head office","center":{"latitude":52.37,"longitude":4.9},"radiusMeters":300}
            """;

        var fence = JsonSerializer.Deserialize<Geofence>(json, TrackingJson.Options);

        var circle = Assert.IsType<CircleGeofence>(fence);
        Assert.Equal("hq", circle.Id);
        Assert.Equal(300, circle.RadiusMeters);
    }
}
