using Tracking.Core.Geo;

namespace Tracking.Core.Tests.Geo;

public class GeoMathTests
{
    private static readonly GeoPoint London = new(51.5074, -0.1278);
    private static readonly GeoPoint Paris = new(48.8566, 2.3522);

    [Fact]
    public void Haversine_of_identical_points_is_zero()
    {
        Assert.Equal(0, GeoMath.HaversineMeters(London, London), precision: 9);
    }

    [Fact]
    public void Haversine_london_to_paris_matches_reference_distance()
    {
        // The great-circle distance on a 6371 km sphere is about 343.5 km.
        var meters = GeoMath.HaversineMeters(London, Paris);

        Assert.InRange(meters, 343_000, 344_000);
    }

    [Fact]
    public void Haversine_is_symmetric()
    {
        Assert.Equal(
            GeoMath.HaversineMeters(London, Paris),
            GeoMath.HaversineMeters(Paris, London),
            precision: 6);
    }

    [Fact]
    public void One_degree_of_latitude_is_about_111_km()
    {
        var meters = GeoMath.HaversineMeters(new GeoPoint(0, 0), new GeoPoint(1, 0));

        Assert.InRange(meters, 111_000, 111_400);
    }

    [Fact]
    public void Haversine_handles_antipodal_points()
    {
        var meters = GeoMath.HaversineMeters(new GeoPoint(0, 0), new GeoPoint(0, 180));

        Assert.Equal(Math.PI * GeoMath.EarthRadiusMeters, meters, precision: 3);
    }

    [Fact]
    public void Haversine_crosses_the_antimeridian_the_short_way()
    {
        var meters = GeoMath.HaversineMeters(new GeoPoint(0, 179.9), new GeoPoint(0, -179.9));

        Assert.InRange(meters, 22_000, 22_500);
    }

    [Theory]
    [InlineData(0, 0, 1, 0, 0)]
    [InlineData(0, 0, 0, 1, 90)]
    [InlineData(1, 0, 0, 0, 180)]
    [InlineData(0, 1, 0, 0, 270)]
    public void Initial_bearing_points_along_cardinal_directions(
        double fromLat, double fromLon, double toLat, double toLon, double expected)
    {
        var bearing = GeoMath.InitialBearingDegrees(new GeoPoint(fromLat, fromLon), new GeoPoint(toLat, toLon));

        Assert.Equal(expected, bearing, precision: 6);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(45)]
    [InlineData(200)]
    [InlineData(359)]
    public void Destination_round_trips_with_distance_and_bearing(double bearing)
    {
        var destination = GeoMath.Destination(London, bearing, 5_000);

        Assert.Equal(5_000, GeoMath.HaversineMeters(London, destination), precision: 3);
        Assert.Equal(bearing, GeoMath.InitialBearingDegrees(London, destination), precision: 1);
    }

    [Fact]
    public void Destination_wraps_longitude_across_the_antimeridian()
    {
        var destination = GeoMath.Destination(new GeoPoint(0, 179.99), 90, 5_000);

        Assert.True(destination.IsValid);
        Assert.InRange(destination.Longitude, -180, -179.9);
    }

    [Fact]
    public void Speed_is_distance_over_elapsed_time()
    {
        var start = new GeoPoint(0, 0);
        var end = GeoMath.Destination(start, 90, 1_000);
        var t0 = new DateTimeOffset(2026, 1, 1, 12, 0, 0, TimeSpan.Zero);

        var speed = GeoMath.SpeedMetersPerSecond(start, t0, end, t0.AddSeconds(100));

        Assert.NotNull(speed);
        Assert.Equal(10, speed.Value, precision: 6);
    }

    [Fact]
    public void Speed_is_undefined_when_time_does_not_advance()
    {
        var t0 = DateTimeOffset.UnixEpoch;

        Assert.Null(GeoMath.SpeedMetersPerSecond(London, t0, Paris, t0));
        Assert.Null(GeoMath.SpeedMetersPerSecond(London, t0, Paris, t0.AddSeconds(-1)));
    }

    [Theory]
    [InlineData(91, 0, false)]
    [InlineData(-90, 180, true)]
    [InlineData(0, -180.0001, false)]
    [InlineData(double.NaN, 0, false)]
    public void GeoPoint_validates_coordinate_ranges(double lat, double lon, bool expected)
    {
        Assert.Equal(expected, new GeoPoint(lat, lon).IsValid);
    }
}
