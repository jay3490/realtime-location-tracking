namespace Tracking.Core.Geo;

/// <summary>
/// Great-circle calculations on a spherical earth model.
/// </summary>
/// <remarks>
/// The haversine formula treats the earth as a sphere with the IUGG mean radius, which keeps the
/// error under roughly 0.5% compared with an ellipsoidal model. That is well inside typical GPS noise.
/// </remarks>
public static class GeoMath
{
    public const double EarthRadiusMeters = 6_371_008.8;

    public static double HaversineMeters(GeoPoint from, GeoPoint to)
    {
        var lat1 = ToRadians(from.Latitude);
        var lat2 = ToRadians(to.Latitude);
        var deltaLat = lat2 - lat1;
        var deltaLon = ToRadians(to.Longitude - from.Longitude);

        var sinHalfLat = Math.Sin(deltaLat / 2);
        var sinHalfLon = Math.Sin(deltaLon / 2);
        var a = (sinHalfLat * sinHalfLat) + (Math.Cos(lat1) * Math.Cos(lat2) * sinHalfLon * sinHalfLon);

        // Clamp guards against a value slightly above one, caused by rounding, for antipodal points.
        var c = 2 * Math.Asin(Math.Sqrt(Math.Min(1.0, a)));
        return EarthRadiusMeters * c;
    }

    /// <summary>
    /// Initial bearing (forward azimuth) from <paramref name="from"/> to <paramref name="to"/>,
    /// in degrees clockwise from true north, normalised to [0, 360).
    /// </summary>
    public static double InitialBearingDegrees(GeoPoint from, GeoPoint to)
    {
        var lat1 = ToRadians(from.Latitude);
        var lat2 = ToRadians(to.Latitude);
        var deltaLon = ToRadians(to.Longitude - from.Longitude);

        var y = Math.Sin(deltaLon) * Math.Cos(lat2);
        var x = (Math.Cos(lat1) * Math.Sin(lat2)) - (Math.Sin(lat1) * Math.Cos(lat2) * Math.Cos(deltaLon));
        return NormalizeDegrees(ToDegrees(Math.Atan2(y, x)));
    }

    /// <summary>
    /// The point reached by travelling <paramref name="distanceMeters"/> from <paramref name="start"/>
    /// along a great circle with the given initial bearing.
    /// </summary>
    public static GeoPoint Destination(GeoPoint start, double bearingDegrees, double distanceMeters)
    {
        var angularDistance = distanceMeters / EarthRadiusMeters;
        var bearing = ToRadians(bearingDegrees);
        var lat1 = ToRadians(start.Latitude);
        var lon1 = ToRadians(start.Longitude);

        var lat2 = Math.Asin(
            (Math.Sin(lat1) * Math.Cos(angularDistance)) +
            (Math.Cos(lat1) * Math.Sin(angularDistance) * Math.Cos(bearing)));
        var lon2 = lon1 + Math.Atan2(
            Math.Sin(bearing) * Math.Sin(angularDistance) * Math.Cos(lat1),
            Math.Cos(angularDistance) - (Math.Sin(lat1) * Math.Sin(lat2)));

        return new GeoPoint(ToDegrees(lat2), NormalizeLongitude(ToDegrees(lon2)));
    }

    /// <summary>
    /// Average ground speed between two fixes. Returns <c>null</c> when the elapsed time is not
    /// positive, because no speed can be derived from simultaneous or reordered fixes.
    /// </summary>
    public static double? SpeedMetersPerSecond(GeoPoint from, DateTimeOffset fromTime, GeoPoint to, DateTimeOffset toTime)
    {
        var elapsedSeconds = (toTime - fromTime).TotalSeconds;
        if (elapsedSeconds <= 0)
        {
            return null;
        }

        return HaversineMeters(from, to) / elapsedSeconds;
    }

    public static double ToRadians(double degrees) => degrees * Math.PI / 180.0;

    public static double ToDegrees(double radians) => radians * 180.0 / Math.PI;

    private static double NormalizeDegrees(double degrees) => ((degrees % 360) + 360) % 360;

    private static double NormalizeLongitude(double longitude) => ((longitude + 540) % 360) - 180;
}
