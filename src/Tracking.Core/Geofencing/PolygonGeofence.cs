using Tracking.Core.Geo;

namespace Tracking.Core.Geofencing;

/// <summary>
/// A simple polygon whose vertices are listed in order (either winding). The ring is closed
/// implicitly, so the first vertex should not be repeated at the end.
/// </summary>
/// <remarks>
/// Containment uses the even-odd ray casting rule with latitude and longitude treated as planar
/// coordinates. That is accurate for city-scale fences, but a polygon must not cross the
/// antimeridian, which validation enforces by limiting its longitude span to under 180 degrees.
/// Points on an edge or vertex count as inside.
/// </remarks>
public sealed record PolygonGeofence(string Id, string Name, IReadOnlyList<GeoPoint> Vertices)
    : Geofence(Id, Name)
{
    public const int MaxVertices = 1_000;

    // Roughly one centimetre at the equator; used to decide whether a point lies on an edge.
    private const double EdgeToleranceDegrees = 1e-7;

    public override bool Contains(GeoPoint point)
    {
        var vertices = Vertices;
        if (vertices.Count < 3)
        {
            return false;
        }

        var inside = false;
        for (int i = 0, j = vertices.Count - 1; i < vertices.Count; j = i++)
        {
            var a = vertices[i];
            var b = vertices[j];

            if (IsOnSegment(point, a, b))
            {
                return true;
            }

            var crossesRay = (a.Latitude > point.Latitude) != (b.Latitude > point.Latitude);
            if (crossesRay)
            {
                var intersectionLon = a.Longitude +
                    ((point.Latitude - a.Latitude) * (b.Longitude - a.Longitude) / (b.Latitude - a.Latitude));
                if (point.Longitude < intersectionLon)
                {
                    inside = !inside;
                }
            }
        }

        return inside;
    }

    protected override void ValidateShape(List<string> errors)
    {
        if (Vertices is null || Vertices.Count < 3)
        {
            errors.Add("vertices must contain at least 3 points.");
            return;
        }

        if (Vertices.Count > MaxVertices)
        {
            errors.Add($"vertices must contain at most {MaxVertices} points.");
        }

        if (Vertices.Any(v => !v.IsValid))
        {
            errors.Add("every vertex must have latitude in [-90, 90] and longitude in [-180, 180].");
            return;
        }

        if (Vertices.Distinct().Count() < 3)
        {
            errors.Add("vertices must contain at least 3 distinct points.");
        }

        var longitudeSpan = Vertices.Max(v => v.Longitude) - Vertices.Min(v => v.Longitude);
        if (longitudeSpan >= 180)
        {
            errors.Add("polygon must span less than 180 degrees of longitude (antimeridian crossing is not supported).");
        }
    }

    private static bool IsOnSegment(GeoPoint p, GeoPoint a, GeoPoint b)
    {
        var cross = ((b.Longitude - a.Longitude) * (p.Latitude - a.Latitude)) -
                    ((b.Latitude - a.Latitude) * (p.Longitude - a.Longitude));
        var length = Math.Sqrt(Math.Pow(b.Longitude - a.Longitude, 2) + Math.Pow(b.Latitude - a.Latitude, 2));
        if (length == 0)
        {
            return Math.Abs(p.Latitude - a.Latitude) <= EdgeToleranceDegrees &&
                   Math.Abs(p.Longitude - a.Longitude) <= EdgeToleranceDegrees;
        }

        if (Math.Abs(cross) / length > EdgeToleranceDegrees)
        {
            return false;
        }

        return p.Longitude >= Math.Min(a.Longitude, b.Longitude) - EdgeToleranceDegrees &&
               p.Longitude <= Math.Max(a.Longitude, b.Longitude) + EdgeToleranceDegrees &&
               p.Latitude >= Math.Min(a.Latitude, b.Latitude) - EdgeToleranceDegrees &&
               p.Latitude <= Math.Max(a.Latitude, b.Latitude) + EdgeToleranceDegrees;
    }
}
