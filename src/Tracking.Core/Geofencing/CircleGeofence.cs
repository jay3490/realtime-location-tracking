using Tracking.Core.Geo;

namespace Tracking.Core.Geofencing;

/// <summary>
/// Every point within <see cref="RadiusMeters"/> great-circle metres of <see cref="Center"/>.
/// A point exactly on the boundary counts as inside.
/// </summary>
public sealed record CircleGeofence(string Id, string Name, GeoPoint Center, double RadiusMeters)
    : Geofence(Id, Name)
{
    /// <summary>Upper bound that keeps a circle well inside one hemisphere.</summary>
    public const double MaxRadiusMeters = 1_000_000;

    public override bool Contains(GeoPoint point) => GeoMath.HaversineMeters(Center, point) <= RadiusMeters;

    protected override void ValidateShape(List<string> errors)
    {
        if (!Center.IsValid)
        {
            errors.Add("center must have latitude in [-90, 90] and longitude in [-180, 180].");
        }

        if (!double.IsFinite(RadiusMeters) || RadiusMeters <= 0 || RadiusMeters > MaxRadiusMeters)
        {
            errors.Add($"radiusMeters must be greater than 0 and at most {MaxRadiusMeters:0}.");
        }
    }
}
