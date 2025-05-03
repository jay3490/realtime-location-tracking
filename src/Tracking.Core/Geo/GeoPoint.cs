using System.Text.Json.Serialization;

namespace Tracking.Core.Geo;

/// <summary>
/// A WGS84 coordinate expressed in decimal degrees.
/// </summary>
public readonly record struct GeoPoint(double Latitude, double Longitude)
{
    [JsonIgnore]
    public bool IsValid =>
        double.IsFinite(Latitude) && double.IsFinite(Longitude) &&
        Latitude is >= -90 and <= 90 &&
        Longitude is >= -180 and <= 180;

    public override string ToString() => FormattableString.Invariant($"({Latitude:F6}, {Longitude:F6})");
}
