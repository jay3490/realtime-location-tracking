using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
using Tracking.Core.Geo;

namespace Tracking.Core.Geofencing;

/// <summary>
/// A named region that devices can enter and leave.
/// </summary>
[JsonPolymorphic(TypeDiscriminatorPropertyName = "type")]
[JsonDerivedType(typeof(CircleGeofence), "circle")]
[JsonDerivedType(typeof(PolygonGeofence), "polygon")]
public abstract partial record Geofence(string Id, string Name)
{
    public const int MaxIdLength = 64;
    public const int MaxNameLength = 200;

    public abstract bool Contains(GeoPoint point);

    /// <summary>
    /// Returns every problem with this definition; an empty list means it is valid.
    /// </summary>
    public IReadOnlyList<string> Validate()
    {
        var errors = new List<string>();

        if (!IsValidId(Id))
        {
            errors.Add($"id must be 1-{MaxIdLength} characters of letters, digits, '-' or '_'.");
        }

        if (string.IsNullOrWhiteSpace(Name) || Name.Length > MaxNameLength)
        {
            errors.Add($"name must be 1-{MaxNameLength} characters.");
        }

        ValidateShape(errors);
        return errors;
    }

    public static bool IsValidId(string? id) => id is not null && IdPattern().IsMatch(id);

    protected abstract void ValidateShape(List<string> errors);

    [GeneratedRegex("^[A-Za-z0-9_-]{1,64}$")]
    private static partial Regex IdPattern();
}
