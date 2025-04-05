using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using Tracking.Core.Geo;

namespace Tracking.Core.Devices;

/// <summary>
/// Converts pings to and from the flat field/value pairs stored in a stream entry.
/// </summary>
/// <remarks>
/// Fields are kept flat (rather than one JSON blob) so entries stay readable with plain
/// <c>XRANGE</c>. Timestamps are Unix milliseconds; numbers use the invariant culture with
/// round-trip formatting so no precision is lost.
/// </remarks>
public static class LocationPingCodec
{
    public const string DeviceField = "device";
    public const string LatitudeField = "lat";
    public const string LongitudeField = "lon";
    public const string RecordedAtField = "ts";
    public const string AccuracyField = "acc";

    public static IReadOnlyList<KeyValuePair<string, string>> Encode(LocationPing ping)
    {
        ArgumentNullException.ThrowIfNull(ping);

        var fields = new List<KeyValuePair<string, string>>(5)
        {
            new(DeviceField, ping.DeviceId),
            new(LatitudeField, Format(ping.Position.Latitude)),
            new(LongitudeField, Format(ping.Position.Longitude)),
            new(RecordedAtField, ping.RecordedAt.ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture)),
        };
        if (ping.AccuracyMeters is { } accuracy)
        {
            fields.Add(new(AccuracyField, Format(accuracy)));
        }

        return fields;
    }

    public static bool TryDecode(
        IReadOnlyDictionary<string, string> fields,
        [NotNullWhen(true)] out LocationPing? ping,
        [NotNullWhen(false)] out string? error)
    {
        ArgumentNullException.ThrowIfNull(fields);
        ping = null;

        if (!fields.TryGetValue(DeviceField, out var deviceId) || string.IsNullOrWhiteSpace(deviceId))
        {
            error = $"missing '{DeviceField}'";
            return false;
        }

        if (!TryParseDouble(fields, LatitudeField, out var latitude) ||
            !TryParseDouble(fields, LongitudeField, out var longitude))
        {
            error = $"missing or malformed '{LatitudeField}'/'{LongitudeField}'";
            return false;
        }

        var position = new GeoPoint(latitude, longitude);
        if (!position.IsValid)
        {
            error = $"coordinates out of range {position}";
            return false;
        }

        if (!fields.TryGetValue(RecordedAtField, out var tsText) ||
            !long.TryParse(tsText, NumberStyles.Integer, CultureInfo.InvariantCulture, out var unixMs) ||
            unixMs < 0 || unixMs > DateTimeOffset.MaxValue.ToUnixTimeMilliseconds())
        {
            error = $"missing or malformed '{RecordedAtField}'";
            return false;
        }

        double? accuracy = TryParseDouble(fields, AccuracyField, out var acc) ? acc : null;
        ping = new LocationPing(deviceId, position, DateTimeOffset.FromUnixTimeMilliseconds(unixMs), accuracy);
        error = null;
        return true;
    }

    private static string Format(double value) => value.ToString("R", CultureInfo.InvariantCulture);

    private static bool TryParseDouble(IReadOnlyDictionary<string, string> fields, string name, out double value)
    {
        value = 0;
        return fields.TryGetValue(name, out var text) &&
               double.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out value) &&
               double.IsFinite(value);
    }
}
