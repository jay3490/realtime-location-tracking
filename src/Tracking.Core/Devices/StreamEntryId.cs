using System.Globalization;

namespace Tracking.Core.Devices;

/// <summary>
/// Redis stream entry ids have the form <c>&lt;unix-ms&gt;-&lt;sequence&gt;</c>, where the first part is the
/// server time the entry was appended. That makes every id a free append timestamp.
/// </summary>
public static class StreamEntryId
{
    public static bool TryGetTimestamp(string? entryId, out DateTimeOffset appendedAt)
    {
        appendedAt = default;
        if (string.IsNullOrEmpty(entryId))
        {
            return false;
        }

        var dash = entryId.IndexOf('-', StringComparison.Ordinal);
        var millisText = dash < 0 ? entryId.AsSpan() : entryId.AsSpan(0, dash);
        if (!long.TryParse(millisText, NumberStyles.None, CultureInfo.InvariantCulture, out var millis) ||
            millis > DateTimeOffset.MaxValue.ToUnixTimeMilliseconds())
        {
            return false;
        }

        appendedAt = DateTimeOffset.FromUnixTimeMilliseconds(millis);
        return true;
    }
}
