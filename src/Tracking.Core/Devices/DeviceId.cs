using System.Diagnostics.CodeAnalysis;
using System.Text.RegularExpressions;

namespace Tracking.Core.Devices;

/// <summary>
/// Device ids become part of Redis keys and geo set members, so they are restricted to a small,
/// unambiguous alphabet.
/// </summary>
public static partial class DeviceId
{
    public const int MaxLength = 64;

    public static bool IsValid([NotNullWhen(true)] string? deviceId) => deviceId is not null && Pattern().IsMatch(deviceId);

    [GeneratedRegex("^[A-Za-z0-9_.:-]{1,64}$")]
    private static partial Regex Pattern();
}
