using Tracking.Core.Geo;

namespace Tracking.Core.Devices;

/// <summary>
/// The latest known state of a device, derived from its most recent in-order ping.
/// </summary>
/// <param name="SpeedMetersPerSecond">Average speed since the previous ping; null for the first ping.</param>
/// <param name="HeadingDegrees">Direction of travel clockwise from north; null until the device has moved.</param>
/// <param name="InsideGeofenceIds">Ids of the geofences the device is currently inside, sorted ordinally.</param>
public sealed record DeviceState(
    string DeviceId,
    GeoPoint Position,
    DateTimeOffset RecordedAt,
    double? SpeedMetersPerSecond,
    double? HeadingDegrees,
    IReadOnlyList<string> InsideGeofenceIds);
