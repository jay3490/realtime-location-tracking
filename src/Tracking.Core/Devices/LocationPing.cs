using Tracking.Core.Geo;

namespace Tracking.Core.Devices;

/// <summary>
/// One position report from a device, as accepted by the ingest service.
/// </summary>
public sealed record LocationPing(
    string DeviceId,
    GeoPoint Position,
    DateTimeOffset RecordedAt,
    double? AccuracyMeters = null);
