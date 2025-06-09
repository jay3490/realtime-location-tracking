namespace Tracking.Analytics.Data;

/// <summary>
/// The idempotency ledger: one row per analytics event already applied. Kafka delivery is at least
/// once, so an event is applied only if inserting its id here succeeds in the same transaction.
/// </summary>
public sealed class ProcessedEvent
{
    public required string EventId { get; init; }

    public DateTime ProcessedAt { get; init; }
}

/// <summary>
/// Distance travelled by one device on one UTC day. A movement between two fixes is attributed to the
/// day of the later fix.
/// </summary>
public sealed class DeviceDailyDistance
{
    public required string DeviceId { get; init; }

    public DateOnly Day { get; init; }

    public double DistanceMeters { get; set; }

    /// <summary>Committed positions that day.</summary>
    public int Positions { get; set; }

    public DateTime FirstRecordedAt { get; set; }

    public DateTime LastRecordedAt { get; set; }
}

/// <summary>
/// One stay of a device inside a geofence. Enter and exit events can arrive in either order, so a
/// visit may exist with only one end known until the other arrives.
/// </summary>
public sealed class GeofenceVisit
{
    public long Id { get; init; }

    public required string DeviceId { get; init; }

    public required string GeofenceId { get; init; }

    public required string GeofenceName { get; set; }

    /// <summary>Null while only the exit has been seen.</summary>
    public DateTime? EnteredAt { get; set; }

    /// <summary>Null while the device is (as far as has been seen) still inside.</summary>
    public DateTime? ExitedAt { get; set; }
}

/// <summary>A completed visit that lasted at least the configured dwell threshold.</summary>
public sealed class DwellEvent
{
    public long Id { get; init; }

    public required string DeviceId { get; init; }

    public required string GeofenceId { get; init; }

    public required string GeofenceName { get; init; }

    public DateTime EnteredAt { get; init; }

    public DateTime ExitedAt { get; init; }

    public double DurationSeconds { get; init; }
}
