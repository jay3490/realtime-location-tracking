using System.ComponentModel.DataAnnotations;

namespace Tracking.Ingest;

public sealed class IngestOptions
{
    public const string SectionName = "Ingest";

    /// <summary>How far ahead of the server clock a ping timestamp may be (device clock drift).</summary>
    [Range(typeof(TimeSpan), "00:00:00", "1.00:00:00")]
    public TimeSpan MaxClockSkew { get; set; } = TimeSpan.FromMinutes(2);

    /// <summary>Pings older than this are rejected rather than stored.</summary>
    [Range(typeof(TimeSpan), "00:00:01", "30.00:00:00")]
    public TimeSpan MaxPingAge { get; set; } = TimeSpan.FromHours(24);

    /// <summary>
    /// Messages buffered per connection before acks apply back-pressure to the device's own stream
    /// and further alerts for it are dropped.
    /// </summary>
    [Range(8, 100_000)]
    public int OutboxCapacity { get; set; } = 256;
}
