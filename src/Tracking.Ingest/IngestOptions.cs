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

    /// <summary>
    /// Pings read from one device stream but not yet appended. When it is full the stream stops
    /// reading, so HTTP/2 flow control slows the device down instead of the server buffering for it.
    /// </summary>
    [Range(1, 10_000)]
    public int InboundCapacity { get; set; } = 64;

    /// <summary>Pings queued for appending across all streams on this instance.</summary>
    [Range(1, 1_000_000)]
    public int AppendQueueCapacity { get; set; } = 4_096;

    /// <summary>
    /// How long shutdown waits for open streams to acknowledge what they have read and close. Keep it
    /// below the pod's termination grace period minus any preStop delay.
    /// </summary>
    [Range(typeof(TimeSpan), "00:00:01", "00:10:00")]
    public TimeSpan DrainTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Most pings sent to Redis in one pipelined batch.</summary>
    [Range(1, 10_000)]
    public int MaxBatchSize { get; set; } = 256;
}
