using System.ComponentModel.DataAnnotations;

namespace Tracking.Processor;

public sealed class ProcessorOptions
{
    public const string SectionName = "Processor";

    /// <summary>
    /// Consumer name within the group. It must be unique per running instance and stable across
    /// restarts of the same instance, so the default is the host name (the pod name in Kubernetes).
    /// </summary>
    [Required]
    public string ConsumerName { get; set; } = Environment.MachineName;

    [Range(1, 10_000)]
    public int BatchSize { get; set; } = 100;

    /// <summary>Delay after an empty read; doubles up to <see cref="MaxIdleDelay"/> while the stream stays empty.</summary>
    [Range(typeof(TimeSpan), "00:00:00.001", "00:01:00")]
    public TimeSpan MinIdleDelay { get; set; } = TimeSpan.FromMilliseconds(50);

    [Range(typeof(TimeSpan), "00:00:00.001", "00:01:00")]
    public TimeSpan MaxIdleDelay { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>Delay before retrying after Redis or processing errors.</summary>
    [Range(typeof(TimeSpan), "00:00:00.001", "00:05:00")]
    public TimeSpan ErrorDelay { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>How long an entry must sit unacknowledged on another consumer before it is claimed.</summary>
    [Range(typeof(TimeSpan), "00:00:01", "01:00:00")]
    public TimeSpan ClaimMinIdle { get; set; } = TimeSpan.FromSeconds(30);

    [Range(typeof(TimeSpan), "00:00:01", "01:00:00")]
    public TimeSpan ClaimInterval { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>How often the geofence version counter is checked for changes.</summary>
    [Range(typeof(TimeSpan), "00:00:00", "00:10:00")]
    public TimeSpan GeofenceRefreshInterval { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>Optimistic commit attempts per ping before giving up on it for this delivery.</summary>
    [Range(1, 100)]
    public int MaxCommitAttempts { get; set; } = 5;

    /// <summary>
    /// Failed processing attempts, counted by this consumer, after which an entry is moved to the
    /// dead-letter stream. Transient Redis errors do not count.
    /// </summary>
    [Range(1, 1_000)]
    public int MaxDeliveryAttempts { get; set; } = 5;

    /// <summary>
    /// Deliveries, as counted by Redis for the whole group, after which an entry is dead-lettered
    /// before it is processed again. This catches entries that never fail cleanly, such as one that
    /// crashes the process or keeps a consumer from acknowledging it before it is claimed elsewhere.
    /// </summary>
    [Range(2, 10_000)]
    public int MaxDeliveryCount { get; set; } = 20;

    /// <summary>How often the consumer group backlog is sampled for the lag metrics.</summary>
    [Range(typeof(TimeSpan), "00:00:01", "00:10:00")]
    public TimeSpan LagSampleInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>The worker is reported unhealthy if its loop has not completed an iteration for this long.</summary>
    [Range(typeof(TimeSpan), "00:00:05", "01:00:00")]
    public TimeSpan StallTimeout { get; set; } = TimeSpan.FromMinutes(1);
}
