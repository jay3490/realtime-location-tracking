using System.ComponentModel.DataAnnotations;

namespace Tracking.Infrastructure.Redis;

public sealed class RedisOptions
{
    public const string SectionName = "Redis";

    /// <summary>A StackExchange.Redis configuration string, e.g. <c>localhost:6379</c>.</summary>
    [Required]
    public string ConnectionString { get; set; } = "localhost:6379";

    /// <summary>Prefix for every key, so several deployments can share one Redis.</summary>
    [Required]
    [RegularExpression("^[A-Za-z0-9_.-]+$")]
    public string KeyPrefix { get; set; } = "tracking";

    [Required]
    public string ConsumerGroup { get; set; } = "processors";

    /// <summary>Consumer group on the analytics outbox, shared by the relays that forward it to Kafka.</summary>
    [Required]
    public string AnalyticsRelayGroup { get; set; } = "analytics-relay";

    /// <summary>
    /// Approximate cap on the ping stream (<c>XADD MAXLEN ~</c>). Trimming removes the oldest entries
    /// whether or not they were processed, so this must exceed the backlog a processor outage can build.
    /// </summary>
    [Range(1_000, int.MaxValue)]
    public int PingStreamMaxLength { get; set; } = 1_000_000;

    /// <summary>
    /// Trim with <c>MAXLEN ~</c> (whole radix-tree nodes, cheap, may overshoot slightly) rather than an
    /// exact <c>MAXLEN</c>. Exact trimming costs more per append and is rarely worth it.
    /// </summary>
    public bool PingStreamTrimApproximate { get; set; } = true;

    [Range(100, int.MaxValue)]
    public int AlertLogMaxLength { get; set; } = 10_000;

    /// <summary>Attempts per connect cycle before the multiplexer falls back to its reconnect policy.</summary>
    [Range(1, 100)]
    public int ConnectRetry { get; set; } = 3;

    [Range(typeof(TimeSpan), "00:00:00.100", "00:01:00")]
    public TimeSpan ConnectTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>How long one command may take before it fails with a timeout.</summary>
    [Range(typeof(TimeSpan), "00:00:00.050", "00:01:00")]
    public TimeSpan OperationTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>Idle interval after which a keep-alive is sent, so dead connections are noticed.</summary>
    [Range(typeof(TimeSpan), "00:00:01", "00:10:00")]
    public TimeSpan KeepAlive { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>First delay of the exponential reconnect back-off.</summary>
    [Range(typeof(TimeSpan), "00:00:00.010", "00:01:00")]
    public TimeSpan ReconnectBaseDelay { get; set; } = TimeSpan.FromMilliseconds(500);

    /// <summary>Upper bound of the exponential reconnect back-off.</summary>
    [Range(typeof(TimeSpan), "00:00:00.100", "00:10:00")]
    public TimeSpan ReconnectMaxDelay { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Fail commands immediately while disconnected instead of queueing them until they time out.
    /// </summary>
    public bool FailFastWhenDisconnected { get; set; } = true;

    /// <summary>
    /// Approximate cap on the analytics outbox. Like the ping stream, it must exceed the backlog a
    /// Kafka outage can build, or the oldest unrelayed events are trimmed.
    /// </summary>
    [Range(1_000, int.MaxValue)]
    public int AnalyticsOutboxMaxLength { get; set; } = 1_000_000;

    /// <summary>Approximate cap on the dead-letter stream.</summary>
    [Range(100, int.MaxValue)]
    public int DeadLetterMaxLength { get; set; } = 100_000;
}
