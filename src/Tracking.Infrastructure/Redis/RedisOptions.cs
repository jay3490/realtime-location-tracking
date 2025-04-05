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

    /// <summary>
    /// Approximate cap on the ping stream (<c>XADD MAXLEN ~</c>). Trimming removes the oldest entries
    /// whether or not they were processed, so this must exceed the backlog a processor outage can build.
    /// </summary>
    [Range(1_000, int.MaxValue)]
    public int PingStreamMaxLength { get; set; } = 1_000_000;

    [Range(100, int.MaxValue)]
    public int AlertLogMaxLength { get; set; } = 10_000;
}
