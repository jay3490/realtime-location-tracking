using System.ComponentModel.DataAnnotations;
using Confluent.Kafka;

namespace Tracking.Kafka;

/// <summary>Kafka connection, topic and producer settings shared by the relay and the analytics consumer.</summary>
public sealed class KafkaOptions
{
    public const string SectionName = "Kafka";

    [Required]
    public string BootstrapServers { get; set; } = "localhost:9092";

    /// <summary>Topic for enriched position events, keyed by device id.</summary>
    [Required]
    [RegularExpression("^[A-Za-z0-9._-]{1,249}$")]
    public string PositionsTopic { get; set; } = "tracking.positions.v1";

    /// <summary>Topic for geofence enter/exit events, keyed by device id.</summary>
    [Required]
    [RegularExpression("^[A-Za-z0-9._-]{1,249}$")]
    public string GeofenceEventsTopic { get; set; } = "tracking.geofence-events.v1";

    /// <summary>
    /// Broker acknowledgements a produce waits for: <c>All</c> (every in-sync replica), <c>Leader</c> or
    /// <c>None</c>. The idempotent producer requires <c>All</c>.
    /// </summary>
    public Acks Acks { get; set; } = Acks.All;

    /// <summary>
    /// Idempotent producer: the broker discards duplicates caused by producer retries and keeps
    /// per-partition order, so retries never reorder or duplicate a device's events.
    /// </summary>
    public bool EnableIdempotence { get; set; } = true;

    /// <summary>How long the producer waits to fill a batch before sending it.</summary>
    [Range(0, 1_000)]
    public int LingerMs { get; set; } = 5;

    public CompressionType Compression { get; set; } = CompressionType.Lz4;

    /// <summary>Upper bound on how long a produce may be retried before it is reported as failed.</summary>
    [Range(typeof(TimeSpan), "00:00:01", "00:30:00")]
    public TimeSpan DeliveryTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Client id reported to the brokers; defaults to the host name.</summary>
    public string? ClientId { get; set; }

    /// <summary>
    /// Extra librdkafka properties passed through unchanged, for example <c>security.protocol</c>,
    /// <c>sasl.mechanism</c> or <c>ssl.ca.location</c>.
    /// </summary>
    public Dictionary<string, string> Properties { get; } = new(StringComparer.Ordinal);

    public string TopicFor(Core.Analytics.AnalyticsEventKind kind) => kind switch
    {
        Core.Analytics.AnalyticsEventKind.Position => PositionsTopic,
        Core.Analytics.AnalyticsEventKind.Geofence => GeofenceEventsTopic,
        _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unknown analytics event kind."),
    };

    /// <summary>Settings both producer and consumer configurations start from.</summary>
    internal ClientConfig CreateClientConfig()
    {
        var config = new ClientConfig(new Dictionary<string, string>(Properties, StringComparer.Ordinal))
        {
            BootstrapServers = BootstrapServers,
            ClientId = ClientId ?? Environment.MachineName,
        };
        return config;
    }

    public ProducerConfig CreateProducerConfig() => new(CreateClientConfig())
    {
        Acks = Acks,
        EnableIdempotence = EnableIdempotence,
        LingerMs = LingerMs,
        CompressionType = Compression,
        MessageTimeoutMs = (int)DeliveryTimeout.TotalMilliseconds,

        // Same partition for the same key as the Java client, so other producers and tools agree.
        Partitioner = Partitioner.Murmur2Random,
    };
}
