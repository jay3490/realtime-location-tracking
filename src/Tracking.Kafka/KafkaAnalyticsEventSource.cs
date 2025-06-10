using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Tracking.Core;
using Tracking.Core.Analytics;

namespace Tracking.Kafka;

/// <summary>
/// One consumed message: the decoded event (null when the payload was unusable, with the reason in
/// <see cref="Error"/>) and the position to commit once it has been handled.
/// </summary>
public sealed record ConsumedEvent(AnalyticsEvent? Event, TopicPartitionOffset Position, string? Error = null);

/// <summary>Batches of analytics events from Kafka, with offsets committed only when the caller says so.</summary>
public interface IAnalyticsEventSource : IDisposable
{
    /// <summary>
    /// Blocks until at least one message is available or <paramref name="maxWait"/> passes, then returns
    /// up to <paramref name="maxCount"/> messages that are already waiting.
    /// </summary>
    IReadOnlyList<ConsumedEvent> ConsumeBatch(int maxCount, TimeSpan maxWait, CancellationToken cancellationToken);

    /// <summary>Commits the offsets after every message in <paramref name="batch"/>.</summary>
    void Commit(IReadOnlyList<ConsumedEvent> batch);
}

public sealed class AnalyticsConsumerOptions
{
    public const string SectionName = "AnalyticsConsumer";

    /// <summary>Kafka consumer group shared by every analytics instance.</summary>
    public string GroupId { get; set; } = "tracking-analytics";

    /// <summary>Most events applied (and committed) in one database transaction.</summary>
    [System.ComponentModel.DataAnnotations.Range(1, 10_000)]
    public int BatchSize { get; set; } = 500;

    /// <summary>How long to wait for the first message of a batch.</summary>
    [System.ComponentModel.DataAnnotations.Range(typeof(TimeSpan), "00:00:00.010", "00:01:00")]
    public TimeSpan PollTimeout { get; set; } = TimeSpan.FromSeconds(1);
}

/// <summary>
/// A Kafka consumer over the position and geofence topics. Offsets are never committed automatically:
/// the caller commits a batch after its effects are stored, which makes delivery at least once.
/// </summary>
public sealed partial class KafkaAnalyticsEventSource : IAnalyticsEventSource
{
    private readonly IConsumer<string, string> _consumer;
    private readonly KafkaOptions _kafka;
    private readonly ILogger<KafkaAnalyticsEventSource> _logger;

    public KafkaAnalyticsEventSource(IOptions<KafkaOptions> kafka, IOptions<AnalyticsConsumerOptions> options, ILogger<KafkaAnalyticsEventSource> logger)
    {
        ArgumentNullException.ThrowIfNull(kafka);
        ArgumentNullException.ThrowIfNull(options);
        _kafka = kafka.Value;
        _logger = logger;

        _consumer = new ConsumerBuilder<string, string>(CreateConsumerConfig(_kafka, options.Value))
            .SetErrorHandler((_, error) => LogClientError(error.Code, error.Reason, error.IsFatal))
            .SetPartitionsAssignedHandler((_, partitions) => LogAssigned(string.Join(", ", partitions)))
            .SetPartitionsRevokedHandler((_, partitions) => LogRevoked(string.Join(", ", partitions)))
            .Build();
        _consumer.Subscribe([_kafka.PositionsTopic, _kafka.GeofenceEventsTopic]);
    }

    public static ConsumerConfig CreateConsumerConfig(KafkaOptions kafka, AnalyticsConsumerOptions options)
    {
        ArgumentNullException.ThrowIfNull(kafka);
        ArgumentNullException.ThrowIfNull(options);

        return new ConsumerConfig(kafka.CreateClientConfig())
        {
            GroupId = options.GroupId,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,

            // A new group starts from the oldest retained event, so history is not skipped.
            AutoOffsetReset = AutoOffsetReset.Earliest,

            // Rebalances move only the partitions that change owner instead of stopping the whole group.
            PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky,
        };
    }

    public IReadOnlyList<ConsumedEvent> ConsumeBatch(int maxCount, TimeSpan maxWait, CancellationToken cancellationToken)
    {
        var batch = new List<ConsumedEvent>(maxCount);
        var result = _consumer.Consume(maxWait);
        while (result is not null)
        {
            batch.Add(Decode(result));
            if (batch.Count >= maxCount || cancellationToken.IsCancellationRequested)
            {
                break;
            }

            // Take what has already been fetched without waiting again.
            result = _consumer.Consume(TimeSpan.Zero);
        }

        return batch;
    }

    public void Commit(IReadOnlyList<ConsumedEvent> batch)
    {
        ArgumentNullException.ThrowIfNull(batch);
        if (batch.Count == 0)
        {
            return;
        }

        // Kafka commits the offset of the next message to read, per partition.
        var offsets = batch
            .GroupBy(e => e.Position.TopicPartition)
            .Select(g => new TopicPartitionOffset(g.Key, new Offset(g.Max(e => e.Position.Offset.Value) + 1)))
            .ToArray();
        try
        {
            _consumer.Commit(offsets);
        }
        catch (KafkaException ex)
        {
            // Typically a partition moved to another instance mid-batch. Its new owner re-reads from the
            // last committed offset, and the ledger turns those replays into no-ops.
            LogCommitFailed(ex, ex.Error.Code);
        }
    }

    public void Dispose()
    {
        _consumer.Close();
        _consumer.Dispose();
    }

    private ConsumedEvent Decode(ConsumeResult<string, string> result)
    {
        try
        {
            AnalyticsEvent? analyticsEvent = result.Topic == _kafka.PositionsTopic
                ? JsonSerializer.Deserialize<PositionEvent>(result.Message.Value, TrackingJson.Options)
                : JsonSerializer.Deserialize<GeofenceEvent>(result.Message.Value, TrackingJson.Options);

            return analyticsEvent is { EventId.Length: > 0, DeviceId.Length: > 0 }
                ? new ConsumedEvent(analyticsEvent, result.TopicPartitionOffset)
                : new ConsumedEvent(null, result.TopicPartitionOffset, "event id or device id missing");
        }
        catch (JsonException ex)
        {
            return new ConsumedEvent(null, result.TopicPartitionOffset, ex.Message);
        }
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Assigned partitions: {Partitions}")]
    private partial void LogAssigned(string partitions);

    [LoggerMessage(Level = LogLevel.Information, Message = "Revoked partitions: {Partitions}")]
    private partial void LogRevoked(string partitions);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Kafka client error {Code}: {Reason} (fatal: {IsFatal})")]
    private partial void LogClientError(ErrorCode code, string reason, bool isFatal);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Could not commit offsets ({Code}); the events will be replayed and deduplicated")]
    private partial void LogCommitFailed(Exception exception, ErrorCode code);
}
