namespace Tracking.Core.Abstractions;

/// <summary>
/// Consumer-group access to the analytics outbox the processor writes in its commit transaction.
/// Entries stay pending until acknowledged, so an event is relayed at least once.
/// </summary>
public interface IAnalyticsOutbox
{
    Task EnsureConsumerGroupAsync(CancellationToken cancellationToken = default);

    Task<IReadOnlyList<LogEntry>> ReadNewAsync(string consumer, int count, CancellationToken cancellationToken = default);

    Task<IReadOnlyList<LogEntry>> ReadOwnPendingAsync(string consumer, string afterId, int count, CancellationToken cancellationToken = default);

    Task<ClaimResult> ClaimAbandonedAsync(string consumer, TimeSpan minIdle, string startId, int count, CancellationToken cancellationToken = default);

    Task AcknowledgeAsync(IReadOnlyCollection<string> entryIds, CancellationToken cancellationToken = default);
}

/// <summary>One outbox event ready to publish.</summary>
/// <param name="EntryId">Id of the outbox entry, used to acknowledge it once published.</param>
/// <param name="Key">Partitioning key: the device id, so one device's events share a partition.</param>
public sealed record OutboundEvent(string EntryId, Analytics.AnalyticsEventKind Kind, string Key, string EventId, string Payload);

/// <summary>Publishes analytics events to the durable analytics log (Kafka).</summary>
public interface IAnalyticsPublisher
{
    /// <summary>
    /// Publishes the events in order and waits for the broker to acknowledge them. Returns one flag
    /// per event: true once that event is durably stored, false if it must be retried.
    /// </summary>
    Task<IReadOnlyList<bool>> PublishAsync(IReadOnlyList<OutboundEvent> events, CancellationToken cancellationToken = default);
}
