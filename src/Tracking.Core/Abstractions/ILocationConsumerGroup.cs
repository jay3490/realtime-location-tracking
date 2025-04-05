namespace Tracking.Core.Abstractions;

/// <summary>A raw entry read from the location log.</summary>
public sealed record LogEntry(string Id, IReadOnlyDictionary<string, string> Fields);

/// <summary>
/// The outcome of claiming entries that other consumers read but never acknowledged.
/// </summary>
/// <param name="NextStartId">Cursor for the next claim scan; "0-0" once the pending list has been fully scanned.</param>
public sealed record ClaimResult(string NextStartId, IReadOnlyList<LogEntry> Entries);

/// <summary>
/// Consumer-group access to the location log, with at-least-once delivery: an entry stays
/// pending against the consumer that read it until that consumer, or one that claims it,
/// acknowledges it.
/// </summary>
public interface ILocationConsumerGroup
{
    /// <summary>Creates the consumer group (and the stream) if they do not exist yet.</summary>
    Task EnsureConsumerGroupAsync(CancellationToken cancellationToken = default);

    /// <summary>Reads entries never delivered to any consumer in the group.</summary>
    Task<IReadOnlyList<LogEntry>> ReadNewAsync(string consumer, int count, CancellationToken cancellationToken = default);

    /// <summary>
    /// Re-reads entries already delivered to <paramref name="consumer"/> but not acknowledged,
    /// starting after <paramref name="afterId"/>. Used on startup to finish work from a previous run.
    /// </summary>
    Task<IReadOnlyList<LogEntry>> ReadOwnPendingAsync(string consumer, string afterId, int count, CancellationToken cancellationToken = default);

    /// <summary>
    /// Transfers to <paramref name="consumer"/> entries that have been pending on any consumer for
    /// at least <paramref name="minIdle"/>, scanning from <paramref name="startId"/>.
    /// </summary>
    Task<ClaimResult> ClaimAbandonedAsync(string consumer, TimeSpan minIdle, string startId, int count, CancellationToken cancellationToken = default);

    Task AcknowledgeAsync(IReadOnlyCollection<string> entryIds, CancellationToken cancellationToken = default);

    /// <summary>Copies an unprocessable entry to the dead-letter stream and acknowledges it.</summary>
    Task DeadLetterAsync(LogEntry entry, string reason, CancellationToken cancellationToken = default);
}
