using Tracking.Core.Devices;

namespace Tracking.Core.Abstractions;

/// <summary>
/// The durable, append-only log that ingest writes accepted pings to.
/// </summary>
public interface ILocationLog
{
    /// <summary>Appends a ping and returns the id of the new log entry.</summary>
    Task<string> AppendAsync(LocationPing ping, CancellationToken cancellationToken = default);

    /// <summary>
    /// Appends pings in order as one pipelined round trip. The result has one element per ping:
    /// the new entry id, or null when that append failed (the others may still have succeeded).
    /// Throws only when nothing could be sent at all.
    /// </summary>
    Task<IReadOnlyList<string?>> AppendBatchAsync(IReadOnlyList<LocationPing> pings, CancellationToken cancellationToken = default);
}
