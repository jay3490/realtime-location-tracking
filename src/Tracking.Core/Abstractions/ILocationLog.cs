using Tracking.Core.Devices;

namespace Tracking.Core.Abstractions;

/// <summary>
/// The durable, append-only log that ingest writes accepted pings to.
/// </summary>
public interface ILocationLog
{
    /// <summary>Appends a ping and returns the id of the new log entry.</summary>
    Task<string> AppendAsync(LocationPing ping, CancellationToken cancellationToken = default);
}
