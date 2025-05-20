using Tracking.Core.Abstractions;
using Tracking.Core.Devices;
using Tracking.Core.Geo;
using Tracking.Core.Geofencing;

namespace Tracking.Testing;

public sealed class ManualClock(DateTimeOffset start) : TimeProvider
{
    public DateTimeOffset Now { get; private set; } = start;

    public override DateTimeOffset GetUtcNow() => Now;

    public void Advance(TimeSpan by) => Now += by;
}

/// <summary>
/// In-memory model of one Redis stream with one consumer group: a delivery cursor, a pending
/// entries list with owner and delivery time, acknowledgement and claiming by idle time.
/// </summary>
public sealed class InMemoryConsumerGroup(ManualClock clock) : ILocationConsumerGroup
{
    private readonly List<LogEntry> _entries = [];
    private readonly Dictionary<string, (string Consumer, DateTimeOffset DeliveredAt)> _pending = new(StringComparer.Ordinal);
    private int _delivered;
    private long _nextId;

    public List<(LogEntry Entry, string Reason)> DeadLetters { get; } = [];

    public HashSet<string> Acknowledged { get; } = new(StringComparer.Ordinal);

    public IReadOnlyCollection<string> PendingIds => _pending.Keys;

    public bool GroupCreated { get; private set; }

    public string Append(LocationPing ping) =>
        AppendRaw(LocationPingCodec.Encode(ping).ToDictionary(kv => kv.Key, kv => kv.Value, StringComparer.Ordinal));

    public string AppendRaw(IReadOnlyDictionary<string, string> fields)
    {
        var id = $"{++_nextId}-0";
        _entries.Add(new LogEntry(id, fields));
        return id;
    }

    public IReadOnlyList<string> PendingFor(string consumer) =>
        _pending.Where(p => p.Value.Consumer == consumer).Select(p => p.Key).Order(StringComparer.Ordinal).ToArray();

    public Task EnsureConsumerGroupAsync(CancellationToken cancellationToken = default)
    {
        GroupCreated = true;
        return Task.CompletedTask;
    }

    public Task<IReadOnlyList<LogEntry>> ReadNewAsync(string consumer, int count, CancellationToken cancellationToken = default)
    {
        var batch = _entries.Skip(_delivered).Take(count).ToArray();
        _delivered += batch.Length;
        foreach (var entry in batch)
        {
            _pending[entry.Id] = (consumer, clock.Now);
        }

        return Task.FromResult<IReadOnlyList<LogEntry>>(batch);
    }

    public Task<IReadOnlyList<LogEntry>> ReadOwnPendingAsync(string consumer, string afterId, int count, CancellationToken cancellationToken = default)
    {
        var batch = _entries
            .Where(e => _pending.TryGetValue(e.Id, out var p) && p.Consumer == consumer && Sequence(e.Id) > Sequence(afterId))
            .Take(count)
            .ToArray();
        foreach (var entry in batch)
        {
            _pending[entry.Id] = (consumer, clock.Now);
        }

        return Task.FromResult<IReadOnlyList<LogEntry>>(batch);
    }

    public Task<ClaimResult> ClaimAbandonedAsync(string consumer, TimeSpan minIdle, string startId, int count, CancellationToken cancellationToken = default)
    {
        var candidates = _entries
            .Where(e => _pending.ContainsKey(e.Id) && Sequence(e.Id) >= Sequence(startId))
            .ToArray();
        var scanned = candidates.Take(count).ToArray();
        var claimed = scanned.Where(e => clock.Now - _pending[e.Id].DeliveredAt >= minIdle).ToArray();
        foreach (var entry in claimed)
        {
            _pending[entry.Id] = (consumer, clock.Now);
        }

        var next = candidates.Length > count ? candidates[count].Id : "0-0";
        return Task.FromResult(new ClaimResult(next, claimed));
    }

    public Task AcknowledgeAsync(IReadOnlyCollection<string> entryIds, CancellationToken cancellationToken = default)
    {
        foreach (var id in entryIds)
        {
            if (_pending.Remove(id))
            {
                Acknowledged.Add(id);
            }
        }

        return Task.CompletedTask;
    }

    public Task<ConsumerGroupStatus> GetStatusAsync(CancellationToken cancellationToken = default) =>
        Task.FromResult(new ConsumerGroupStatus(_entries.Count, _entries.Count - _delivered, _pending.Count));

    public Task DeadLetterAsync(LogEntry entry, string reason, CancellationToken cancellationToken = default)
    {
        DeadLetters.Add((entry, reason));
        return AcknowledgeAsync([entry.Id], cancellationToken);
    }

    private static long Sequence(string id) => long.Parse(id.AsSpan(0, id.IndexOf('-', StringComparison.Ordinal)), provider: null);
}

public sealed class InMemoryDeviceStateStore : IDeviceStateStore
{
    private readonly Dictionary<string, DeviceState> _states = new(StringComparer.Ordinal);

    public List<GeofenceAlert> CommittedAlerts { get; } = [];

    public int Commits { get; private set; }

    /// <summary>Called before each commit; return false to simulate a racing writer.</summary>
    public Func<DeviceState, bool>? BeforeCommit { get; set; }

    /// <summary>When set, every read fails with this exception.</summary>
    public Exception? ReadFailure { get; set; }

    public DeviceState? this[string deviceId] => _states.GetValueOrDefault(deviceId);

    public Task<DeviceState?> GetAsync(string deviceId, CancellationToken cancellationToken = default) =>
        ReadFailure is { } failure
            ? Task.FromException<DeviceState?>(failure)
            : Task.FromResult(_states.GetValueOrDefault(deviceId));

    public Task<bool> TryCommitAsync(DeviceState? expected, DeviceState updated, IReadOnlyList<GeofenceAlert> alerts, CancellationToken cancellationToken = default)
    {
        if (BeforeCommit is { } hook && !hook(updated))
        {
            return Task.FromResult(false);
        }

        var stored = _states.GetValueOrDefault(updated.DeviceId);
        if (stored?.RecordedAt != expected?.RecordedAt)
        {
            return Task.FromResult(false);
        }

        _states[updated.DeviceId] = updated;
        CommittedAlerts.AddRange(alerts);
        Commits++;
        return Task.FromResult(true);
    }

    /// <summary>Stores state directly, as another processor would.</summary>
    public void Put(DeviceState state) => _states[state.DeviceId] = state;

    public Task<IReadOnlyList<NearbyDevice>> FindNearbyAsync(GeoPoint center, double radiusMeters, int limit, CancellationToken cancellationToken = default) =>
        Task.FromResult<IReadOnlyList<NearbyDevice>>(_states.Values
            .Select(s => new NearbyDevice(s.DeviceId, s.Position, GeoMath.HaversineMeters(center, s.Position), s.RecordedAt))
            .Where(n => n.DistanceMeters <= radiusMeters)
            .OrderBy(n => n.DistanceMeters)
            .Take(limit)
            .ToArray());
}

public sealed class InMemoryGeofenceStore : IGeofenceStore
{
    private readonly Dictionary<string, Geofence> _fences = new(StringComparer.Ordinal);
    private long _version;

    public int FullReads { get; private set; }

    public Task<IReadOnlyList<Geofence>> GetAllAsync(CancellationToken cancellationToken = default)
    {
        FullReads++;
        return Task.FromResult<IReadOnlyList<Geofence>>(_fences.Values.OrderBy(f => f.Id, StringComparer.Ordinal).ToArray());
    }

    public Task<Geofence?> GetAsync(string id, CancellationToken cancellationToken = default) =>
        Task.FromResult(_fences.GetValueOrDefault(id));

    public Task<bool> UpsertAsync(Geofence geofence, CancellationToken cancellationToken = default)
    {
        var created = !_fences.ContainsKey(geofence.Id);
        _fences[geofence.Id] = geofence;
        _version++;
        return Task.FromResult(created);
    }

    public Task<bool> TryCreateAsync(Geofence geofence, CancellationToken cancellationToken = default)
    {
        if (_fences.ContainsKey(geofence.Id))
        {
            return Task.FromResult(false);
        }

        return UpsertAsync(geofence, cancellationToken);
    }

    public Task<bool> DeleteAsync(string id, CancellationToken cancellationToken = default)
    {
        var removed = _fences.Remove(id);
        if (removed)
        {
            _version++;
        }

        return Task.FromResult(removed);
    }

    public Task<long> GetVersionAsync(CancellationToken cancellationToken = default) => Task.FromResult(_version);
}

public sealed class InMemoryAlertLog : IAlertLog
{
    private readonly List<GeofenceAlert> _alerts = [];

    public void Add(GeofenceAlert alert) => _alerts.Add(alert);

    public Task<IReadOnlyList<GeofenceAlert>> GetRecentAsync(int count, CancellationToken cancellationToken = default) =>
        Task.FromResult<IReadOnlyList<GeofenceAlert>>(Enumerable.Reverse(_alerts).Take(count).ToArray());
}
