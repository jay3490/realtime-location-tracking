using System.Collections.Concurrent;
using System.Threading.Channels;
using Tracking.Contracts;
using Tracking.Contracts.V1;

namespace Tracking.Ingest.Services;

/// <summary>
/// Maps device ids to the outbound queue of their open stream on this ingest instance, so alerts
/// can be routed to the right connection.
/// </summary>
/// <remarks>
/// A device has at most one registered session: when it reconnects, the newest stream takes over
/// alert delivery and the older one keeps receiving only its own acks until it closes.
/// </remarks>
public sealed class DeviceSessionRegistry
{
    private readonly ConcurrentDictionary<string, DeviceSession> _sessions = new(StringComparer.Ordinal);

    public int Count => _sessions.Count;

    public DeviceSession Register(string deviceId, ChannelWriter<ServerMessage> outbox)
    {
        var session = new DeviceSession(this, deviceId, outbox);
        _sessions[deviceId] = session;
        return session;
    }

    /// <summary>
    /// Queues an alert for the device's stream without waiting. Returns false when the device has
    /// no stream on this instance or its queue is full.
    /// </summary>
    public bool TryDeliver(Core.Geofencing.GeofenceAlert alert)
    {
        ArgumentNullException.ThrowIfNull(alert);
        return _sessions.TryGetValue(alert.DeviceId, out var session) &&
               session.Outbox.TryWrite(new ServerMessage { Alert = alert.ToContract() });
    }

    private void Unregister(DeviceSession session) =>
        _sessions.TryRemove(new KeyValuePair<string, DeviceSession>(session.DeviceId, session));

    public sealed class DeviceSession : IDisposable
    {
        private readonly DeviceSessionRegistry _registry;

        internal DeviceSession(DeviceSessionRegistry registry, string deviceId, ChannelWriter<ServerMessage> outbox)
        {
            _registry = registry;
            DeviceId = deviceId;
            Outbox = outbox;
        }

        public string DeviceId { get; }

        internal ChannelWriter<ServerMessage> Outbox { get; }

        /// <summary>Removes this session, unless a newer one for the same device replaced it.</summary>
        public void Dispose() => _registry.Unregister(this);
    }
}
