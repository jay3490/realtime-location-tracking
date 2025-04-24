namespace Tracking.Simulator;

/// <summary>Counters shared by all simulated devices.</summary>
public sealed class SimulatorStats
{
    private long _sent;
    private long _accepted;
    private long _rejected;
    private long _alerts;
    private long _reconnects;

    public long Sent => Interlocked.Read(ref _sent);

    public long Accepted => Interlocked.Read(ref _accepted);

    public long Rejected => Interlocked.Read(ref _rejected);

    public long Alerts => Interlocked.Read(ref _alerts);

    public long Reconnects => Interlocked.Read(ref _reconnects);

    public void PingSent() => Interlocked.Increment(ref _sent);

    public void AckReceived(bool accepted) => Interlocked.Increment(ref accepted ? ref _accepted : ref _rejected);

    public void AlertReceived() => Interlocked.Increment(ref _alerts);

    public void Reconnected() => Interlocked.Increment(ref _reconnects);

    public override string ToString() =>
        $"sent={Sent} accepted={Accepted} rejected={Rejected} alerts={Alerts} reconnects={Reconnects}";
}
