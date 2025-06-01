using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Tracking.Ingest.Services;

/// <summary>
/// Coordinates a graceful shutdown of the open device streams.
/// </summary>
/// <remarks>
/// When the host is asked to stop (SIGTERM in a container), <see cref="Begin"/> is called. From then on
/// new streams are refused, readiness fails so the instance leaves the load balancer, and every open
/// stream stops reading new pings, appends and acknowledges the ones it already read, and ends with
/// <c>UNAVAILABLE</c> so the device reconnects to another instance. A ping is therefore either
/// acknowledged after it was stored or never acknowledged at all; nothing acknowledged is lost.
/// </remarks>
public sealed partial class StreamDrain(ILogger<StreamDrain> logger) : IDisposable
{
    private readonly CancellationTokenSource _draining = new();
    private readonly TaskCompletionSource _idle = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _active;

    /// <summary>Cancelled when draining begins.</summary>
    public CancellationToken Token => _draining.Token;

    public bool IsDraining => _draining.IsCancellationRequested;

    public int ActiveStreams => Volatile.Read(ref _active);

    public void Begin()
    {
        if (IsDraining)
        {
            return;
        }

        LogDraining(ActiveStreams);
        _draining.Cancel();
        if (ActiveStreams == 0)
        {
            _idle.TrySetResult();
        }
    }

    /// <summary>
    /// Registers an open stream. Returns null, registering nothing, once draining has begun.
    /// </summary>
    public IDisposable? TryEnter()
    {
        Interlocked.Increment(ref _active);
        var lease = new Lease(this);
        if (IsDraining)
        {
            lease.Dispose();
            return null;
        }

        return lease;
    }

    /// <summary>Completes once draining has begun and every stream has finished.</summary>
    public Task WhenIdleAsync(CancellationToken cancellationToken) => _idle.Task.WaitAsync(cancellationToken);

    public void Dispose() => _draining.Dispose();

    private void Exit()
    {
        if (Interlocked.Decrement(ref _active) == 0 && IsDraining)
        {
            _idle.TrySetResult();
        }
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Draining {Streams} open device streams before shutdown")]
    private partial void LogDraining(int streams);

    private sealed class Lease(StreamDrain owner) : IDisposable
    {
        private int _disposed;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
            {
                owner.Exit();
            }
        }
    }
}

/// <summary>Fails readiness while draining, so the orchestrator stops routing new streams here.</summary>
public sealed class StreamDrainHealthCheck(StreamDrain drain) : IHealthCheck
{
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default) =>
        Task.FromResult(drain.IsDraining
            ? HealthCheckResult.Unhealthy($"draining, {drain.ActiveStreams} streams still open")
            : HealthCheckResult.Healthy($"{drain.ActiveStreams} streams open"));
}
