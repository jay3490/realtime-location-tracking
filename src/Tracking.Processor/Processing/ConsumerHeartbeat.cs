using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;

namespace Tracking.Processor.Processing;

/// <summary>
/// Records when the consumer loop last completed an iteration. Errors still count as an iteration,
/// so a Redis outage shows up in readiness (the Redis check) rather than liveness; only a loop that
/// is stuck or has died fails liveness.
/// </summary>
public sealed class ConsumerHeartbeat(TimeProvider clock)
{
    private long _lastBeatTicks = clock.GetUtcNow().UtcTicks;

    public DateTimeOffset LastBeat => new(Interlocked.Read(ref _lastBeatTicks), TimeSpan.Zero);

    public void Beat() => Interlocked.Exchange(ref _lastBeatTicks, clock.GetUtcNow().UtcTicks);
}

public sealed class ConsumerHeartbeatHealthCheck(ConsumerHeartbeat heartbeat, TimeProvider clock, IOptions<ProcessorOptions> options)
    : IHealthCheck
{
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        var silence = clock.GetUtcNow() - heartbeat.LastBeat;
        return Task.FromResult(silence <= options.Value.StallTimeout
            ? HealthCheckResult.Healthy($"last iteration {silence.TotalSeconds:F1}s ago")
            : HealthCheckResult.Unhealthy($"consumer loop has not completed an iteration for {silence.TotalSeconds:F0}s"));
    }
}
