using Microsoft.Extensions.Options;
using Tracking.Core.Abstractions;
using Tracking.Core.Analytics;
using Tracking.Core.Devices;
using Tracking.Core.Geofencing;

namespace Tracking.Processor.Processing;

public enum PingOutcome
{
    /// <summary>The ping advanced the device state.</summary>
    Applied,

    /// <summary>The ping was not newer than the stored state (a redelivery or out-of-order arrival).</summary>
    Stale,
}

public sealed class CommitConflictException(string deviceId, int attempts)
    : Exception($"Device '{deviceId}' state kept changing; gave up after {attempts} commit attempts.");

/// <summary>
/// Applies one ping: read state, compute the transition, commit it (with its analytics events)
/// conditionally, retry on conflict.
/// </summary>
public sealed class PingProcessor(IDeviceStateStore states, GeofenceCache geofences, IOptions<ProcessorOptions> options)
{
    private readonly int _maxAttempts = options.Value.MaxCommitAttempts;
    private readonly bool _publishAnalytics = options.Value.PublishAnalytics;

    public async Task<(PingOutcome Outcome, IReadOnlyList<GeofenceAlert> Alerts)> ProcessAsync(
        LocationPing ping,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(ping);

        var fences = await geofences.GetAsync(cancellationToken).ConfigureAwait(false);
        for (var attempt = 1; attempt <= _maxAttempts; attempt++)
        {
            var current = await states.GetAsync(ping.DeviceId, cancellationToken).ConfigureAwait(false);
            var outcome = DeviceTracker.Apply(current, ping, fences);
            if (outcome.IsStale)
            {
                return (PingOutcome.Stale, []);
            }

            var analytics = _publishAnalytics
                ? AnalyticsEvents.For(current, outcome.State, ping, outcome.Alerts)
                : [];
            if (await states.TryCommitAsync(current, outcome.State, outcome.Alerts, analytics, cancellationToken).ConfigureAwait(false))
            {
                return (PingOutcome.Applied, outcome.Alerts);
            }
        }

        throw new CommitConflictException(ping.DeviceId, _maxAttempts);
    }
}
