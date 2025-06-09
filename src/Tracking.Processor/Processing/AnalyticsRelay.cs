using Microsoft.Extensions.Options;
using Tracking.Core.Abstractions;
using Tracking.Infrastructure.Redis;

namespace Tracking.Processor.Processing;

/// <summary>
/// Forwards the analytics outbox to Kafka. Every processor instance runs one relay, as a consumer of
/// the outbox's own group, so relaying scales with the processors.
/// </summary>
/// <remarks>
/// <para>
/// An outbox entry is acknowledged only after Kafka has acknowledged the event, so events are
/// delivered at least once; consumers deduplicate on the event id. Entries Kafka rejects stay pending
/// and are retried from memory with back-off, so a Kafka outage stalls only this path: pings keep
/// being processed and the outbox absorbs the backlog.
/// </para>
/// <para>
/// One relay publishes a batch in outbox order, so each device's events reach its partition in commit
/// order. Two relays can interleave batches for the same device, and a retried event is published after
/// the ones behind it that succeeded, so strict order across the whole topic is not guaranteed; the
/// events are built to be aggregated without relying on it.
/// </para>
/// </remarks>
public sealed partial class AnalyticsRelay(
    IAnalyticsOutbox outbox,
    IAnalyticsPublisher publisher,
    TimeProvider clock,
    IOptions<ProcessorOptions> options,
    ILogger<AnalyticsRelay> logger) : BackgroundService
{
    private const string ScanStart = "0-0";

    private readonly ProcessorOptions _options = options.Value;
    private readonly ErrorBackoff _backoff = new(options.Value.ErrorDelay, options.Value.MaxErrorDelay, new Random());
    private bool _recovered;
    private IReadOnlyList<LogEntry> _retry = [];
    private DateTimeOffset _nextClaimAt = DateTimeOffset.MinValue;

    private string ConsumerName => _options.ConsumerName;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var idleDelay = _options.MinIdleDelay;
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var relayed = await RelayOnceAsync(stoppingToken).ConfigureAwait(false);
                _backoff.Reset();
                if (relayed > 0)
                {
                    idleDelay = _options.MinIdleDelay;
                    continue;
                }

                await Task.Delay(idleDelay, clock, stoppingToken).ConfigureAwait(false);
                idleDelay = TimeSpan.FromTicks(Math.Min(idleDelay.Ticks * 2, _options.MaxIdleDelay.Ticks));
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                var delay = _backoff.NextDelay();
                LogRelayFailed(ex, _backoff.ConsecutiveFailures, delay);
                await Task.Delay(delay, clock, stoppingToken).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// One iteration: recover this relay's own pending entries (once), retry what failed last time,
    /// claim entries abandoned by other relays when due, then relay a batch of new entries. Throws
    /// when anything could not be published, after acknowledging what was.
    /// </summary>
    public async Task<int> RelayOnceAsync(CancellationToken cancellationToken = default)
    {
        var relayed = 0;
        if (!_recovered)
        {
            await outbox.EnsureConsumerGroupAsync(cancellationToken).ConfigureAwait(false);
            var after = ScanStart;
            while (await outbox.ReadOwnPendingAsync(ConsumerName, after, _options.BatchSize, cancellationToken).ConfigureAwait(false) is { Count: > 0 } pending)
            {
                relayed += await PublishAsync(pending, cancellationToken).ConfigureAwait(false);
                after = pending[^1].Id;
            }

            _recovered = true;
        }

        if (_retry.Count > 0)
        {
            var retry = _retry;
            _retry = [];
            relayed += await PublishAsync(retry, cancellationToken).ConfigureAwait(false);
        }

        if (clock.GetUtcNow() >= _nextClaimAt)
        {
            var start = ScanStart;
            do
            {
                var claim = await outbox.ClaimAbandonedAsync(ConsumerName, _options.ClaimMinIdle, start, _options.BatchSize, cancellationToken)
                    .ConfigureAwait(false);
                relayed += await PublishAsync(claim.Entries, cancellationToken).ConfigureAwait(false);
                start = claim.NextStartId;
            }
            while (start != ScanStart);

            _nextClaimAt = clock.GetUtcNow() + _options.ClaimInterval;
        }

        var entries = await outbox.ReadNewAsync(ConsumerName, _options.BatchSize, cancellationToken).ConfigureAwait(false);
        relayed += await PublishAsync(entries, cancellationToken).ConfigureAwait(false);
        return relayed;
    }

    private async Task<int> PublishAsync(IReadOnlyList<LogEntry> entries, CancellationToken cancellationToken)
    {
        if (entries.Count == 0)
        {
            return 0;
        }

        var done = new List<string>(entries.Count);
        var outbound = new List<(LogEntry Entry, OutboundEvent Event)>(entries.Count);
        foreach (var entry in entries)
        {
            if (AnalyticsOutboxCodec.TryDecode(entry.Fields, out var kind, out var device, out var eventId, out var payload))
            {
                outbound.Add((entry, new OutboundEvent(entry.Id, kind, device, eventId, payload)));
            }
            else
            {
                // Nothing a retry could fix; skip it rather than block the outbox behind it.
                LogUndecodable(entry.Id);
                done.Add(entry.Id);
            }
        }

        var failed = new List<LogEntry>();
        if (outbound.Count > 0)
        {
            var results = await publisher.PublishAsync(outbound.ConvertAll(o => o.Event), cancellationToken).ConfigureAwait(false);
            for (var i = 0; i < outbound.Count; i++)
            {
                if (results[i])
                {
                    done.Add(outbound[i].Entry.Id);
                }
                else
                {
                    failed.Add(outbound[i].Entry);
                }
            }
        }

        await outbox.AcknowledgeAsync(done, CancellationToken.None).ConfigureAwait(false);
        if (failed.Count > 0)
        {
            _retry = [.. _retry, .. failed];
            throw new AnalyticsRelayException(failed.Count, entries.Count);
        }

        return entries.Count;
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "Analytics relay iteration failed ({Failures} in a row); retrying in {Delay}")]
    private partial void LogRelayFailed(Exception exception, int failures, TimeSpan delay);

    [LoggerMessage(Level = LogLevel.Error, Message = "Skipping analytics outbox entry {EntryId}: it is not a valid event")]
    private partial void LogUndecodable(string entryId);
}

public sealed class AnalyticsRelayException(int failed, int total)
    : Exception($"{failed} of {total} analytics events were not acknowledged by Kafka; they will be retried.");
