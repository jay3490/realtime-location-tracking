using Microsoft.Extensions.Options;
using StackExchange.Redis;
using Tracking.Core.Abstractions;
using Tracking.Core.Devices;

namespace Tracking.Processor.Processing;

/// <summary>
/// One member of the processor consumer group.
/// </summary>
/// <remarks>
/// Delivery is at least once. An entry is acknowledged only after its effects are committed, so a
/// crash between commit and <c>XACK</c> leads to redelivery; the redelivered ping is then stale and is
/// acknowledged without side effects. Entries left pending by this consumer (after an error or a
/// restart) are re-read before new ones; entries left by a consumer that disappeared are claimed
/// with <c>XAUTOCLAIM</c> once they have been idle for <see cref="ProcessorOptions.ClaimMinIdle"/>.
/// </remarks>
public sealed partial class PingConsumer(
    ILocationConsumerGroup group,
    PingProcessor processor,
    ConsumerHeartbeat heartbeat,
    ProcessorMetrics metrics,
    TimeProvider clock,
    IOptions<ProcessorOptions> options,
    ILogger<PingConsumer> logger)
{
    private const string ScanStart = "0-0";

    private readonly ProcessorOptions _options = options.Value;
    private readonly Dictionary<string, int> _failedAttempts = new(StringComparer.Ordinal);
    private bool _ownPendingDirty = true;
    private DateTimeOffset _nextClaimAt = DateTimeOffset.MinValue;

    public string ConsumerName => _options.ConsumerName;

    public async Task RunAsync(CancellationToken stoppingToken)
    {
        LogStarting(ConsumerName);
        var idleDelay = _options.MinIdleDelay;

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var processed = await PollAsync(stoppingToken).ConfigureAwait(false);
                heartbeat.Beat();

                if (processed > 0)
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
                heartbeat.Beat();
                LogPollFailed(ex, _options.ErrorDelay);
                await Task.Delay(_options.ErrorDelay, clock, stoppingToken).ConfigureAwait(false);
            }
        }

        LogStopped(ConsumerName);
    }

    /// <summary>
    /// One iteration: make sure the group exists, finish this consumer's own pending entries, claim
    /// abandoned entries when due, then read a batch of new entries. Returns how many entries were
    /// handled (processed, skipped as stale, or dead-lettered).
    /// </summary>
    public async Task<int> PollAsync(CancellationToken cancellationToken = default)
    {
        var handled = 0;

        if (_ownPendingDirty)
        {
            await group.EnsureConsumerGroupAsync(cancellationToken).ConfigureAwait(false);
            handled += await DrainOwnPendingAsync(cancellationToken).ConfigureAwait(false);
            _ownPendingDirty = false;
        }

        if (clock.GetUtcNow() >= _nextClaimAt)
        {
            handled += await ClaimAbandonedAsync(cancellationToken).ConfigureAwait(false);
            _nextClaimAt = clock.GetUtcNow() + _options.ClaimInterval;
        }

        var entries = await group.ReadNewAsync(ConsumerName, _options.BatchSize, cancellationToken).ConfigureAwait(false);
        handled += await HandleAsync(entries, cancellationToken).ConfigureAwait(false);
        return handled;
    }

    private async Task<int> DrainOwnPendingAsync(CancellationToken cancellationToken)
    {
        var handled = 0;
        var after = ScanStart;
        while (true)
        {
            var entries = await group.ReadOwnPendingAsync(ConsumerName, after, _options.BatchSize, cancellationToken).ConfigureAwait(false);
            if (entries.Count == 0)
            {
                break;
            }

            if (handled == 0)
            {
                LogRecoveringPending(entries.Count);
            }

            handled += await HandleAsync(entries, cancellationToken).ConfigureAwait(false);
            after = entries[^1].Id;
        }

        return handled;
    }

    private async Task<int> ClaimAbandonedAsync(CancellationToken cancellationToken)
    {
        var handled = 0;
        var start = ScanStart;
        do
        {
            var claim = await group.ClaimAbandonedAsync(ConsumerName, _options.ClaimMinIdle, start, _options.BatchSize, cancellationToken)
                .ConfigureAwait(false);
            if (claim.Entries.Count > 0)
            {
                LogClaimed(claim.Entries.Count);
                handled += await HandleAsync(claim.Entries, cancellationToken).ConfigureAwait(false);
            }

            start = claim.NextStartId;
        }
        while (start != ScanStart);

        return handled;
    }

    /// <summary>
    /// Processes entries in order and acknowledges the ones that are done. Stops at the first failure,
    /// leaving it and everything after it pending on this consumer for the next iteration.
    /// </summary>
    private async Task<int> HandleAsync(IReadOnlyList<LogEntry> entries, CancellationToken cancellationToken)
    {
        var done = new List<string>(entries.Count);
        try
        {
            foreach (var entry in entries)
            {
                if (!LocationPingCodec.TryDecode(entry.Fields, out var ping, out var error))
                {
                    await DeadLetterAsync(entry, error, cancellationToken).ConfigureAwait(false);
                    continue;
                }

                try
                {
                    var (outcome, alerts) = await processor.ProcessAsync(ping, cancellationToken).ConfigureAwait(false);
                    _failedAttempts.Remove(entry.Id);
                    done.Add(entry.Id);
                    if (outcome == PingOutcome.Stale)
                    {
                        metrics.Stale();
                        LogStale(entry.Id, ping.DeviceId);
                    }
                    else
                    {
                        metrics.Applied(StreamEntryId.TryGetTimestamp(entry.Id, out var appendedAt)
                            ? clock.GetUtcNow() - appendedAt
                            : TimeSpan.Zero);
                    }

                    foreach (var alert in alerts)
                    {
                        LogAlert(alert.DeviceId, alert.Transition.ToString(), alert.GeofenceId);
                    }
                }
                catch (Exception ex) when (IsTransient(ex))
                {
                    // Redis is unreachable: not the entry's fault, so it does not count towards dead-lettering.
                    _ownPendingDirty = true;
                    throw;
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    var attempts = _failedAttempts.GetValueOrDefault(entry.Id) + 1;
                    if (attempts < _options.MaxDeliveryAttempts)
                    {
                        _failedAttempts[entry.Id] = attempts;
                        _ownPendingDirty = true;
                        throw;
                    }

                    _failedAttempts.Remove(entry.Id);
                    await DeadLetterAsync(entry, $"failed {attempts} times: {ex.Message}", cancellationToken).ConfigureAwait(false);
                }
            }
        }
        finally
        {
            if (done.Count > 0)
            {
                await group.AcknowledgeAsync(done, CancellationToken.None).ConfigureAwait(false);
            }
        }

        return entries.Count;
    }

    private static bool IsTransient(Exception exception) => exception is RedisException or TimeoutException;

    private async Task DeadLetterAsync(LogEntry entry, string reason, CancellationToken cancellationToken)
    {
        LogDeadLettered(entry.Id, reason);
        await group.DeadLetterAsync(entry, reason, cancellationToken).ConfigureAwait(false);
        metrics.DeadLettered();
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Consumer {Consumer} starting")]
    private partial void LogStarting(string consumer);

    [LoggerMessage(Level = LogLevel.Information, Message = "Consumer {Consumer} stopped")]
    private partial void LogStopped(string consumer);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Poll failed; retrying in {Delay}")]
    private partial void LogPollFailed(Exception exception, TimeSpan delay);

    [LoggerMessage(Level = LogLevel.Information, Message = "Re-reading {Count} entries left pending by a previous run")]
    private partial void LogRecoveringPending(int count);

    [LoggerMessage(Level = LogLevel.Information, Message = "Claimed {Count} entries abandoned by other consumers")]
    private partial void LogClaimed(int count);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Skipped stale entry {EntryId} for {DeviceId}")]
    private partial void LogStale(string entryId, string deviceId);

    [LoggerMessage(Level = LogLevel.Information, Message = "{DeviceId} {Transition} geofence {GeofenceId}")]
    private partial void LogAlert(string deviceId, string transition, string geofenceId);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Dead-lettering entry {EntryId}: {Reason}")]
    private partial void LogDeadLettered(string entryId, string reason);
}
