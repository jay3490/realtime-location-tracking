using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Options;
using Tracking.Analytics.Data;
using Tracking.Core.Analytics;
using Tracking.Infrastructure.Observability;
using Tracking.Kafka;

namespace Tracking.Analytics;

/// <summary>Where consumed events are applied. The production sink is the EF Core aggregator.</summary>
public interface IAnalyticsEventSink
{
    Task<AggregationResult> ApplyAsync(IReadOnlyList<AnalyticsEvent> events, CancellationToken cancellationToken);
}

/// <summary>Applies each batch through a fresh scope, so every batch gets its own DbContext.</summary>
public sealed class ScopedAggregatorSink(IServiceScopeFactory scopes) : IAnalyticsEventSink
{
    public async Task<AggregationResult> ApplyAsync(IReadOnlyList<AnalyticsEvent> events, CancellationToken cancellationToken)
    {
        await using var scope = scopes.CreateAsyncScope();
        return await scope.ServiceProvider.GetRequiredService<AnalyticsAggregator>().ApplyAsync(events, cancellationToken).ConfigureAwait(false);
    }
}

public sealed class AnalyticsMetrics
{
    private readonly Counter<long> _events;
    private readonly Histogram<double> _applyLatency;

    public AnalyticsMetrics(IMeterFactory meterFactory)
    {
        ArgumentNullException.ThrowIfNull(meterFactory);
        var meter = meterFactory.Create(TrackingMeters.Analytics);
        _events = meter.CreateCounter<long>(
            "tracking.analytics.events",
            unit: "{event}",
            description: "Analytics events consumed from Kafka, by outcome (applied, duplicate, invalid).");
        _applyLatency = meter.CreateHistogram<double>(
            "tracking.analytics.apply.latency",
            unit: "s",
            description: "Time to apply one batch of events to PostgreSQL, including its transaction.");
    }

    public void Recorded(AggregationResult result, int invalid, TimeSpan elapsed)
    {
        ArgumentNullException.ThrowIfNull(result);
        _events.Add(result.Applied, new KeyValuePair<string, object?>("outcome", "applied"));
        _events.Add(result.Duplicates, new KeyValuePair<string, object?>("outcome", "duplicate"));
        _events.Add(invalid, new KeyValuePair<string, object?>("outcome", "invalid"));
        _applyLatency.Record(elapsed.TotalSeconds);
    }
}

/// <summary>
/// Consumes the analytics topics and folds each batch into PostgreSQL, committing Kafka offsets only
/// after the batch's transaction has committed.
/// </summary>
/// <remarks>
/// If applying a batch fails (the database is down, say), the same batch is retried with back-off and
/// nothing further is consumed, so the committed offset never passes an event that was not stored. A
/// crash between the database commit and the offset commit replays the batch, which the ledger turns
/// into a no-op.
/// </remarks>
public sealed partial class AnalyticsIngestWorker(
    IAnalyticsEventSource source,
    IAnalyticsEventSink sink,
    AnalyticsMetrics metrics,
    IOptions<AnalyticsConsumerOptions> options,
    IOptions<AnalyticsServiceOptions> serviceOptions,
    TimeProvider clock,
    ILogger<AnalyticsIngestWorker> logger) : BackgroundService
{
    private readonly AnalyticsConsumerOptions _options = options.Value;
    private readonly AnalyticsServiceOptions _service = serviceOptions.Value;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Consume blocks the calling thread, so the loop runs off the host's startup path.
        await Task.Yield();

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var batch = source.ConsumeBatch(_options.BatchSize, _options.PollTimeout, stoppingToken);
                if (batch.Count > 0)
                {
                    await HandleBatchAsync(batch, stoppingToken).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Confluent.Kafka.ConsumeException ex)
            {
                LogConsumeFailed(ex, ex.Error.Code);
                await Task.Delay(_service.ErrorDelay, clock, stoppingToken).ConfigureAwait(false);
            }
        }
    }

    /// <summary>Applies one batch, retrying until it is stored, then commits its offsets.</summary>
    public async Task HandleBatchAsync(IReadOnlyList<ConsumedEvent> batch, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(batch);

        var events = new List<AnalyticsEvent>(batch.Count);
        foreach (var consumed in batch)
        {
            if (consumed.Event is { } analyticsEvent)
            {
                events.Add(analyticsEvent);
            }
            else
            {
                // A malformed message can never be applied; skip it so it cannot block its partition.
                LogInvalid(consumed.Position.ToString(), consumed.Error ?? "unknown");
            }
        }

        var delay = _service.ErrorDelay;
        while (true)
        {
            var started = Stopwatch.GetTimestamp();
            try
            {
                var result = await sink.ApplyAsync(events, cancellationToken).ConfigureAwait(false);
                metrics.Recorded(result, batch.Count - events.Count, Stopwatch.GetElapsedTime(started));
                break;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                LogApplyFailed(ex, batch.Count, delay);
                await Task.Delay(delay, clock, cancellationToken).ConfigureAwait(false);
                delay = TimeSpan.FromTicks(Math.Min(delay.Ticks * 2, _service.MaxErrorDelay.Ticks));
            }
        }

        source.Commit(batch);
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "Could not consume from Kafka ({Code})")]
    private partial void LogConsumeFailed(Exception exception, Confluent.Kafka.ErrorCode code);

    [LoggerMessage(Level = LogLevel.Error, Message = "Skipping message at {Position}: {Error}")]
    private partial void LogInvalid(string position, string error);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Could not apply a batch of {Count} events; retrying in {Delay}")]
    private partial void LogApplyFailed(Exception exception, int count, TimeSpan delay);
}
