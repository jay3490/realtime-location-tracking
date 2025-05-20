using System.Diagnostics.Metrics;
using Tracking.Core.Abstractions;
using Tracking.Infrastructure.Observability;

namespace Tracking.Processor.Processing;

/// <summary>The processor's instruments, on the <see cref="TrackingMeters.Processor"/> meter.</summary>
public sealed class ProcessorMetrics
{
    private readonly Counter<long> _entries;
    private readonly Histogram<double> _latency;
    private ConsumerGroupStatus _status = ConsumerGroupStatus.Empty;

    public ProcessorMetrics(IMeterFactory meterFactory)
    {
        ArgumentNullException.ThrowIfNull(meterFactory);
        var meter = meterFactory.Create(TrackingMeters.Processor);

        _entries = meter.CreateCounter<long>(
            "tracking.processor.entries",
            unit: "{entry}",
            description: "Ping stream entries handled, by outcome (applied, stale, dead_lettered).");
        _latency = meter.CreateHistogram<double>(
            "tracking.processor.latency",
            unit: "s",
            description: "Time from a ping being appended to the stream to its state change being committed.");
        meter.CreateObservableGauge(
            "tracking.stream.lag",
            () => Volatile.Read(ref _status).Lag ?? 0,
            unit: "{entry}",
            description: "Ping stream entries not yet delivered to any processor.");
        meter.CreateObservableGauge(
            "tracking.stream.pending",
            () => Volatile.Read(ref _status).Pending,
            unit: "{entry}",
            description: "Ping stream entries delivered to a processor but not yet acknowledged.");
        meter.CreateObservableGauge(
            "tracking.stream.length",
            () => Volatile.Read(ref _status).Length,
            unit: "{entry}",
            description: "Entries currently held in the ping stream.");
    }

    public ConsumerGroupStatus LastStatus => Volatile.Read(ref _status);

    public void Applied(TimeSpan sinceAppend)
    {
        _entries.Add(1, new KeyValuePair<string, object?>("outcome", "applied"));
        _latency.Record(Math.Max(0, sinceAppend.TotalSeconds));
    }

    public void Stale() => _entries.Add(1, new KeyValuePair<string, object?>("outcome", "stale"));

    public void DeadLettered() => _entries.Add(1, new KeyValuePair<string, object?>("outcome", "dead_lettered"));

    public void ObserveStatus(ConsumerGroupStatus status) => Volatile.Write(ref _status, status ?? ConsumerGroupStatus.Empty);
}
