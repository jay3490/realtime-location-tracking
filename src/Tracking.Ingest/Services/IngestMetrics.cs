using System.Diagnostics.Metrics;
using Tracking.Infrastructure.Observability;

namespace Tracking.Ingest.Services;

/// <summary>The ingest service's instruments, on the <see cref="TrackingMeters.Ingest"/> meter.</summary>
public sealed class IngestMetrics
{
    private readonly Counter<long> _pings;
    private readonly UpDownCounter<long> _activeStreams;
    private readonly Histogram<int> _batchSize;
    private readonly Histogram<double> _appendLatency;

    public IngestMetrics(IMeterFactory meterFactory)
    {
        ArgumentNullException.ThrowIfNull(meterFactory);
        var meter = meterFactory.Create(TrackingMeters.Ingest);

        _pings = meter.CreateCounter<long>(
            "tracking.ingest.pings",
            unit: "{ping}",
            description: "Pings received on device streams, by outcome (accepted, rejected, unavailable).");
        _activeStreams = meter.CreateUpDownCounter<long>(
            "tracking.ingest.streams.active",
            unit: "{stream}",
            description: "Device streams currently open on this instance.");
        _batchSize = meter.CreateHistogram<int>(
            "tracking.ingest.append.batch_size",
            unit: "{ping}",
            description: "Pings per pipelined XADD batch.");
        _appendLatency = meter.CreateHistogram<double>(
            "tracking.ingest.append.latency",
            unit: "s",
            description: "Round-trip time of one pipelined XADD batch.");
    }

    public void Accepted(int count = 1) => _pings.Add(count, new KeyValuePair<string, object?>("outcome", "accepted"));

    public void Rejected() => _pings.Add(1, new KeyValuePair<string, object?>("outcome", "rejected"));

    public void Unavailable(int count = 1) => _pings.Add(count, new KeyValuePair<string, object?>("outcome", "unavailable"));

    public void BatchAppended(int size, TimeSpan elapsed)
    {
        _batchSize.Record(size);
        _appendLatency.Record(elapsed.TotalSeconds);
    }

    public void StreamOpened() => _activeStreams.Add(1);

    public void StreamClosed() => _activeStreams.Add(-1);
}
