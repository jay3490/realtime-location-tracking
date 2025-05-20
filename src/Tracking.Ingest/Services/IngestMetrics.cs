using System.Diagnostics.Metrics;
using Tracking.Infrastructure.Observability;

namespace Tracking.Ingest.Services;

/// <summary>The ingest service's instruments, on the <see cref="TrackingMeters.Ingest"/> meter.</summary>
public sealed class IngestMetrics
{
    private readonly Counter<long> _pings;
    private readonly UpDownCounter<long> _activeStreams;

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
    }

    public void Accepted(int count = 1) => _pings.Add(count, new KeyValuePair<string, object?>("outcome", "accepted"));

    public void Rejected() => _pings.Add(1, new KeyValuePair<string, object?>("outcome", "rejected"));

    public void Unavailable(int count = 1) => _pings.Add(count, new KeyValuePair<string, object?>("outcome", "unavailable"));

    public void StreamOpened() => _activeStreams.Add(1);

    public void StreamClosed() => _activeStreams.Add(-1);
}
