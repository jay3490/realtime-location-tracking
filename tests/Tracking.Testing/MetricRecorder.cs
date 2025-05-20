using System.Collections.Concurrent;
using System.Diagnostics.Metrics;

namespace Tracking.Testing;

/// <summary>An <see cref="IMeterFactory"/> whose meters are disposed with it, so tests do not leak listeners.</summary>
public sealed class TestMeterFactory : IMeterFactory
{
    private readonly ConcurrentBag<Meter> _meters = [];

    public Meter Create(MeterOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        var meter = new Meter(options.Name, options.Version, options.Tags, scope: this);
        _meters.Add(meter);
        return meter;
    }

    public void Dispose()
    {
        foreach (var meter in _meters)
        {
            meter.Dispose();
        }
    }
}

/// <summary>Collects every measurement published by meters created from one <see cref="IMeterFactory"/>.</summary>
public sealed class MetricRecorder : IDisposable
{
    private readonly MeterListener _listener = new();
    private readonly ConcurrentQueue<(string Instrument, double Value, IReadOnlyDictionary<string, object?> Tags)> _measurements = new();

    public MetricRecorder(IMeterFactory scope)
    {
        _listener.InstrumentPublished = (instrument, listener) =>
        {
            if (ReferenceEquals(instrument.Meter.Scope, scope))
            {
                listener.EnableMeasurementEvents(instrument);
            }
        };
        _listener.SetMeasurementEventCallback<long>((i, v, t, _) => Record(i, v, t));
        _listener.SetMeasurementEventCallback<int>((i, v, t, _) => Record(i, v, t));
        _listener.SetMeasurementEventCallback<double>((i, v, t, _) => Record(i, v, t));
        _listener.Start();
    }

    /// <summary>Polls observable instruments so their current values are recorded.</summary>
    public void Observe() => _listener.RecordObservableInstruments();

    public IReadOnlyList<double> Values(string instrument) =>
        _measurements.Where(m => m.Instrument == instrument).Select(m => m.Value).ToArray();

    public double Sum(string instrument, string? tag = null, object? tagValue = null) =>
        _measurements
            .Where(m => m.Instrument == instrument && (tag is null || Equals(m.Tags.GetValueOrDefault(tag), tagValue)))
            .Sum(m => m.Value);

    public double? Last(string instrument) =>
        _measurements.LastOrDefault(m => m.Instrument == instrument) is { Instrument: not null } last ? last.Value : null;

    public void Dispose() => _listener.Dispose();

    private void Record(Instrument instrument, double value, ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        var copy = new Dictionary<string, object?>(StringComparer.Ordinal);
        foreach (var tag in tags)
        {
            copy[tag.Key] = tag.Value;
        }

        _measurements.Enqueue((instrument.Name, value, copy));
    }
}
