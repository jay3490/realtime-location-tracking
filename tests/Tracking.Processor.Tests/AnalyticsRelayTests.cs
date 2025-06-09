using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Tracking.Core.Analytics;
using Tracking.Core.Geofencing;
using Tracking.Processor.Processing;
using Tracking.Testing;

namespace Tracking.Processor.Tests;

public sealed class AnalyticsRelayTests : IDisposable
{
    private static readonly DateTimeOffset T0 = new(2026, 9, 1, 6, 0, 0, TimeSpan.Zero);

    private readonly ManualClock _clock = new(T0);
    private readonly InMemoryConsumerGroup _outbox;
    private readonly RecordingAnalyticsPublisher _publisher = new();
    private readonly List<AnalyticsRelay> _relays = [];

    public AnalyticsRelayTests() => _outbox = new InMemoryConsumerGroup(_clock);

    public void Dispose()
    {
        foreach (var relay in _relays)
        {
            relay.Dispose();
        }
    }

    private AnalyticsRelay Relay(string name)
    {
        var relay = new AnalyticsRelay(
            _outbox,
            _publisher,
            _clock,
            Options.Create(new ProcessorOptions { ConsumerName = name, BatchSize = 10, ClaimMinIdle = TimeSpan.FromSeconds(30) }),
            NullLogger<AnalyticsRelay>.Instance);
        _relays.Add(relay);
        return relay;
    }

    private static PositionEvent Position(string device, int second) =>
        new(AnalyticsEvents.PositionEventId(device, T0.AddSeconds(second)), device, 1, 2, T0.AddSeconds(second), null, null, null, 0, null, []);

    [Fact]
    public async Task Events_are_published_in_outbox_order_keyed_by_device_and_then_acknowledged()
    {
        var ids = new[]
        {
            _outbox.AppendEvent(Position("van", 0)),
            _outbox.AppendEvent(new GeofenceEvent("van/dam/enter/1", "van", "dam", "Dam", GeofenceTransition.Enter, 1, 2, T0)),
            _outbox.AppendEvent(Position("bus", 1)),
        };

        var relayed = await Relay("r1").RelayOnceAsync();

        Assert.Equal(3, relayed);
        Assert.Equal(["van", "van", "bus"], _publisher.Published.Select(e => e.Key));
        Assert.Equal([AnalyticsEventKind.Position, AnalyticsEventKind.Geofence, AnalyticsEventKind.Position], _publisher.Published.Select(e => e.Kind));
        Assert.Equal("van/dam/enter/1", _publisher.Published[1].EventId);
        Assert.Contains("\"geofenceId\":\"dam\"", _publisher.Published[1].Payload, StringComparison.Ordinal);
        Assert.Equal(ids, _outbox.Acknowledged.Order(StringComparer.Ordinal));
        Assert.Empty(_outbox.PendingIds);
    }

    [Fact]
    public async Task Events_kafka_rejects_stay_pending_and_are_retried_without_rereading()
    {
        var relay = Relay("r1");
        var ok = _outbox.AppendEvent(Position("van", 0));
        var rejected = _outbox.AppendEvent(Position("bus", 0));
        _publisher.FailWhere = e => e.Key == "bus";

        await Assert.ThrowsAsync<AnalyticsRelayException>(() => relay.RelayOnceAsync());

        Assert.Contains(ok, _outbox.Acknowledged);
        Assert.Equal([rejected], _outbox.PendingFor("r1"));

        _publisher.FailWhere = null;
        await relay.RelayOnceAsync();

        Assert.Empty(_outbox.PendingIds);
        Assert.Equal(1, _outbox.DeliveryCount(rejected));
        Assert.Equal(["van", "bus"], _publisher.Published.Select(e => e.Key));
    }

    [Fact]
    public async Task Undecodable_entries_are_skipped_so_they_cannot_block_the_outbox()
    {
        var junk = _outbox.AppendRaw(new Dictionary<string, string> { ["kind"] = "Teleport", ["device"] = "van" });
        _outbox.AppendEvent(Position("van", 0));

        await Relay("r1").RelayOnceAsync();

        Assert.Contains(junk, _outbox.Acknowledged);
        Assert.Single(_publisher.Published);
        Assert.Empty(_outbox.PendingIds);
    }

    [Fact]
    public async Task A_restarted_relay_publishes_its_own_pending_entries_and_claims_abandoned_ones()
    {
        var mine = _outbox.AppendEvent(Position("van", 0));
        var abandoned = _outbox.AppendEvent(Position("bus", 0));
        await _outbox.ReadNewAsync("r1", 1);
        await _outbox.ReadNewAsync("crashed", 1);
        _clock.Advance(TimeSpan.FromMinutes(1));

        await Relay("r1").RelayOnceAsync();

        Assert.Contains(mine, _outbox.Acknowledged);
        Assert.Contains(abandoned, _outbox.Acknowledged);
        Assert.Equal(2, _publisher.Published.Count);
    }
}
