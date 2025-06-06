using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using StackExchange.Redis;
using Tracking.Core.Devices;
using Tracking.Core.Geo;
using Tracking.Core.Geofencing;
using Tracking.Processor.Processing;
using Tracking.Testing;

namespace Tracking.Processor.Tests;

public sealed class PingConsumerTests : IDisposable
{
    private static readonly GeoPoint Depot = new(48.8584, 2.2945);
    private static readonly DateTimeOffset T0 = new(2026, 7, 14, 10, 0, 0, TimeSpan.Zero);

    private readonly ManualClock _clock = new(T0);
    private readonly InMemoryConsumerGroup _group;
    private readonly InMemoryDeviceStateStore _states = new();
    private readonly InMemoryGeofenceStore _geofences = new();
    private readonly ProcessorOptions _options = new()
    {
        BatchSize = 10,
        ClaimMinIdle = TimeSpan.FromSeconds(30),
        ClaimInterval = TimeSpan.FromSeconds(10),
        GeofenceRefreshInterval = TimeSpan.Zero,
        MaxCommitAttempts = 3,
        MaxDeliveryAttempts = 3,
        MaxDeliveryCount = 4,
    };

    private readonly TestMeterFactory _meters = new();
    private readonly MetricRecorder _recorder;

    public PingConsumerTests()
    {
        _recorder = new MetricRecorder(_meters);
        _group = new InMemoryConsumerGroup(_clock);
        _geofences.UpsertAsync(new CircleGeofence("depot", "Depot", Depot, 200)).GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        _recorder.Dispose();
        _meters.Dispose();
    }

    private PingConsumer Consumer(string name)
    {
        var options = Options.Create(new ProcessorOptions
        {
            ConsumerName = name,
            BatchSize = _options.BatchSize,
            ClaimMinIdle = _options.ClaimMinIdle,
            ClaimInterval = _options.ClaimInterval,
            GeofenceRefreshInterval = _options.GeofenceRefreshInterval,
            MaxCommitAttempts = _options.MaxCommitAttempts,
            MaxDeliveryAttempts = _options.MaxDeliveryAttempts,
            MaxDeliveryCount = _options.MaxDeliveryCount,
        });
        var processor = new PingProcessor(_states, new GeofenceCache(_geofences, _clock, options), options);
        return new PingConsumer(_group, processor, new ConsumerHeartbeat(_clock), new ProcessorMetrics(_meters), _clock, options, NullLogger<PingConsumer>.Instance);
    }

    private static LocationPing Ping(string device, double metersFromDepot, int second) =>
        new(device, GeoMath.Destination(Depot, 90, metersFromDepot), T0.AddSeconds(second));

    [Fact]
    public async Task New_pings_update_state_raise_alerts_and_are_acknowledged()
    {
        var consumer = Consumer("c1");
        var ids = new[]
        {
            _group.Append(Ping("van", 1_000, 0)),
            _group.Append(Ping("van", 100, 60)),
            _group.Append(Ping("van", 1_000, 120)),
        };

        var handled = await consumer.PollAsync();

        Assert.True(_group.GroupCreated);
        Assert.Equal(3, handled);
        Assert.Equal(ids, _group.Acknowledged.Order(StringComparer.Ordinal));
        Assert.Empty(_group.PendingIds);
        Assert.Equal(T0.AddSeconds(120), _states["van"]!.RecordedAt);
        Assert.Equal(15, _states["van"]!.SpeedMetersPerSecond!.Value, precision: 3);
        Assert.Equal(
            [GeofenceTransition.Enter, GeofenceTransition.Exit],
            _states.CommittedAlerts.Select(a => a.Transition));
    }

    [Fact]
    public async Task Duplicate_and_out_of_order_pings_are_acknowledged_without_effects()
    {
        var consumer = Consumer("c1");
        _group.Append(Ping("van", 100, 10));
        _group.Append(Ping("van", 100, 10));
        _group.Append(Ping("van", 5_000, 5));

        await consumer.PollAsync();

        Assert.Equal(1, _states.Commits);
        Assert.Single(_states.CommittedAlerts);
        Assert.Equal(3, _group.Acknowledged.Count);
        Assert.Equal(T0.AddSeconds(10), _states["van"]!.RecordedAt);
    }

    [Fact]
    public async Task Undecodable_entries_are_dead_lettered()
    {
        var consumer = Consumer("c1");
        var bad = _group.AppendRaw(new Dictionary<string, string> { ["device"] = "van", ["lat"] = "north" });
        _group.Append(Ping("van", 100, 0));

        await consumer.PollAsync();

        var (entry, reason, _) = Assert.Single(_group.DeadLetters);
        Assert.Equal(bad, entry.Id);
        Assert.Contains("lat", reason, StringComparison.Ordinal);
        Assert.Empty(_group.PendingIds);
        Assert.NotNull(_states["van"]);
    }

    [Fact]
    public async Task Entries_abandoned_by_a_crashed_consumer_are_claimed_after_the_idle_timeout()
    {
        var crashed = Consumer("crashed");
        var survivor = Consumer("survivor");
        var id = _group.Append(Ping("van", 100, 0));
        await _group.ReadNewAsync("crashed", 10); // read, then crashed before processing

        await survivor.PollAsync();
        Assert.Equal([id], _group.PendingFor("crashed"));

        _clock.Advance(TimeSpan.FromSeconds(31));
        await survivor.PollAsync();

        Assert.Empty(_group.PendingIds);
        Assert.Contains(id, _group.Acknowledged);
        Assert.NotNull(_states["van"]);
        GC.KeepAlive(crashed);
    }

    [Fact]
    public async Task Claiming_pages_through_a_large_pending_list()
    {
        var survivor = Consumer("survivor");
        for (var i = 0; i < 25; i++)
        {
            _group.Append(Ping($"van-{i}", 100, i));
        }

        await _group.ReadNewAsync("crashed", 100);
        _clock.Advance(TimeSpan.FromMinutes(1));

        await survivor.PollAsync();

        Assert.Empty(_group.PendingIds);
        Assert.Equal(25, _states.Commits);
    }

    [Fact]
    public async Task A_restarted_consumer_finishes_its_own_pending_entries_first()
    {
        var id = _group.Append(Ping("van", 100, 0));
        await _group.ReadNewAsync("c1", 10); // previous run of c1 died here

        var restarted = Consumer("c1");
        var handled = await restarted.PollAsync();

        Assert.Equal(1, handled);
        Assert.Contains(id, _group.Acknowledged);
    }

    [Fact]
    public async Task Commit_conflicts_are_retried_against_fresh_state()
    {
        var consumer = Consumer("c1");
        _group.Append(Ping("van", 100, 60));
        var raced = false;
        _states.BeforeCommit = _ =>
        {
            if (raced)
            {
                return true;
            }

            // Another processor stores an older ping for the same device first.
            raced = true;
            _states.Put(new DeviceState("van", GeoMath.Destination(Depot, 90, 5_000), T0, null, null, []));
            return false;
        };

        await consumer.PollAsync();

        Assert.Equal(T0.AddSeconds(60), _states["van"]!.RecordedAt);
        Assert.NotNull(_states["van"]!.SpeedMetersPerSecond);
        Assert.Empty(_group.PendingIds);
    }

    [Fact]
    public async Task Redis_outages_leave_entries_pending_and_they_are_retried()
    {
        var consumer = Consumer("c1");
        var first = _group.Append(Ping("van", 100, 0));
        var second = _group.Append(Ping("van", 150, 1));
        _states.ReadFailure = new RedisConnectionException(
            ConnectionFailureType.SocketFailure, CommandFlags.None, "down", innerException: null, CommandStatus.Unknown);

        for (var attempt = 0; attempt < 5; attempt++)
        {
            await Assert.ThrowsAsync<RedisConnectionException>(() => consumer.PollAsync());
        }

        Assert.Equal([first, second], _group.PendingFor("c1"));
        Assert.Empty(_group.DeadLetters);
        Assert.Equal(1, _group.DeliveryCount(first)); // retried from memory, never re-read

        _states.ReadFailure = null;
        await consumer.PollAsync();

        Assert.Empty(_group.PendingIds);
        Assert.Equal(T0.AddSeconds(1), _states["van"]!.RecordedAt);
    }

    [Fact]
    public async Task Entries_that_keep_failing_are_dead_lettered_after_the_attempt_limit()
    {
        var consumer = Consumer("c1");
        var poison = _group.Append(Ping("van", 100, 0));
        _states.BeforeCommit = _ => false; // every commit conflicts

        await Assert.ThrowsAsync<CommitConflictException>(() => consumer.PollAsync());
        await Assert.ThrowsAsync<CommitConflictException>(() => consumer.PollAsync());
        await consumer.PollAsync();

        var (entry, reason, _) = Assert.Single(_group.DeadLetters);
        Assert.Equal(poison, entry.Id);
        Assert.Contains("failed 3 times", reason, StringComparison.Ordinal);
        Assert.Empty(_group.PendingIds);
    }

    [Fact]
    public async Task Run_loop_processes_until_cancelled()
    {
        var consumer = Consumer("c1");
        _group.Append(Ping("van", 100, 0));
        using var cts = new CancellationTokenSource();
        _states.BeforeCommit = _ =>
        {
            cts.Cancel();
            return true;
        };

        await consumer.RunAsync(cts.Token).WaitAsync(TimeSpan.FromSeconds(10));

        Assert.NotNull(_states["van"]);
    }

    [Fact]
    public async Task Outcomes_and_append_to_commit_latency_are_recorded()
    {
        var consumer = Consumer("c1");
        _group.Append(Ping("van", 100, 10));
        _group.Append(Ping("van", 100, 10));
        _group.AppendRaw(new Dictionary<string, string> { ["device"] = "van" });

        await consumer.PollAsync();

        Assert.Equal(1, _recorder.Sum("tracking.processor.entries", "outcome", "applied"));
        Assert.Equal(1, _recorder.Sum("tracking.processor.entries", "outcome", "stale"));
        Assert.Equal(1, _recorder.Sum("tracking.processor.entries", "outcome", "dead_lettered"));
        var latency = Assert.Single(_recorder.Values("tracking.processor.latency"));
        Assert.True(latency >= 0);
    }

    [Fact]
    public async Task Lag_monitor_publishes_the_group_backlog()
    {
        var metrics = new ProcessorMetrics(_meters);
        var monitor = new StreamLagMonitor(
            _group, metrics, _clock, Options.Create(_options), NullLogger<StreamLagMonitor>.Instance);
        _group.Append(Ping("van", 100, 0));
        _group.Append(Ping("van", 100, 1));
        _group.Append(Ping("van", 100, 2));
        await _group.ReadNewAsync("c1", 1);

        await monitor.SampleAsync(CancellationToken.None);
        _recorder.Observe();

        Assert.Equal(2, _recorder.Last("tracking.stream.lag"));
        Assert.Equal(1, _recorder.Last("tracking.stream.pending"));
        Assert.Equal(3, _recorder.Last("tracking.stream.length"));
    }

    [Fact]
    public async Task Entries_that_keep_crashing_their_consumer_are_dead_lettered_by_delivery_count()
    {
        var poison = _group.Append(Ping("van", 100, 0));
        var healthy = _group.Append(Ping("bus", 100, 0));

        // Four runs of c1 read the entry and died before acknowledging it: nothing was ever counted
        // in memory, but Redis counted every delivery.
        await _group.ReadNewAsync("c1", 1);
        for (var run = 0; run < 3; run++)
        {
            await _group.ReadOwnPendingAsync("c1", "0-0", 1);
        }

        var restarted = Consumer("c1");
        await restarted.PollAsync();

        var (entry, reason, consumer) = Assert.Single(_group.DeadLetters);
        Assert.Equal(poison, entry.Id);
        Assert.Equal(5, entry.DeliveryCount);
        Assert.Contains("delivered 5 times", reason, StringComparison.Ordinal);
        Assert.Equal("c1", consumer);
        Assert.Null(_states["van"]);
        Assert.Contains(healthy, _group.Acknowledged);
        Assert.Empty(_group.PendingIds);
    }

    [Fact]
    public async Task Delivery_counts_follow_an_entry_when_it_is_claimed()
    {
        var id = _group.Append(Ping("van", 100, 0));
        await _group.ReadNewAsync("a", 1);
        _clock.Advance(TimeSpan.FromMinutes(1));
        await _group.ClaimAbandonedAsync("b", TimeSpan.FromSeconds(30), "0-0", 10);
        _clock.Advance(TimeSpan.FromMinutes(1));
        await _group.ClaimAbandonedAsync("c", TimeSpan.FromSeconds(30), "0-0", 10);
        _clock.Advance(TimeSpan.FromMinutes(1));

        // The survivor's claim is the fourth delivery: still within the limit, so it is processed.
        var survivor = Consumer("survivor");
        await survivor.PollAsync();

        Assert.Empty(_group.DeadLetters);
        Assert.Contains(id, _group.Acknowledged);
        Assert.NotNull(_states["van"]);
    }
}
