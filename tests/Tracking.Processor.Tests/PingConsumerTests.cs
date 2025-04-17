using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using StackExchange.Redis;
using Tracking.Core.Devices;
using Tracking.Core.Geo;
using Tracking.Core.Geofencing;
using Tracking.Processor.Processing;

namespace Tracking.Processor.Tests;

public sealed class PingConsumerTests
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
    };

    public PingConsumerTests()
    {
        _group = new InMemoryConsumerGroup(_clock);
        _geofences.UpsertAsync(new CircleGeofence("depot", "Depot", Depot, 200)).GetAwaiter().GetResult();
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
        });
        var processor = new PingProcessor(_states, new GeofenceCache(_geofences, _clock, options), options);
        return new PingConsumer(_group, processor, new ConsumerHeartbeat(_clock), _clock, options, NullLogger<PingConsumer>.Instance);
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

        var (entry, reason) = Assert.Single(_group.DeadLetters);
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

        var (entry, reason) = Assert.Single(_group.DeadLetters);
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
}
