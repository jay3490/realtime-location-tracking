using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using System.Text.Json;
using Tracking.Core;
using Tracking.Core.Analytics;
using Tracking.Core.Devices;
using Tracking.Core.Geo;
using Tracking.Core.Geofencing;
using Tracking.Infrastructure.Redis;

namespace Tracking.Infrastructure.Tests;

[Collection(RedisTestGroup.Name)]
public sealed class RedisStoreTests(RedisFixture redis) : IAsyncLifetime
{
    private static readonly GeoPoint Dam = new(52.3731, 4.8926);
    private static readonly DateTimeOffset T0 = DateTimeOffset.FromUnixTimeMilliseconds(1_767_000_000_000);

    private readonly IOptions<RedisOptions> _options = RedisFixture.UniqueOptions();

    private RedisKeys Keys => new(_options);

    public Task InitializeAsync() => Task.CompletedTask;

    public Task DisposeAsync() =>
        string.IsNullOrEmpty(Environment.GetEnvironmentVariable(RedisFactAttribute.EnvironmentVariable))
            ? Task.CompletedTask
            : redis.DeleteKeysAsync(_options.Value.KeyPrefix);

    [RedisFact]
    public async Task Device_state_round_trips()
    {
        var store = new RedisDeviceStateStore(redis.Connection, Keys, _options);
        var state = new DeviceState("van-1", Dam, T0, 12.25, 271.5, ["a", "b"]);

        Assert.True(await store.TryCommitAsync(null, state, [], []));

        var loaded = await store.GetAsync("van-1");
        Assert.NotNull(loaded);
        Assert.Equal(state.Position, loaded.Position);
        Assert.Equal(state.RecordedAt, loaded.RecordedAt);
        Assert.Equal(12.25, loaded.SpeedMetersPerSecond);
        Assert.Equal(271.5, loaded.HeadingDegrees);
        Assert.Equal(["a", "b"], loaded.InsideGeofenceIds);
        Assert.Null(await store.GetAsync("unknown"));
    }

    [RedisFact]
    public async Task Missing_speed_and_heading_stay_null()
    {
        var store = new RedisDeviceStateStore(redis.Connection, Keys, _options);

        await store.TryCommitAsync(null, new DeviceState("d", Dam, T0, null, null, []), [], []);
        var loaded = await store.GetAsync("d");

        Assert.Null(loaded!.SpeedMetersPerSecond);
        Assert.Null(loaded.HeadingDegrees);
        Assert.Empty(loaded.InsideGeofenceIds);
    }

    [RedisFact]
    public async Task Commit_fails_when_the_stored_state_moved_on()
    {
        var store = new RedisDeviceStateStore(redis.Connection, Keys, _options);
        var v1 = new DeviceState("d", Dam, T0, null, null, []);
        var v2 = v1 with { RecordedAt = T0.AddSeconds(1) };
        var v3 = v1 with { RecordedAt = T0.AddSeconds(2) };

        Assert.True(await store.TryCommitAsync(null, v1, [], []));
        Assert.False(await store.TryCommitAsync(null, v2, [], []));
        Assert.True(await store.TryCommitAsync(v1, v2, [], []));
        Assert.False(await store.TryCommitAsync(v1, v3, [], []));

        Assert.Equal(T0.AddSeconds(1), (await store.GetAsync("d"))!.RecordedAt);
    }

    [RedisFact]
    public async Task Committed_alerts_are_logged_and_published()
    {
        var store = new RedisDeviceStateStore(redis.Connection, Keys, _options);
        var feed = new RedisAlertFeed(redis.Connection, Keys, NullLogger<RedisAlertFeed>.Instance);
        var log = new RedisAlertLog(redis.Connection, Keys);
        var received = new TaskCompletionSource<GeofenceAlert>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var subscription = await feed.SubscribeAsync(a =>
        {
            received.TrySetResult(a);
            return Task.CompletedTask;
        });

        var enter = new GeofenceAlert("d", "dam", "Dam Square", GeofenceTransition.Enter, Dam, T0);
        var exit = enter with { Transition = GeofenceTransition.Exit, OccurredAt = T0.AddMinutes(1) };
        await store.TryCommitAsync(null, new DeviceState("d", Dam, T0, null, null, ["dam"]), [enter], []);
        await store.TryCommitAsync(
            new DeviceState("d", Dam, T0, null, null, ["dam"]),
            new DeviceState("d", Dam, T0.AddMinutes(1), null, null, []),
            [exit], []);

        Assert.Equal(enter, await received.Task.WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.Equal([exit, enter], await log.GetRecentAsync(10));
    }

    [RedisFact]
    public async Task A_failed_commit_writes_no_alerts()
    {
        var store = new RedisDeviceStateStore(redis.Connection, Keys, _options);
        var log = new RedisAlertLog(redis.Connection, Keys);
        var state = new DeviceState("d", Dam, T0, null, null, []);
        await store.TryCommitAsync(null, state, [], []);

        var alert = new GeofenceAlert("d", "x", "X", GeofenceTransition.Enter, Dam, T0);
        Assert.False(await store.TryCommitAsync(null, state with { RecordedAt = T0.AddSeconds(5) }, [alert], []));

        Assert.Empty(await log.GetRecentAsync(10));
    }

    [RedisFact]
    public async Task Nearby_search_returns_devices_nearest_first_within_the_radius()
    {
        var store = new RedisDeviceStateStore(redis.Connection, Keys, _options);
        await store.TryCommitAsync(null, new DeviceState("far", GeoMath.Destination(Dam, 90, 900), T0, null, null, []), [], []);
        await store.TryCommitAsync(null, new DeviceState("near", GeoMath.Destination(Dam, 0, 100), T0, null, null, []), [], []);
        await store.TryCommitAsync(null, new DeviceState("outside", GeoMath.Destination(Dam, 0, 5_000), T0, null, null, []), [], []);

        var nearby = await store.FindNearbyAsync(Dam, 1_000, 10);

        Assert.Equal(["near", "far"], nearby.Select(n => n.DeviceId));
        Assert.InRange(nearby[0].DistanceMeters, 99, 101);
        Assert.Equal(T0, nearby[0].RecordedAt);
        Assert.Equal("near", Assert.Single(await store.FindNearbyAsync(Dam, 1_000, 1)).DeviceId);
    }

    [RedisFact]
    public async Task Geofences_support_crud_and_bump_the_version()
    {
        var store = new RedisGeofenceStore(redis.Connection, Keys);
        var circle = new CircleGeofence("dam", "Dam Square", Dam, 150);
        var polygon = new PolygonGeofence("block", "Block", [new(0, 0), new(0, 1), new(1, 1)]);

        Assert.Equal(0, await store.GetVersionAsync());
        Assert.True(await store.TryCreateAsync(circle));
        Assert.False(await store.TryCreateAsync(circle with { Name = "Duplicate" }));
        Assert.True(await store.UpsertAsync(polygon));
        Assert.False(await store.UpsertAsync(circle with { RadiusMeters = 300 }));

        var all = await store.GetAllAsync();
        Assert.Equal(["block", "dam"], all.Select(g => g.Id));
        Assert.Equal(300, Assert.IsType<CircleGeofence>(await store.GetAsync("dam")).RadiusMeters);
        Assert.Equal(polygon.Vertices, Assert.IsType<PolygonGeofence>(all[0]).Vertices);

        var versionBeforeDelete = await store.GetVersionAsync();
        Assert.True(await store.DeleteAsync("dam"));
        Assert.False(await store.DeleteAsync("dam"));
        Assert.Null(await store.GetAsync("dam"));
        Assert.Equal(versionBeforeDelete + 1, await store.GetVersionAsync());
    }

    [RedisFact]
    public async Task Analytics_events_are_queued_in_the_outbox_only_when_the_commit_lands()
    {
        var store = new RedisDeviceStateStore(redis.Connection, Keys, _options);
        var state = new DeviceState("d", Dam, T0, null, null, ["dam"]);
        var enter = new GeofenceAlert("d", "dam", "Dam", GeofenceTransition.Enter, Dam, T0);
        var events = AnalyticsEvents.For(null, state, new LocationPing("d", Dam, T0), [enter]);

        Assert.True(await store.TryCommitAsync(null, state, [enter], events));
        Assert.False(await store.TryCommitAsync(null, state with { RecordedAt = T0.AddSeconds(1) }, [], events));

        var outbox = await redis.Connection.GetDatabase().StreamRangeAsync(Keys.AnalyticsOutbox);
        Assert.Equal(2, outbox.Length);
        var fields = outbox[0].Values.ToDictionary(v => v.Name.ToString(), v => v.Value.ToString(), StringComparer.Ordinal);
        Assert.True(AnalyticsOutboxCodec.TryDecode(fields, out var kind, out var device, out var id, out var payload));
        Assert.Equal(AnalyticsEventKind.Position, kind);
        Assert.Equal("d", device);
        Assert.Equal(events[0].EventId, id);
        var position = JsonSerializer.Deserialize<PositionEvent>(payload, TrackingJson.Options)!;
        Assert.Equal(T0, position.RecordedAt);
        Assert.Equal(["dam"], position.InsideGeofenceIds);
        Assert.Equal("Geofence", outbox[1]["kind"].ToString());
    }

    [RedisFact]
    public async Task The_outbox_group_hands_each_event_to_one_relay_until_acknowledged()
    {
        var store = new RedisDeviceStateStore(redis.Connection, Keys, _options);
        var outbox = new RedisAnalyticsOutbox(redis.Connection, Keys, _options);
        var state = new DeviceState("d", Dam, T0, null, null, []);
        await store.TryCommitAsync(null, state, [], AnalyticsEvents.For(null, state, new LocationPing("d", Dam, T0), []));
        await outbox.EnsureConsumerGroupAsync();
        await outbox.EnsureConsumerGroupAsync();

        var entry = Assert.Single(await outbox.ReadNewAsync("r1", 10));
        Assert.Empty(await outbox.ReadNewAsync("r2", 10));
        await Task.Delay(30);
        var claimed = await outbox.ClaimAbandonedAsync("r2", TimeSpan.FromMilliseconds(10), "0-0", 10);
        await outbox.AcknowledgeAsync([entry.Id]);

        Assert.Equal(entry.Id, Assert.Single(claimed.Entries).Id);
        Assert.Empty(await outbox.ReadOwnPendingAsync("r2", "0", 10));
    }
}
