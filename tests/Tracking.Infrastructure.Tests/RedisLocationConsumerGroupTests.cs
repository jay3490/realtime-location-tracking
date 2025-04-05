using Microsoft.Extensions.Options;
using Tracking.Core.Devices;
using Tracking.Core.Geo;
using Tracking.Infrastructure.Redis;

namespace Tracking.Infrastructure.Tests;

[Collection(RedisTestGroup.Name)]
public sealed class RedisLocationConsumerGroupTests(RedisFixture redis) : IAsyncLifetime
{
    private readonly IOptions<RedisOptions> _options = RedisFixture.UniqueOptions();

    private RedisKeys Keys => new(_options);

    public Task InitializeAsync() => Task.CompletedTask;

    public Task DisposeAsync() =>
        string.IsNullOrEmpty(Environment.GetEnvironmentVariable(RedisFactAttribute.EnvironmentVariable))
            ? Task.CompletedTask
            : redis.DeleteKeysAsync(_options.Value.KeyPrefix);

    private static LocationPing Ping(string device, int second) =>
        new(device, new GeoPoint(10, 20 + (second / 1000.0)), DateTimeOffset.FromUnixTimeSeconds(1_700_000_000 + second));

    [RedisFact]
    public async Task Appended_pings_are_read_once_by_the_group()
    {
        var log = new RedisLocationLog(redis.Connection, Keys, _options);
        var group = new RedisLocationConsumerGroup(redis.Connection, Keys, _options);
        await group.EnsureConsumerGroupAsync();
        await group.EnsureConsumerGroupAsync(); // idempotent

        var id1 = await log.AppendAsync(Ping("a", 1));
        var id2 = await log.AppendAsync(Ping("b", 2));

        var first = await group.ReadNewAsync("c1", 10);
        var second = await group.ReadNewAsync("c2", 10);

        Assert.Equal([id1, id2], first.Select(e => e.Id));
        Assert.Empty(second);
        Assert.True(LocationPingCodec.TryDecode(first[0].Fields, out var decoded, out _));
        Assert.Equal(Ping("a", 1), decoded);
    }

    [RedisFact]
    public async Task Pings_appended_before_the_group_exists_are_still_delivered()
    {
        var log = new RedisLocationLog(redis.Connection, Keys, _options);
        var group = new RedisLocationConsumerGroup(redis.Connection, Keys, _options);

        var id = await log.AppendAsync(Ping("a", 1));
        await group.EnsureConsumerGroupAsync();

        Assert.Equal(id, Assert.Single(await group.ReadNewAsync("c1", 10)).Id);
    }

    [RedisFact]
    public async Task Unacknowledged_entries_stay_pending_for_their_consumer_until_acked()
    {
        var log = new RedisLocationLog(redis.Connection, Keys, _options);
        var group = new RedisLocationConsumerGroup(redis.Connection, Keys, _options);
        await group.EnsureConsumerGroupAsync();
        var id1 = await log.AppendAsync(Ping("a", 1));
        var id2 = await log.AppendAsync(Ping("a", 2));
        await group.ReadNewAsync("c1", 10);

        await group.AcknowledgeAsync([id1]);
        var pending = await group.ReadOwnPendingAsync("c1", "0", 10);

        Assert.Equal(id2, Assert.Single(pending).Id);
        Assert.Empty(await group.ReadOwnPendingAsync("c2", "0", 10));
    }

    [RedisFact]
    public async Task Idle_entries_of_a_crashed_consumer_can_be_claimed()
    {
        var log = new RedisLocationLog(redis.Connection, Keys, _options);
        var group = new RedisLocationConsumerGroup(redis.Connection, Keys, _options);
        await group.EnsureConsumerGroupAsync();
        var id = await log.AppendAsync(Ping("a", 1));
        await group.ReadNewAsync("crashed", 10);

        var tooEarly = await group.ClaimAbandonedAsync("survivor", TimeSpan.FromMinutes(5), "0-0", 10);
        await Task.Delay(50);
        var claimed = await group.ClaimAbandonedAsync("survivor", TimeSpan.FromMilliseconds(20), "0-0", 10);

        Assert.Empty(tooEarly.Entries);
        Assert.Equal(id, Assert.Single(claimed.Entries).Id);
        Assert.Equal("0-0", claimed.NextStartId);
        Assert.Equal(id, Assert.Single(await group.ReadOwnPendingAsync("survivor", "0", 10)).Id);
        Assert.Empty(await group.ReadOwnPendingAsync("crashed", "0", 10));
    }

    [RedisFact]
    public async Task Dead_lettered_entries_are_copied_and_acknowledged()
    {
        var log = new RedisLocationLog(redis.Connection, Keys, _options);
        var group = new RedisLocationConsumerGroup(redis.Connection, Keys, _options);
        await group.EnsureConsumerGroupAsync();
        await log.AppendAsync(Ping("a", 1));
        var entry = Assert.Single(await group.ReadNewAsync("c1", 10));

        await group.DeadLetterAsync(entry, "bad data");

        Assert.Empty(await group.ReadOwnPendingAsync("c1", "0", 10));
        var dead = Assert.Single(await redis.Connection.GetDatabase().StreamRangeAsync(Keys.DeadLetters));
        Assert.Equal("bad data", dead["dead.reason"].ToString());
        Assert.Equal(entry.Id, dead["dead.sourceId"].ToString());
        Assert.Equal("a", dead["device"].ToString());
    }
}
