using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Tracking.Core.Geo;
using Tracking.Ingest.Services;
using Tracking.Testing;
using DomainPing = Tracking.Core.Devices.LocationPing;

namespace Tracking.Ingest.Tests;

public sealed class PingAppenderTests : IAsyncLifetime, IDisposable
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);
    private const long Epoch = 1_780_000_000;

    private readonly InMemoryLocationLog _log = new();
    private readonly TestMeterFactory _meters = new();
    private PingAppender _appender = null!;

    public async Task InitializeAsync()
    {
        var options = new IngestOptions { AppendQueueCapacity = 4, MaxBatchSize = 100 };
        _appender = new PingAppender(_log, new IngestMetrics(_meters), Options.Create(options), NullLogger<PingAppender>.Instance);
        await _appender.StartAsync(CancellationToken.None);
    }

    public async Task DisposeAsync()
    {
        _log.Open();
        await _appender.StopAsync(CancellationToken.None);
    }

    public void Dispose()
    {
        _appender.Dispose();
        _meters.Dispose();
    }

    private static DomainPing Ping(string device, int second) =>
        new(device, new GeoPoint(51.5, -0.12), DateTimeOffset.FromUnixTimeSeconds(Epoch + second));

    [Fact]
    public async Task Pings_queued_while_a_batch_is_in_flight_go_out_together()
    {
        _log.Close();
        var first = _appender.AppendAsync([Ping("a", 0)], CancellationToken.None);
        await WaitUntil(() => _appender.Queued == 0); // the flush loop took it and is blocked on the log

        var second = _appender.AppendAsync([Ping("a", 1), Ping("a", 2)], CancellationToken.None);
        var third = _appender.AppendAsync([Ping("b", 1)], CancellationToken.None);
        await WaitUntil(() => _appender.Queued == 3);
        _log.Open();

        await Task.WhenAll(first, second, third).WaitAsync(Timeout);
        Assert.Equal([1, 3], _log.BatchSizes);
        Assert.Equal(
            [("a", 0L), ("a", 1L), ("a", 2L), ("b", 1L)],
            _log.Entries.Select(e => (e.Ping.DeviceId, e.Ping.RecordedAt.ToUnixTimeSeconds() - Epoch)));
        Assert.Equal(_log.Entries.Skip(1).Take(2).Select(e => e.Id), (await second).Select(id => id!));
    }

    [Fact]
    public async Task A_full_queue_holds_callers_back_until_the_log_catches_up()
    {
        _log.Close();
        var inFlight = _appender.AppendAsync([Ping("a", 0)], CancellationToken.None);
        await WaitUntil(() => _appender.Queued == 0);

        // Capacity is 4: the fifth ping cannot be queued, so this call cannot even finish queueing.
        var blocked = _appender.AppendAsync([.. Enumerable.Range(1, 6).Select(i => Ping("a", i))], CancellationToken.None);
        await WaitUntil(() => _appender.Queued == 4);
        await Task.Delay(100);
        Assert.Equal(4, _appender.Queued);
        Assert.False(blocked.IsCompleted);

        _log.Open();
        var ids = await blocked.WaitAsync(Timeout);
        await inFlight.WaitAsync(Timeout);

        Assert.Equal(6, ids.Count);
        Assert.All(ids, Assert.NotNull);
        Assert.Equal(7, _log.Entries.Count);
    }

    [Fact]
    public async Task Failed_appends_come_back_as_null_without_failing_the_rest()
    {
        _log.FailWhere = ping => ping.RecordedAt.ToUnixTimeSeconds() % 2 == 1;

        var ids = await _appender.AppendAsync([Ping("a", 0), Ping("a", 1), Ping("a", 2)], CancellationToken.None).WaitAsync(Timeout);

        Assert.NotNull(ids[0]);
        Assert.Null(ids[1]);
        Assert.NotNull(ids[2]);
    }

    [Fact]
    public async Task An_unreachable_log_fails_the_whole_batch_softly()
    {
        _log.Unavailable = true;

        var ids = await _appender.AppendAsync([Ping("a", 0), Ping("a", 1)], CancellationToken.None).WaitAsync(Timeout);

        Assert.All(ids, Assert.Null);
    }

    [Fact]
    public async Task Stopping_appends_what_was_already_queued_and_refuses_the_rest()
    {
        _log.Close();
        var queued = _appender.AppendAsync([Ping("a", 0), Ping("a", 1)], CancellationToken.None);
        await WaitUntil(() => _appender.Queued <= 1);

        var stopping = _appender.StopAsync(CancellationToken.None);
        _log.Open();
        await stopping.WaitAsync(Timeout);

        Assert.All(await queued.WaitAsync(Timeout), Assert.NotNull);
        Assert.Null(Assert.Single(await _appender.AppendAsync([Ping("a", 2)], CancellationToken.None)));
    }

    private static async Task WaitUntil(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow + Timeout;
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < deadline, "condition was not met in time");
            await Task.Delay(10);
        }
    }
}
