using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Tracking.Analytics.Data;
using Tracking.Core.Analytics;
using Tracking.Kafka;
using Tracking.Testing;

namespace Tracking.Analytics.Tests;

public sealed class AnalyticsIngestWorkerTests : IDisposable
{
    private static readonly DateTimeOffset T0 = new(2026, 10, 5, 12, 0, 0, TimeSpan.Zero);

    private readonly TestMeterFactory _meters = new();
    private readonly MetricRecorder _recorder;
    private readonly FakeSource _source = new();
    private readonly FakeSink _sink = new();

    public AnalyticsIngestWorkerTests() => _recorder = new MetricRecorder(_meters);

    public void Dispose()
    {
        _recorder.Dispose();
        _meters.Dispose();
    }

    private AnalyticsIngestWorker Worker(IAnalyticsEventSink? sink = null) => new(
        _source,
        sink ?? _sink,
        new AnalyticsMetrics(_meters),
        Options.Create(new AnalyticsConsumerOptions()),
        Options.Create(new AnalyticsServiceOptions { ErrorDelay = TimeSpan.FromMilliseconds(10), MaxErrorDelay = TimeSpan.FromMilliseconds(40) }),
        TimeProvider.System,
        NullLogger<AnalyticsIngestWorker>.Instance);

    private static ConsumedEvent Consumed(int partition, long offset, AnalyticsEvent? analyticsEvent, string? error = null) =>
        new(analyticsEvent, new TopicPartitionOffset("tracking.positions.v1", partition, offset), error);

    private static PositionEvent Position(int second) =>
        new(AnalyticsEvents.PositionEventId("van", T0.AddSeconds(second)), "van", 52, 4, T0.AddSeconds(second), null, null, null, 10, null, []);

    [Fact]
    public async Task A_batch_is_committed_only_after_it_was_applied_and_bad_messages_are_skipped()
    {
        var batch = new[]
        {
            Consumed(0, 7, Position(1)),
            Consumed(0, 8, null, "unexpected end of JSON"),
            Consumed(1, 3, Position(2)),
        };

        await Worker().HandleBatchAsync(batch, CancellationToken.None);

        Assert.Equal(["van/" + T0.AddSeconds(1).ToUnixTimeMilliseconds(), "van/" + T0.AddSeconds(2).ToUnixTimeMilliseconds()], _sink.Applied.Select(e => e.EventId));
        Assert.Same(batch, Assert.Single(_source.Committed));
        Assert.Equal(1, _recorder.Sum("tracking.analytics.events", "outcome", "invalid"));
        Assert.Equal(2, _recorder.Sum("tracking.analytics.events", "outcome", "applied"));
    }

    [Fact]
    public async Task A_failing_database_holds_the_batch_and_its_offsets_until_it_recovers()
    {
        _sink.FailuresLeft = 3;
        var batch = new[] { Consumed(0, 1, Position(1)) };

        await Worker().HandleBatchAsync(batch, CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(4, _sink.Attempts);
        Assert.Single(_source.Committed);
    }

    [Fact]
    public async Task Stopping_while_the_database_is_down_commits_nothing()
    {
        _sink.FailuresLeft = int.MaxValue;
        using var stopping = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => Worker().HandleBatchAsync([Consumed(0, 1, Position(1))], stopping.Token));

        Assert.Empty(_source.Committed);
    }

    [Fact]
    public async Task The_scoped_sink_applies_each_batch_in_its_own_context()
    {
        using var database = new SqliteAnalyticsDatabase();
        var services = new ServiceCollection()
            .AddSingleton(TimeProvider.System)
            .AddSingleton(Options.Create(new AnalyticsDataOptions()))
            .AddDbContext<AnalyticsDbContext>(o => o.UseSqlite(database.Connection))
            .AddScoped<AnalyticsAggregator>();
        await using var provider = services.BuildServiceProvider();
        var sink = new ScopedAggregatorSink(provider.GetRequiredService<IServiceScopeFactory>());

        var first = await sink.ApplyAsync([Position(1), Position(2)], CancellationToken.None);
        var replay = await sink.ApplyAsync([Position(2)], CancellationToken.None);

        Assert.Equal(new AggregationResult(2, 0), first);
        Assert.Equal(new AggregationResult(0, 1), replay);
        await using var db = database.CreateContext();
        Assert.Equal(20, (await db.DailyDistances.SingleAsync()).DistanceMeters);
    }

    [Fact]
    public void The_consumer_commits_manually_and_starts_new_groups_from_the_oldest_event()
    {
        var config = KafkaAnalyticsEventSource.CreateConsumerConfig(
            new KafkaOptions { BootstrapServers = "kafka:9092" },
            new AnalyticsConsumerOptions { GroupId = "reports" });

        Assert.Equal("reports", config.GroupId);
        Assert.False(config.EnableAutoCommit);
        Assert.False(config.EnableAutoOffsetStore);
        Assert.Equal(AutoOffsetReset.Earliest, config.AutoOffsetReset);
        Assert.Equal(PartitionAssignmentStrategy.CooperativeSticky, config.PartitionAssignmentStrategy);
    }

    private sealed class FakeSource : IAnalyticsEventSource
    {
        public List<IReadOnlyList<ConsumedEvent>> Committed { get; } = [];

        public IReadOnlyList<ConsumedEvent> ConsumeBatch(int maxCount, TimeSpan maxWait, CancellationToken cancellationToken) => [];

        public void Commit(IReadOnlyList<ConsumedEvent> batch) => Committed.Add(batch);

        public void Dispose()
        {
        }
    }

    private sealed class FakeSink : IAnalyticsEventSink
    {
        public List<AnalyticsEvent> Applied { get; } = [];

        public int FailuresLeft { get; set; }

        public int Attempts { get; private set; }

        public Task<AggregationResult> ApplyAsync(IReadOnlyList<AnalyticsEvent> events, CancellationToken cancellationToken)
        {
            Attempts++;
            if (FailuresLeft > 0)
            {
                FailuresLeft--;
                throw new InvalidOperationException("database unavailable");
            }

            Applied.AddRange(events);
            return Task.FromResult(new AggregationResult(events.Count, 0));
        }
    }
}
