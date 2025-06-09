using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using Tracking.Analytics.Data;
using Tracking.Core.Analytics;
using Tracking.Core.Geofencing;
using Tracking.Testing;

namespace Tracking.Analytics.Tests;

public sealed class AnalyticsAggregatorTests : IDisposable
{
    private static readonly DateTimeOffset Day1 = new(2026, 10, 5, 0, 0, 0, TimeSpan.Zero);

    private readonly SqliteAnalyticsDatabase _database = new();
    private readonly ManualClock _clock = new(Day1.AddDays(2));

    public void Dispose() => _database.Dispose();

    private async Task<AggregationResult> ApplyAsync(params AnalyticsEvent[] events)
    {
        await using var db = _database.CreateContext();
        var aggregator = new AnalyticsAggregator(db, _clock, Options.Create(new AnalyticsDataOptions { DwellThreshold = TimeSpan.FromMinutes(10) }));
        return await aggregator.ApplyAsync(events);
    }

    private static PositionEvent Position(string device, DateTimeOffset at, double distance) =>
        new(AnalyticsEvents.PositionEventId(device, at), device, 52, 4, at, null, null, null, distance, null, []);

    private static GeofenceEvent Transition(string device, string fence, GeofenceTransition transition, DateTimeOffset at) =>
        new($"{device}/{fence}/{transition}/{at.ToUnixTimeMilliseconds()}", device, fence, $"Fence {fence}", transition, 52, 4, at);

    [Fact]
    public async Task Distances_are_summed_per_device_and_utc_day()
    {
        await ApplyAsync(
            Position("van", Day1.AddHours(8), 0),
            Position("van", Day1.AddHours(9), 1_200.5),
            Position("van", Day1.AddHours(23).AddMinutes(59), 300),
            Position("bus", Day1.AddHours(10), 50));
        await ApplyAsync(Position("van", Day1.AddDays(1).AddMinutes(1), 75));

        await using var db = _database.CreateContext();
        var rows = await db.DailyDistances.OrderBy(d => d.DeviceId).ThenBy(d => d.Day).ToListAsync();

        Assert.Equal(3, rows.Count);
        var vanDay1 = rows.Single(r => r.DeviceId == "van" && r.Day == DateOnly.FromDateTime(Day1.UtcDateTime));
        Assert.Equal(1_500.5, vanDay1.DistanceMeters, precision: 6);
        Assert.Equal(3, vanDay1.Positions);
        Assert.Equal(Day1.AddHours(8).UtcDateTime, vanDay1.FirstRecordedAt);
        Assert.Equal(Day1.AddHours(23).AddMinutes(59).UtcDateTime, vanDay1.LastRecordedAt);
        Assert.Equal(75, rows.Single(r => r.DeviceId == "van" && r.Day == DateOnly.FromDateTime(Day1.AddDays(1).UtcDateTime)).DistanceMeters);
    }

    [Fact]
    public async Task Replayed_events_are_not_applied_twice()
    {
        var events = new AnalyticsEvent[]
        {
            Position("van", Day1.AddHours(1), 100),
            Position("van", Day1.AddHours(2), 200),
            Transition("van", "depot", GeofenceTransition.Enter, Day1.AddHours(1)),
        };

        var first = await ApplyAsync(events);
        var replay = await ApplyAsync([.. events, Position("van", Day1.AddHours(3), 50)]);
        var duplicateInBatch = await ApplyAsync(Position("van", Day1.AddHours(4), 10), Position("van", Day1.AddHours(4), 10));

        Assert.Equal(new AggregationResult(3, 0), first);
        Assert.Equal(new AggregationResult(1, 3), replay);
        Assert.Equal(new AggregationResult(1, 1), duplicateInBatch);
        await using var db = _database.CreateContext();
        Assert.Equal(360, (await db.DailyDistances.SingleAsync()).DistanceMeters);
        Assert.Equal(1, await db.GeofenceVisits.CountAsync());
    }

    [Fact]
    public async Task Enter_and_exit_pair_into_a_visit_and_long_visits_become_dwell_events()
    {
        await ApplyAsync(
            Transition("van", "depot", GeofenceTransition.Enter, Day1.AddHours(8)),
            Transition("van", "depot", GeofenceTransition.Exit, Day1.AddHours(8).AddMinutes(5)),
            Transition("van", "depot", GeofenceTransition.Enter, Day1.AddHours(9)));
        await ApplyAsync(Transition("van", "depot", GeofenceTransition.Exit, Day1.AddHours(9).AddMinutes(30)));

        await using var db = _database.CreateContext();
        var visits = await db.GeofenceVisits.OrderBy(v => v.EnteredAt).ToListAsync();
        Assert.Equal(2, visits.Count);
        Assert.All(visits, v => Assert.NotNull(v.ExitedAt));
        var dwell = await db.DwellEvents.SingleAsync();
        Assert.Equal(Day1.AddHours(9).UtcDateTime, dwell.EnteredAt);
        Assert.Equal(1_800, dwell.DurationSeconds);
        Assert.Equal("Fence depot", dwell.GeofenceName);
    }

    [Fact]
    public async Task An_exit_that_arrives_before_its_enter_is_paired_when_the_enter_arrives()
    {
        await ApplyAsync(Transition("van", "depot", GeofenceTransition.Exit, Day1.AddHours(2)));
        await ApplyAsync(Transition("van", "depot", GeofenceTransition.Enter, Day1.AddHours(3)));
        await ApplyAsync(Transition("van", "depot", GeofenceTransition.Enter, Day1.AddHours(1)));

        await using var db = _database.CreateContext();
        var visits = await db.GeofenceVisits.OrderBy(v => v.EnteredAt).ToListAsync();

        Assert.Equal(2, visits.Count);
        Assert.Equal((Day1.AddHours(1).UtcDateTime, Day1.AddHours(2).UtcDateTime), (visits[0].EnteredAt!.Value, visits[0].ExitedAt!.Value));
        Assert.Equal(Day1.AddHours(3).UtcDateTime, visits[1].EnteredAt);
        Assert.Null(visits[1].ExitedAt);
        Assert.Single(await db.DwellEvents.ToListAsync());
    }

    [Fact]
    public async Task Out_of_order_transitions_within_one_batch_pair_correctly()
    {
        await ApplyAsync(
            Transition("van", "depot", GeofenceTransition.Exit, Day1.AddHours(4)),
            Transition("van", "depot", GeofenceTransition.Exit, Day1.AddHours(2)),
            Transition("van", "depot", GeofenceTransition.Enter, Day1.AddHours(3)),
            Transition("van", "depot", GeofenceTransition.Enter, Day1.AddHours(1)),
            Transition("van", "yard", GeofenceTransition.Enter, Day1.AddHours(1)));

        await using var db = _database.CreateContext();
        var depot = await db.GeofenceVisits.Where(v => v.GeofenceId == "depot").OrderBy(v => v.EnteredAt).ToListAsync();

        Assert.Equal(
            [(Day1.AddHours(1).UtcDateTime, Day1.AddHours(2).UtcDateTime), (Day1.AddHours(3).UtcDateTime, Day1.AddHours(4).UtcDateTime)],
            depot.Select(v => (v.EnteredAt!.Value, v.ExitedAt!.Value)));
        Assert.Equal(2, await db.DwellEvents.CountAsync());
        Assert.Null((await db.GeofenceVisits.SingleAsync(v => v.GeofenceId == "yard")).ExitedAt);
    }

    [Fact]
    public async Task The_ledger_can_be_pruned_by_age()
    {
        await ApplyAsync(Position("van", Day1, 1));
        _clock.Advance(TimeSpan.FromDays(10));
        await ApplyAsync(Position("van", Day1.AddHours(1), 1));

        await using var db = _database.CreateContext();
        var aggregator = new AnalyticsAggregator(db, _clock, Options.Create(new AnalyticsDataOptions()));
        var pruned = await aggregator.PruneLedgerAsync(TimeSpan.FromDays(7));

        Assert.Equal(1, pruned);
        Assert.Equal(1, await db.ProcessedEvents.CountAsync());
    }
}
