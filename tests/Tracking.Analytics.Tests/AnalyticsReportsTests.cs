using Tracking.Analytics.Data;

namespace Tracking.Analytics.Tests;

public sealed class AnalyticsReportsTests : IDisposable
{
    private static readonly DateTime Day1 = new(2026, 10, 5, 0, 0, 0, DateTimeKind.Utc);

    private readonly SqliteAnalyticsDatabase _database = new();

    public AnalyticsReportsTests()
    {
        using var db = _database.CreateContext();
        db.DailyDistances.AddRange(
            new DeviceDailyDistance { DeviceId = "van", Day = DateOnly.FromDateTime(Day1), DistanceMeters = 1_000, Positions = 3, FirstRecordedAt = Day1, LastRecordedAt = Day1.AddHours(5) },
            new DeviceDailyDistance { DeviceId = "van", Day = DateOnly.FromDateTime(Day1.AddDays(1)), DistanceMeters = 500, Positions = 2, FirstRecordedAt = Day1.AddDays(1), LastRecordedAt = Day1.AddDays(1) },
            new DeviceDailyDistance { DeviceId = "van", Day = DateOnly.FromDateTime(Day1.AddDays(5)), DistanceMeters = 9, Positions = 1, FirstRecordedAt = Day1.AddDays(5), LastRecordedAt = Day1.AddDays(5) },
            new DeviceDailyDistance { DeviceId = "bus", Day = DateOnly.FromDateTime(Day1), DistanceMeters = 7, Positions = 1, FirstRecordedAt = Day1, LastRecordedAt = Day1 });
        db.GeofenceVisits.AddRange(
            new GeofenceVisit { DeviceId = "van", GeofenceId = "depot", GeofenceName = "Depot", EnteredAt = Day1.AddHours(-1), ExitedAt = Day1.AddHours(1) },
            new GeofenceVisit { DeviceId = "van", GeofenceId = "depot", GeofenceName = "Depot", EnteredAt = Day1.AddHours(10), ExitedAt = Day1.AddHours(11) },
            new GeofenceVisit { DeviceId = "van", GeofenceId = "yard", GeofenceName = "Yard", EnteredAt = Day1.AddHours(20), ExitedAt = null },
            new GeofenceVisit { DeviceId = "van", GeofenceId = "dock", GeofenceName = "Dock", EnteredAt = null, ExitedAt = Day1.AddHours(3) },
            new GeofenceVisit { DeviceId = "bus", GeofenceId = "depot", GeofenceName = "Depot", EnteredAt = Day1, ExitedAt = Day1.AddHours(2) });
        db.DwellEvents.AddRange(
            new DwellEvent { DeviceId = "van", GeofenceId = "depot", GeofenceName = "Depot", EnteredAt = Day1.AddHours(10), ExitedAt = Day1.AddHours(11), DurationSeconds = 3_600 },
            new DwellEvent { DeviceId = "bus", GeofenceId = "depot", GeofenceName = "Depot", EnteredAt = Day1, ExitedAt = Day1.AddHours(2), DurationSeconds = 7_200 },
            new DwellEvent { DeviceId = "van", GeofenceId = "yard", GeofenceName = "Yard", EnteredAt = Day1.AddDays(-3), ExitedAt = Day1.AddDays(-2), DurationSeconds = 86_400 });
        db.SaveChanges();
    }

    public void Dispose() => _database.Dispose();

    [Fact]
    public async Task Daily_distance_covers_the_requested_days_in_order()
    {
        await using var db = _database.CreateContext();

        var report = await new AnalyticsReports(db).GetDailyDistanceAsync("van", DateOnly.FromDateTime(Day1), DateOnly.FromDateTime(Day1.AddDays(1)));

        Assert.Equal([1_000d, 500d], report.Select(r => r.DistanceMeters));
        Assert.Equal(DateOnly.FromDateTime(Day1), report[0].Day);
        Assert.Equal(3, report[0].Positions);
    }

    [Fact]
    public async Task Time_inside_clips_visits_to_the_window_and_counts_open_visits_up_to_now()
    {
        await using var db = _database.CreateContext();

        var report = await new AnalyticsReports(db).GetGeofenceTimeAsync("van", Day1, Day1.AddDays(1), now: Day1.AddHours(22));

        var depot = report.Single(r => r.GeofenceId == "depot");
        Assert.Equal(TimeSpan.FromHours(2).TotalSeconds, depot.SecondsInside); // 1 h clipped + 1 h
        Assert.Equal(2, depot.Visits);
        Assert.False(depot.Ongoing);
        var yard = report.Single(r => r.GeofenceId == "yard");
        Assert.Equal(TimeSpan.FromHours(2).TotalSeconds, yard.SecondsInside);
        Assert.True(yard.Ongoing);
        Assert.DoesNotContain(report, r => r.GeofenceId == "dock"); // its start is unknown
    }

    [Fact]
    public async Task Dwell_events_filter_by_device_geofence_and_window_newest_first()
    {
        await using var db = _database.CreateContext();
        var reports = new AnalyticsReports(db);

        var all = await reports.GetDwellEventsAsync(null, null, Day1.AddDays(-7), Day1.AddDays(1), 10);
        var vanOnly = await reports.GetDwellEventsAsync("van", null, Day1, Day1.AddDays(1), 10);
        var limited = await reports.GetDwellEventsAsync(null, "depot", Day1, Day1.AddDays(1), 1);

        Assert.Equal(["van", "bus", "van"], all.Select(d => d.DeviceId));
        Assert.Equal(3_600, Assert.Single(vanOnly).DurationSeconds);
        Assert.Equal("van", Assert.Single(limited).DeviceId);
    }
}
