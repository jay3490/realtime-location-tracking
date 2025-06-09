using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using Npgsql;
using Tracking.Analytics.Data;
using Tracking.Core.Analytics;
using Tracking.Core.Geofencing;
using Tracking.Testing;

namespace Tracking.Analytics.Tests;

/// <summary>
/// Runs only when <c>TRACKING_TEST_POSTGRES</c> holds a connection string for a PostgreSQL server the
/// tests may create databases on (for example <c>Host=localhost;Username=postgres;Password=postgres</c>).
/// </summary>
public sealed class PostgresFactAttribute : FactAttribute
{
    public const string EnvironmentVariable = "TRACKING_TEST_POSTGRES";

    public PostgresFactAttribute()
    {
        if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(EnvironmentVariable)))
        {
            Skip = $"Set {EnvironmentVariable} to run tests against a real PostgreSQL.";
        }
    }
}

public sealed class PostgresMigrationTests
{
    [PostgresFact]
    public async Task Migrations_create_a_schema_the_aggregator_and_reports_work_against()
    {
        var builder = new NpgsqlConnectionStringBuilder(Environment.GetEnvironmentVariable(PostgresFactAttribute.EnvironmentVariable))
        {
            Database = $"tracking_test_{Guid.NewGuid():N}",
        };
        var options = new DbContextOptionsBuilder<AnalyticsDbContext>().UseNpgsql(builder.ConnectionString).Options;
        var day = new DateTimeOffset(2026, 10, 5, 8, 0, 0, TimeSpan.Zero);
        var clock = new ManualClock(day.AddDays(1));

        await using var db = new AnalyticsDbContext(options);
        try
        {
            await db.Database.MigrateAsync();
            Assert.Empty(await db.Database.GetPendingMigrationsAsync());

            var aggregator = new AnalyticsAggregator(db, clock, Options.Create(new AnalyticsDataOptions { DwellThreshold = TimeSpan.FromMinutes(1) }));
            AnalyticsEvent[] events =
            [
                new PositionEvent("van/1", "van", 52, 4, day, null, null, null, 0, null, []),
                new PositionEvent("van/2", "van", 52, 4, day.AddMinutes(10), null, 3, 90, 1_800, day, ["depot"]),
                new GeofenceEvent("van/depot/enter/1", "van", "depot", "Depot", GeofenceTransition.Enter, 52, 4, day.AddMinutes(10)),
                new GeofenceEvent("van/depot/exit/2", "van", "depot", "Depot", GeofenceTransition.Exit, 52, 4, day.AddMinutes(40)),
            ];

            Assert.Equal(new AggregationResult(4, 0), await aggregator.ApplyAsync(events));
            Assert.Equal(new AggregationResult(0, 4), await aggregator.ApplyAsync(events));

            var reports = new AnalyticsReports(db);
            var distance = Assert.Single(await reports.GetDailyDistanceAsync("van", DateOnly.FromDateTime(day.UtcDateTime), DateOnly.FromDateTime(day.UtcDateTime)));
            var inside = Assert.Single(await reports.GetGeofenceTimeAsync("van", day.UtcDateTime.Date, day.UtcDateTime.Date.AddDays(1), clock.Now.UtcDateTime));
            var dwell = Assert.Single(await reports.GetDwellEventsAsync("van", null, day.UtcDateTime.Date, day.UtcDateTime.Date.AddDays(1), 10));

            Assert.Equal(1_800, distance.DistanceMeters);
            Assert.Equal(DateTimeKind.Utc, distance.FirstRecordedAt.Kind);
            Assert.Equal(1_800, inside.SecondsInside);
            Assert.Equal(1_800, dwell.DurationSeconds);
        }
        finally
        {
            await db.Database.EnsureDeletedAsync();
        }
    }
}
