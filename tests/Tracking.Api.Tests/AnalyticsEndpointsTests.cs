using System.Net;
using System.Text.Json;

namespace Tracking.Api.Tests;

public sealed class AnalyticsEndpointsTests : IClassFixture<ApiFactory>
{
    // ApiFactory.Now is 2026-08-01 09:00 UTC.
    private static readonly DateTime Today = ApiFactory.Now.UtcDateTime.Date;

    private readonly HttpClient _client;

    public AnalyticsEndpointsTests(ApiFactory factory)
    {
        _client = factory.CreateClient();

        using var db = factory.Analytics();
        if (db.DailyDistances.Any())
        {
            return;
        }

        db.DailyDistances.AddRange(
            new() { DeviceId = "an-van", Day = DateOnly.FromDateTime(Today), DistanceMeters = 1_234.567, Positions = 10, FirstRecordedAt = Today, LastRecordedAt = Today.AddHours(8) },
            new() { DeviceId = "an-van", Day = DateOnly.FromDateTime(Today.AddDays(-3)), DistanceMeters = 100, Positions = 2, FirstRecordedAt = Today.AddDays(-3), LastRecordedAt = Today.AddDays(-3) },
            new() { DeviceId = "an-van", Day = DateOnly.FromDateTime(Today.AddDays(-30)), DistanceMeters = 999, Positions = 1, FirstRecordedAt = Today.AddDays(-30), LastRecordedAt = Today.AddDays(-30) });
        db.GeofenceVisits.AddRange(
            new() { DeviceId = "an-van", GeofenceId = "depot", GeofenceName = "Depot", EnteredAt = Today.AddHours(6), ExitedAt = Today.AddHours(7) },
            new() { DeviceId = "an-van", GeofenceId = "yard", GeofenceName = "Yard", EnteredAt = Today.AddHours(8).AddMinutes(30), ExitedAt = null });
        db.DwellEvents.Add(new() { DeviceId = "an-van", GeofenceId = "depot", GeofenceName = "Depot", EnteredAt = Today.AddHours(6), ExitedAt = Today.AddHours(7), DurationSeconds = 3_600 });
        db.SaveChanges();
    }

    private async Task<(HttpStatusCode Status, JsonElement Body)> GetAsync(string url)
    {
        var response = await _client.GetAsync(new Uri(url, UriKind.Relative));
        return (response.StatusCode, JsonDocument.Parse(await response.Content.ReadAsStringAsync()).RootElement);
    }

    [Fact]
    public async Task Daily_distance_defaults_to_the_last_seven_days()
    {
        var (status, body) = await GetAsync("/api/analytics/devices/an-van/distance");

        Assert.Equal(HttpStatusCode.OK, status);
        Assert.Equal("2026-07-26", body.GetProperty("from").GetString());
        Assert.Equal("2026-08-01", body.GetProperty("to").GetString());
        Assert.Equal(1_334.57, body.GetProperty("totalMeters").GetDouble());
        Assert.Equal(2, body.GetProperty("days").GetArrayLength());
        Assert.Equal(10, body.GetProperty("days")[1].GetProperty("positions").GetInt32());
    }

    [Fact]
    public async Task Geofence_time_reports_closed_and_ongoing_visits()
    {
        var (status, body) = await GetAsync("/api/analytics/devices/an-van/geofence-time");

        Assert.Equal(HttpStatusCode.OK, status);
        var fences = body.GetProperty("geofences").EnumerateArray().ToDictionary(g => g.GetProperty("geofenceId").GetString()!);
        Assert.Equal(3_600, fences["depot"].GetProperty("secondsInside").GetDouble());
        Assert.False(fences["depot"].GetProperty("ongoing").GetBoolean());
        Assert.Equal(1_800, fences["yard"].GetProperty("secondsInside").GetDouble()); // 08:30 until now (09:00)
        Assert.True(fences["yard"].GetProperty("ongoing").GetBoolean());
    }

    [Fact]
    public async Task Dwell_events_can_be_filtered_by_geofence()
    {
        var (_, depot) = await GetAsync("/api/analytics/dwell-events?geofenceId=depot");
        var (_, yard) = await GetAsync("/api/analytics/dwell-events?geofenceId=yard");

        Assert.Equal(3_600, Assert.Single(depot.EnumerateArray()).GetProperty("durationSeconds").GetDouble());
        Assert.Empty(yard.EnumerateArray());
    }

    [Theory]
    [InlineData("/api/analytics/devices/bad%20id/distance")]
    [InlineData("/api/analytics/devices/an-van/distance?from=2026-08-02&to=2026-08-01")]
    [InlineData("/api/analytics/devices/an-van/distance?from=2024-01-01&to=2026-08-01")]
    [InlineData("/api/analytics/devices/an-van/geofence-time?from=2026-08-01T10:00:00Z&to=2026-08-01T09:00:00Z")]
    [InlineData("/api/analytics/devices/an-van/geofence-time?from=2026-01-01T00:00:00Z&to=2026-08-01T00:00:00Z")]
    [InlineData("/api/analytics/dwell-events?limit=0")]
    [InlineData("/api/analytics/dwell-events?geofenceId=no%20spaces")]
    public async Task Invalid_report_queries_are_rejected(string url)
    {
        var response = await _client.GetAsync(new Uri(url, UriKind.Relative));

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Equal("application/problem+json", response.Content.Headers.ContentType?.MediaType);
    }
}
