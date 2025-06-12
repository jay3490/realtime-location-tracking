using Microsoft.AspNetCore.Http.HttpResults;
using Tracking.Analytics.Data;
using Tracking.Core.Devices;
using Tracking.Core.Geofencing;

namespace Tracking.Api.Endpoints;

public sealed record DailyDistanceResponse(string DeviceId, DateOnly From, DateOnly To, double TotalMeters, IReadOnlyList<DailyDistanceReport> Days);

public sealed record GeofenceTimeResponse(string DeviceId, DateTimeOffset From, DateTimeOffset To, IReadOnlyList<GeofenceTimeReport> Geofences);

/// <summary>
/// Reports over the analytics database that Tracking.Analytics fills from Kafka. They lag the live
/// state by however long events take to pass through the outbox, Kafka and the consumer.
/// </summary>
public static class AnalyticsEndpoints
{
    public const int MaxDistanceDays = 366;
    public const int MaxGeofenceWindowDays = 31;
    public const int MaxDwellLimit = 1_000;

    public static RouteGroupBuilder MapAnalyticsEndpoints(this IEndpointRouteBuilder routes)
    {
        var group = routes.MapGroup("/api/analytics").WithTags("Analytics");
        group.MapGet("/devices/{deviceId}/distance", GetDailyDistanceAsync);
        group.MapGet("/devices/{deviceId}/geofence-time", GetGeofenceTimeAsync);
        group.MapGet("/dwell-events", GetDwellEventsAsync);
        return group;
    }

    /// <summary>Distance per UTC day for one device; defaults to the last 7 days including today.</summary>
    private static async Task<Results<Ok<DailyDistanceResponse>, ValidationProblem>> GetDailyDistanceAsync(
        string deviceId,
        AnalyticsReports reports,
        TimeProvider clock,
        CancellationToken cancellationToken,
        DateOnly? from = null,
        DateOnly? to = null)
    {
        var today = DateOnly.FromDateTime(clock.GetUtcNow().UtcDateTime);
        var end = to ?? today;
        var start = from ?? end.AddDays(-6);

        var errors = new Dictionary<string, string[]>();
        ValidateDevice(deviceId, errors);
        if (end < start || end.DayNumber - start.DayNumber >= MaxDistanceDays)
        {
            errors["from,to"] = [$"from must not be after to, and the range may span at most {MaxDistanceDays} days"];
        }

        if (errors.Count > 0)
        {
            return TypedResults.ValidationProblem(errors);
        }

        var days = await reports.GetDailyDistanceAsync(deviceId, start, end, cancellationToken);
        return TypedResults.Ok(new DailyDistanceResponse(deviceId, start, end, Math.Round(days.Sum(d => d.DistanceMeters), 2), days));
    }

    /// <summary>Time inside each geofence within a window; defaults to the last 24 hours.</summary>
    private static async Task<Results<Ok<GeofenceTimeResponse>, ValidationProblem>> GetGeofenceTimeAsync(
        string deviceId,
        AnalyticsReports reports,
        TimeProvider clock,
        CancellationToken cancellationToken,
        DateTimeOffset? from = null,
        DateTimeOffset? to = null)
    {
        var now = clock.GetUtcNow();
        var end = to ?? now;
        var start = from ?? end.AddDays(-1);

        var errors = new Dictionary<string, string[]>();
        ValidateDevice(deviceId, errors);
        if (end <= start || end - start > TimeSpan.FromDays(MaxGeofenceWindowDays))
        {
            errors["from,to"] = [$"from must be before to, and the window may span at most {MaxGeofenceWindowDays} days"];
        }

        if (errors.Count > 0)
        {
            return TypedResults.ValidationProblem(errors);
        }

        var geofences = await reports.GetGeofenceTimeAsync(deviceId, start.UtcDateTime, end.UtcDateTime, now.UtcDateTime, cancellationToken);
        return TypedResults.Ok(new GeofenceTimeResponse(deviceId, start, end, geofences));
    }

    /// <summary>Visits that lasted at least the dwell threshold and ended in the window, newest first.</summary>
    private static async Task<Results<Ok<IReadOnlyList<DwellReport>>, ValidationProblem>> GetDwellEventsAsync(
        AnalyticsReports reports,
        TimeProvider clock,
        CancellationToken cancellationToken,
        string? deviceId = null,
        string? geofenceId = null,
        DateTimeOffset? from = null,
        DateTimeOffset? to = null,
        int limit = 100)
    {
        var end = to ?? clock.GetUtcNow();
        var start = from ?? end.AddDays(-7);

        var errors = new Dictionary<string, string[]>();
        if (deviceId is not null)
        {
            ValidateDevice(deviceId, errors);
        }

        if (geofenceId is not null && !Geofence.IsValidId(geofenceId))
        {
            errors["geofenceId"] = [$"must be 1-{Geofence.MaxIdLength} characters of letters, digits, '-' or '_'"];
        }

        if (end <= start)
        {
            errors["from,to"] = ["from must be before to"];
        }

        if (limit is < 1 or > MaxDwellLimit)
        {
            errors["limit"] = [$"must be between 1 and {MaxDwellLimit}"];
        }

        if (errors.Count > 0)
        {
            return TypedResults.ValidationProblem(errors);
        }

        return TypedResults.Ok(await reports.GetDwellEventsAsync(deviceId, geofenceId, start.UtcDateTime, end.UtcDateTime, limit, cancellationToken));
    }

    private static void ValidateDevice(string deviceId, Dictionary<string, string[]> errors)
    {
        if (!DeviceId.IsValid(deviceId))
        {
            errors["deviceId"] = [$"must be 1-{DeviceId.MaxLength} characters of letters, digits, '.', ':', '_' or '-'"];
        }
    }
}
