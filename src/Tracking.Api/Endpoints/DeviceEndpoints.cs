using Microsoft.AspNetCore.Http.HttpResults;
using Tracking.Core.Abstractions;
using Tracking.Core.Devices;
using Tracking.Core.Geo;

namespace Tracking.Api.Endpoints;

public sealed record DevicePositionResponse(
    string DeviceId,
    double Latitude,
    double Longitude,
    DateTimeOffset RecordedAt,
    double? SpeedMetersPerSecond,
    double? HeadingDegrees,
    IReadOnlyList<string> InsideGeofenceIds);

public sealed record NearbyDeviceResponse(
    string DeviceId,
    double Latitude,
    double Longitude,
    double DistanceMeters,
    DateTimeOffset? RecordedAt);

public static class DeviceEndpoints
{
    public const double MaxRadiusMeters = 100_000;
    public const int MaxLimit = 500;

    public static RouteGroupBuilder MapDeviceEndpoints(this IEndpointRouteBuilder routes)
    {
        var group = routes.MapGroup("/api/devices").WithTags("Devices");
        group.MapGet("/{deviceId}/position", GetPositionAsync);
        group.MapGet("/nearby", FindNearbyAsync);
        return group;
    }

    private static async Task<Results<Ok<DevicePositionResponse>, NotFound, ValidationProblem>> GetPositionAsync(
        string deviceId,
        IDeviceStateStore states,
        CancellationToken cancellationToken)
    {
        if (!DeviceId.IsValid(deviceId))
        {
            return TypedResults.ValidationProblem(new Dictionary<string, string[]>
            {
                ["deviceId"] = ["must be 1-64 characters of letters, digits, '.', ':', '_' or '-'"],
            });
        }

        var state = await states.GetAsync(deviceId, cancellationToken);
        if (state is null)
        {
            return TypedResults.NotFound();
        }

        return TypedResults.Ok(new DevicePositionResponse(
            state.DeviceId,
            state.Position.Latitude,
            state.Position.Longitude,
            state.RecordedAt,
            state.SpeedMetersPerSecond,
            state.HeadingDegrees,
            state.InsideGeofenceIds));
    }

    /// <summary>
    /// Devices whose latest position is within the radius, nearest first. <paramref name="maxAgeSeconds"/>
    /// drops devices that have not reported recently, since the index keeps every device's last position.
    /// </summary>
    private static async Task<Results<Ok<NearbyDeviceResponse[]>, ValidationProblem>> FindNearbyAsync(
        double? lat,
        double? lon,
        IDeviceStateStore states,
        TimeProvider clock,
        CancellationToken cancellationToken,
        double radiusMeters = 1_000,
        int limit = 50,
        int? maxAgeSeconds = null)
    {
        var errors = new Dictionary<string, string[]>();
        if (lat is null || lon is null || !new GeoPoint(lat.Value, lon.Value).IsValid)
        {
            errors["lat,lon"] = ["lat in [-90, 90] and lon in [-180, 180] are required"];
        }

        if (!double.IsFinite(radiusMeters) || radiusMeters <= 0 || radiusMeters > MaxRadiusMeters)
        {
            errors["radiusMeters"] = [$"must be greater than 0 and at most {MaxRadiusMeters:0}"];
        }

        if (limit is < 1 or > MaxLimit)
        {
            errors["limit"] = [$"must be between 1 and {MaxLimit}"];
        }

        if (maxAgeSeconds is <= 0)
        {
            errors["maxAgeSeconds"] = ["must be positive"];
        }

        if (errors.Count > 0)
        {
            return TypedResults.ValidationProblem(errors);
        }

        var nearby = await states.FindNearbyAsync(new GeoPoint(lat!.Value, lon!.Value), radiusMeters, limit, cancellationToken);
        var cutoff = maxAgeSeconds is { } age ? clock.GetUtcNow().AddSeconds(-age) : (DateTimeOffset?)null;

        return TypedResults.Ok(nearby
            .Where(n => cutoff is null || (n.RecordedAt is { } at && at >= cutoff))
            .Select(n => new NearbyDeviceResponse(n.DeviceId, n.Position.Latitude, n.Position.Longitude, Math.Round(n.DistanceMeters, 2), n.RecordedAt))
            .ToArray());
    }
}
