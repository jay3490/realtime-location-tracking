using Microsoft.AspNetCore.Http.HttpResults;
using Tracking.Core.Abstractions;
using Tracking.Core.Geofencing;

namespace Tracking.Api.Endpoints;

public static class AlertEndpoints
{
    public const int MaxLimit = 1_000;

    public static RouteGroupBuilder MapAlertEndpoints(this IEndpointRouteBuilder routes)
    {
        var group = routes.MapGroup("/api/alerts").WithTags("Alerts");
        group.MapGet("/", GetRecentAsync);
        return group;
    }

    /// <summary>The most recent geofence alerts from the capped alert log, newest first.</summary>
    private static async Task<Results<Ok<IReadOnlyList<GeofenceAlert>>, ValidationProblem>> GetRecentAsync(
        IAlertLog alerts,
        CancellationToken cancellationToken,
        int limit = 50)
    {
        if (limit is < 1 or > MaxLimit)
        {
            return TypedResults.ValidationProblem(new Dictionary<string, string[]> { ["limit"] = [$"must be between 1 and {MaxLimit}"] });
        }

        return TypedResults.Ok(await alerts.GetRecentAsync(limit, cancellationToken));
    }
}
