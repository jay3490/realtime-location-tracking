using System.Net.Http.Json;
using Microsoft.Extensions.Logging;
using Tracking.Core;
using Tracking.Core.Geo;
using Tracking.Core.Geofencing;

namespace Tracking.Simulator;

/// <summary>
/// Creates (or replaces) a circle at the centre of the simulation area and a square to its north,
/// so simulated devices cross geofence boundaries and alerts flow end to end.
/// </summary>
public static partial class DemoGeofences
{
    private const int MaxAttempts = 15;
    private static readonly TimeSpan RetryDelay = TimeSpan.FromSeconds(2);

    public static IReadOnlyList<Geofence> For(GeoPoint center, double areaRadiusMeters)
    {
        var circle = new CircleGeofence("sim-center", "Simulation centre", center, Math.Round(areaRadiusMeters / 4));

        var squareCenter = GeoMath.Destination(center, 0, areaRadiusMeters / 2);
        var halfSide = areaRadiusMeters / 6;
        var north = GeoMath.Destination(squareCenter, 0, halfSide).Latitude;
        var south = GeoMath.Destination(squareCenter, 180, halfSide).Latitude;
        var east = GeoMath.Destination(squareCenter, 90, halfSide).Longitude;
        var west = GeoMath.Destination(squareCenter, 270, halfSide).Longitude;
        var square = new PolygonGeofence("sim-north", "Simulation north block",
        [
            new GeoPoint(north, west),
            new GeoPoint(north, east),
            new GeoPoint(south, east),
            new GeoPoint(south, west),
        ]);

        return [circle, square];
    }

    public static async Task EnsureAsync(Uri apiUrl, GeoPoint center, double areaRadiusMeters, ILogger logger, CancellationToken cancellationToken)
    {
        using var http = new HttpClient { BaseAddress = apiUrl, Timeout = TimeSpan.FromSeconds(10) };
        foreach (var geofence in For(center, areaRadiusMeters))
        {
            for (var attempt = 1; ; attempt++)
            {
                try
                {
                    using var response = await http.PutAsJsonAsync($"api/geofences/{geofence.Id}", geofence, TrackingJson.Options, cancellationToken)
                        .ConfigureAwait(false);
                    response.EnsureSuccessStatusCode();
                    LogGeofence(logger, geofence.Id, geofence.Name);
                    break;
                }
                catch (HttpRequestException ex) when (attempt < MaxAttempts)
                {
                    // The API may still be starting (for example when launched together with docker compose).
                    LogApiNotReady(logger, apiUrl, attempt, MaxAttempts, ex.Message);
                    await Task.Delay(RetryDelay, cancellationToken).ConfigureAwait(false);
                }
            }
        }
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "API at {ApiUrl} not ready (attempt {Attempt}/{MaxAttempts}): {Error}")]
    private static partial void LogApiNotReady(ILogger logger, Uri apiUrl, int attempt, int maxAttempts, string error);

    [LoggerMessage(Level = LogLevel.Information, Message = "Geofence {Id} ({Name}) is in place")]
    private static partial void LogGeofence(ILogger logger, string id, string name);
}
