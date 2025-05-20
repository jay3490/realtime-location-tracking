using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;

namespace Tracking.Infrastructure.Observability;

/// <summary>Names of the meters the services publish their own instruments on.</summary>
public static class TrackingMeters
{
    public const string Ingest = "Tracking.Ingest";
    public const string Processor = "Tracking.Processor";
    public const string Analytics = "Tracking.Analytics";
    public const string Redis = "Tracking.Redis";

    /// <summary>The Prometheus scrape path every service exposes.</summary>
    public const string ScrapePath = "/metrics";
}

public static class TelemetryServiceCollectionExtensions
{
    /// <summary>
    /// Wires OpenTelemetry metrics for one service: its own meters, ASP.NET Core and runtime
    /// instrumentation, and a Prometheus exporter served by <see cref="MapTrackingMetrics"/>.
    /// </summary>
    public static IServiceCollection AddTrackingTelemetry(this IServiceCollection services, string serviceName, params string[] meterNames)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(serviceName);
        ArgumentNullException.ThrowIfNull(meterNames);

        services.AddOpenTelemetry()
            .ConfigureResource(resource => resource.AddService(serviceName))
            .WithMetrics(metrics => metrics
                .AddMeter([.. meterNames, TrackingMeters.Redis])
                .AddAspNetCoreInstrumentation()
                .AddRuntimeInstrumentation()
                .AddView(TrackingInstruments.LatencyHistogramPattern, new ExplicitBucketHistogramConfiguration
                {
                    Boundaries = TrackingInstruments.LatencyBucketsSeconds,
                })
                .AddPrometheusExporter());

        return services;
    }

    /// <summary>Maps the Prometheus scrape endpoint at <see cref="TrackingMeters.ScrapePath"/>.</summary>
    public static IEndpointConventionBuilder MapTrackingMetrics(this IEndpointRouteBuilder endpoints) =>
        endpoints.MapPrometheusScrapingEndpoint(TrackingMeters.ScrapePath);
}

/// <summary>Conventions shared by the instruments of every service.</summary>
public static class TrackingInstruments
{
    /// <summary>Latency histograms are named <c>*.latency</c> and recorded in seconds.</summary>
    public const string LatencyHistogramPattern = "tracking.*.latency";

    /// <summary>
    /// Buckets from 1 ms to 30 s. The default OpenTelemetry buckets are tuned for milliseconds and
    /// would put every sub-second pipeline latency into the first bucket.
    /// </summary>
    public static readonly double[] LatencyBucketsSeconds =
        [0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30];
}
