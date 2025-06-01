using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using Tracking.Infrastructure;
using Tracking.Infrastructure.Observability;
using Tracking.Ingest;
using Tracking.Ingest.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddGrpc(options => options.EnableDetailedErrors = builder.Environment.IsDevelopment());
builder.Services.AddRedisTracking(builder.Configuration);
builder.Services.AddOptions<IngestOptions>()
    .Bind(builder.Configuration.GetSection(IngestOptions.SectionName))
    .ValidateDataAnnotations()
    .ValidateOnStart();

builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddSingleton<PingValidator>();
builder.Services.AddSingleton<DeviceSessionRegistry>();
builder.Services.AddSingleton<IngestMetrics>();
builder.Services.AddSingleton<StreamDrain>();
builder.Services.AddSingleton<PingAppender>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<PingAppender>());
builder.Services.AddTrackingTelemetry("tracking-ingest", TrackingMeters.Ingest);
builder.Services.AddHostedService<AlertRelay>();

// The host gives hosted services this long to stop; the appender spends it waiting for streams to drain.
builder.Services.AddOptions<HostOptions>()
    .Configure<IOptions<IngestOptions>>((host, ingest) => host.ShutdownTimeout = ingest.Value.DrainTimeout);

builder.Services.AddHealthChecks()
    .AddRedisCheck("ready")
    .AddCheck<StreamDrainHealthCheck>("drain", tags: ["ready"]);

var app = builder.Build();

// SIGTERM (or Ctrl+C) starts draining immediately, before any hosted service is asked to stop.
var drain = app.Services.GetRequiredService<StreamDrain>();
app.Lifetime.ApplicationStopping.Register(drain.Begin);

app.MapGrpcService<LocationIngestService>();
app.MapHealthChecks("/healthz/live", new HealthCheckOptions { Predicate = _ => false });
app.MapHealthChecks("/healthz/ready", new HealthCheckOptions { Predicate = check => check.Tags.Contains("ready") });

// Served on every endpoint, but only the HTTP/1.1 health port is reachable by a Prometheus scraper.
app.MapTrackingMetrics();

app.Run();

/// <summary>Entry point, exposed for integration tests.</summary>
public partial class Program;
