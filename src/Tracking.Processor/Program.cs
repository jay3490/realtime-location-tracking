using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Tracking.Infrastructure;
using Tracking.Infrastructure.Observability;
using Tracking.Kafka;
using Tracking.Processor;
using Tracking.Processor.Processing;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddRedisTracking(builder.Configuration);
builder.Services.AddOptions<ProcessorOptions>()
    .Bind(builder.Configuration.GetSection(ProcessorOptions.SectionName))
    .ValidateDataAnnotations()
    .Validate(o => o.MinIdleDelay <= o.MaxIdleDelay, "MinIdleDelay must not exceed MaxIdleDelay.")
    .Validate(o => o.ErrorDelay <= o.MaxErrorDelay, "ErrorDelay must not exceed MaxErrorDelay.")
    .ValidateOnStart();

builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddSingleton<GeofenceCache>();
builder.Services.AddSingleton<PingProcessor>();
builder.Services.AddSingleton<ConsumerHeartbeat>();
builder.Services.AddSingleton<ProcessorMetrics>();
builder.Services.AddHostedService<StreamLagMonitor>();
builder.Services.AddTrackingTelemetry("tracking-processor", TrackingMeters.Processor);
builder.Services.AddSingleton<PingConsumer>();
builder.Services.AddHostedService<ProcessorWorker>();

// The analytics path (outbox -> Kafka) is optional; without it the processor needs no Kafka at all.
if (builder.Configuration.GetValue("Processor:PublishAnalytics", defaultValue: true))
{
    builder.Services.AddKafkaAnalyticsPublisher(builder.Configuration);
    builder.Services.AddHostedService<AnalyticsRelay>();
}

builder.Services.AddHealthChecks()
    .AddCheck<ConsumerHeartbeatHealthCheck>("consumer-loop", tags: ["live"])
    .AddRedisCheck("ready");

var app = builder.Build();

// The processor has no public API; HTTP exists only for orchestrator probes and metrics scraping.
app.MapHealthChecks("/healthz/live", new HealthCheckOptions { Predicate = check => check.Tags.Contains("live") });
app.MapHealthChecks("/healthz/ready", new HealthCheckOptions { Predicate = check => check.Tags.Contains("ready") });
app.MapTrackingMetrics();

app.Run();
