using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Tracking.Infrastructure;
using Tracking.Processor;
using Tracking.Processor.Processing;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddRedisTracking(builder.Configuration);
builder.Services.AddOptions<ProcessorOptions>()
    .Bind(builder.Configuration.GetSection(ProcessorOptions.SectionName))
    .ValidateDataAnnotations()
    .Validate(o => o.MinIdleDelay <= o.MaxIdleDelay, "MinIdleDelay must not exceed MaxIdleDelay.")
    .ValidateOnStart();

builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddSingleton<GeofenceCache>();
builder.Services.AddSingleton<PingProcessor>();
builder.Services.AddSingleton<ConsumerHeartbeat>();
builder.Services.AddSingleton<PingConsumer>();
builder.Services.AddHostedService<ProcessorWorker>();

builder.Services.AddHealthChecks()
    .AddCheck<ConsumerHeartbeatHealthCheck>("consumer-loop", tags: ["live"])
    .AddRedisCheck("ready");

var app = builder.Build();

// The processor has no public API; HTTP exists only for orchestrator probes.
app.MapHealthChecks("/healthz/live", new HealthCheckOptions { Predicate = check => check.Tags.Contains("live") });
app.MapHealthChecks("/healthz/ready", new HealthCheckOptions { Predicate = check => check.Tags.Contains("ready") });

app.Run();
