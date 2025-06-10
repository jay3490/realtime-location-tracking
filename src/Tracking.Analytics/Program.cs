using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Tracking.Analytics;
using Tracking.Analytics.Data;
using Tracking.Infrastructure.Observability;
using Tracking.Kafka;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddAnalyticsData(builder.Configuration);
builder.Services.AddKafkaOptions(builder.Configuration);
builder.Services.AddOptions<AnalyticsConsumerOptions>()
    .Bind(builder.Configuration.GetSection(AnalyticsConsumerOptions.SectionName))
    .ValidateDataAnnotations()
    .ValidateOnStart();
builder.Services.AddOptions<AnalyticsServiceOptions>()
    .Bind(builder.Configuration.GetSection(AnalyticsServiceOptions.SectionName))
    .ValidateDataAnnotations()
    .Validate(o => o.ErrorDelay <= o.MaxErrorDelay, "ErrorDelay must not exceed MaxErrorDelay.")
    .ValidateOnStart();

// Order matters: hosted services start in registration order, so the schema is migrated before the
// consumer applies anything.
builder.Services.AddHostedService<DatabaseMigrator>();
builder.Services.AddSingleton<IAnalyticsEventSource, KafkaAnalyticsEventSource>();
builder.Services.AddSingleton<IAnalyticsEventSink, ScopedAggregatorSink>();
builder.Services.AddSingleton<AnalyticsMetrics>();
builder.Services.AddHostedService<AnalyticsIngestWorker>();
builder.Services.AddHostedService<LedgerPruner>();

builder.Services.AddTrackingTelemetry("tracking-analytics", TrackingMeters.Analytics);
builder.Services.AddHealthChecks().AddAnalyticsDatabaseCheck("ready");

var app = builder.Build();

// No public API: HTTP serves orchestrator probes and metrics only. Reports are served by Tracking.Api.
app.MapHealthChecks("/healthz/live", new HealthCheckOptions { Predicate = _ => false });
app.MapHealthChecks("/healthz/ready", new HealthCheckOptions { Predicate = check => check.Tags.Contains("ready") });
app.MapTrackingMetrics();

app.Run();
