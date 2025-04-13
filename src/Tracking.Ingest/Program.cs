using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Tracking.Infrastructure;
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
builder.Services.AddHostedService<AlertRelay>();

builder.Services.AddHealthChecks().AddRedisCheck("ready");

var app = builder.Build();

app.MapGrpcService<LocationIngestService>();
app.MapHealthChecks("/healthz/live", new HealthCheckOptions { Predicate = _ => false });
app.MapHealthChecks("/healthz/ready", new HealthCheckOptions { Predicate = check => check.Tags.Contains("ready") });

app.Run();

/// <summary>Entry point, exposed for integration tests.</summary>
public partial class Program;
