using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Tracking.Api.Endpoints;
using Tracking.Core;
using Tracking.Infrastructure;

var builder = WebApplication.CreateBuilder(args);

builder.Services.ConfigureHttpJsonOptions(options => TrackingJson.Configure(options.SerializerOptions));
builder.Services.AddProblemDetails();
builder.Services.AddRedisTracking(builder.Configuration);
builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddHealthChecks().AddRedisCheck("ready");

var app = builder.Build();

app.UseExceptionHandler();
app.UseStatusCodePages();

app.MapDeviceEndpoints();
app.MapGeofenceEndpoints();
app.MapAlertEndpoints();
app.MapHealthChecks("/healthz/live", new HealthCheckOptions { Predicate = _ => false });
app.MapHealthChecks("/healthz/ready", new HealthCheckOptions { Predicate = check => check.Tags.Contains("ready") });

app.Run();

/// <summary>Entry point, exposed for integration tests.</summary>
public partial class Program;
