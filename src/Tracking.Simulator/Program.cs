using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Tracking.Simulator;

// Short switches for the common settings; everything else binds from "--Simulator:<Name>" arguments
// or "Simulator__<Name>" environment variables.
var switches = new Dictionary<string, string>
{
    ["--ingest"] = "Simulator:IngestUrl",
    ["--api"] = "Simulator:ApiUrl",
    ["--devices"] = "Simulator:Devices",
    ["--interval"] = "Simulator:Interval",
    ["--duration"] = "Simulator:Duration",
    ["--seed"] = "Simulator:Seed",
};

var builder = Host.CreateApplicationBuilder();
builder.Configuration.AddCommandLine(args, switches);
builder.Services.AddOptions<SimulatorOptions>()
    .Bind(builder.Configuration.GetSection(SimulatorOptions.SectionName))
    .ValidateDataAnnotations()
    .Validate(o => o.MinSpeedMetersPerSecond <= o.MaxSpeedMetersPerSecond, "MinSpeedMetersPerSecond must not exceed MaxSpeedMetersPerSecond.")
    .ValidateOnStart();
builder.Services.AddSingleton<SimulatorStats>();
builder.Services.AddHostedService<SimulatorRunner>();

await builder.Build().RunAsync();
