using System.Diagnostics;
using Grpc.Net.Client;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Tracking.Contracts.V1;
using Tracking.Core.Geo;

namespace Tracking.Simulator;

/// <summary>
/// Creates the simulated devices, streams them until the configured duration elapses or the
/// process is asked to stop, then stops the host.
/// </summary>
public sealed partial class SimulatorRunner(
    IOptions<SimulatorOptions> options,
    SimulatorStats stats,
    IHostApplicationLifetime lifetime,
    ILoggerFactory loggerFactory) : BackgroundService
{
    private static readonly TimeSpan ReportInterval = TimeSpan.FromSeconds(10);

    private readonly SimulatorOptions _options = options.Value;
    private readonly ILogger<SimulatorRunner> _logger = loggerFactory.CreateLogger<SimulatorRunner>();

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            await RunAsync(stoppingToken).ConfigureAwait(false);
        }
        finally
        {
            lifetime.StopApplication();
        }
    }

    private async Task RunAsync(CancellationToken stoppingToken)
    {
        var center = new GeoPoint(_options.CenterLatitude, _options.CenterLongitude);
        if (_options.ApiUrl is not null)
        {
            await DemoGeofences.EnsureAsync(_options.ApiUrl, center, _options.AreaRadiusMeters, _logger, stoppingToken).ConfigureAwait(false);
        }

        // The duration covers streaming only, not geofence setup.
        using var run = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        if (_options.Duration > TimeSpan.Zero)
        {
            run.CancelAfter(_options.Duration);
        }

        var cancellationToken = run.Token;

        using var channel = GrpcChannel.ForAddress(_options.IngestUrl, new GrpcChannelOptions
        {
            // A single HTTP/2 connection is limited to the server's concurrent stream cap (100 by default
            // in Kestrel), so let the client open more connections when simulating many devices.
            HttpHandler = new SocketsHttpHandler { EnableMultipleHttp2Connections = true },
        });
        var client = new LocationIngest.LocationIngestClient(channel);
        var random = _options.Seed is { } seed ? new Random(seed) : new Random();
        var speedRange = _options.MaxSpeedMetersPerSecond - _options.MinSpeedMetersPerSecond;
        var streamerLogger = loggerFactory.CreateLogger<DeviceStreamer>();

        var streamers = Enumerable.Range(1, _options.Devices)
            .Select(i => new SimulatedDevice(
                $"{_options.DevicePrefix}-{i:D4}",
                center,
                _options.AreaRadiusMeters,
                _options.MinSpeedMetersPerSecond + (random.NextDouble() * speedRange),
                new Random(random.Next())))
            .Select(device => new DeviceStreamer(client, device, _options.Interval, stats, streamerLogger))
            .ToArray();

        LogStarting(streamers.Length, _options.IngestUrl, _options.Interval, _options.Duration);
        var stopwatch = Stopwatch.StartNew();
        var reporter = ReportAsync(cancellationToken);

        await Task.WhenAll(streamers.Select(s => s.RunAsync(cancellationToken))).ConfigureAwait(false);
        await reporter.ConfigureAwait(false);

        LogFinished(stopwatch.Elapsed.TotalSeconds, stats.ToString());
    }

    private async Task ReportAsync(CancellationToken cancellationToken)
    {
        using var timer = new PeriodicTimer(ReportInterval);
        try
        {
            while (await timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false))
            {
                LogProgress(stats.ToString());
            }
        }
        catch (OperationCanceledException)
        {
            // Run is over.
        }
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Streaming {Devices} devices to {Ingest} every {Interval} (duration {Duration}; 00:00:00 means until stopped)")]
    private partial void LogStarting(int devices, Uri ingest, TimeSpan interval, TimeSpan duration);

    [LoggerMessage(Level = LogLevel.Information, Message = "{Stats}")]
    private partial void LogProgress(string stats);

    [LoggerMessage(Level = LogLevel.Information, Message = "Finished after {Seconds:F1}s: {Stats}")]
    private partial void LogFinished(double seconds, string stats);
}
