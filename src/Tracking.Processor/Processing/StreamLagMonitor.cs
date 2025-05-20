using Microsoft.Extensions.Options;
using StackExchange.Redis;
using Tracking.Core.Abstractions;

namespace Tracking.Processor.Processing;

/// <summary>
/// Samples the consumer group's backlog on a timer so the lag gauges can report it without doing
/// I/O inside a metrics callback.
/// </summary>
public sealed partial class StreamLagMonitor(
    ILocationConsumerGroup group,
    ProcessorMetrics metrics,
    TimeProvider clock,
    IOptions<ProcessorOptions> options,
    ILogger<StreamLagMonitor> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(options.Value.LagSampleInterval, clock);
        do
        {
            await SampleAsync(stoppingToken).ConfigureAwait(false);
        }
        while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false));
    }

    public async Task SampleAsync(CancellationToken cancellationToken)
    {
        try
        {
            metrics.ObserveStatus(await group.GetStatusAsync(cancellationToken).ConfigureAwait(false));
        }
        catch (Exception ex) when (ex is RedisException or TimeoutException)
        {
            // Keep reporting the last known values; the readiness check covers the outage itself.
            LogSampleFailed(ex);
        }
    }

    [LoggerMessage(Level = LogLevel.Debug, Message = "Could not sample the consumer group backlog")]
    private partial void LogSampleFailed(Exception exception);
}
