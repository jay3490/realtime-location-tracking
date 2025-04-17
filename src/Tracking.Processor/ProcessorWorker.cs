using Tracking.Processor.Processing;

namespace Tracking.Processor;

public sealed class ProcessorWorker(PingConsumer consumer) : BackgroundService
{
    protected override Task ExecuteAsync(CancellationToken stoppingToken) => consumer.RunAsync(stoppingToken);
}
