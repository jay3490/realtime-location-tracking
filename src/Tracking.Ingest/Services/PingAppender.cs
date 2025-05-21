using System.Diagnostics;
using System.Threading.Channels;
using Microsoft.Extensions.Options;
using StackExchange.Redis;
using Tracking.Core.Abstractions;
using Tracking.Core.Devices;

namespace Tracking.Ingest.Services;

/// <summary>
/// Funnels the pings of every stream on this instance into pipelined batch appends.
/// </summary>
/// <remarks>
/// <para>
/// Streams queue pings on one bounded channel; a single flush loop takes whatever has accumulated
/// (up to <see cref="IngestOptions.MaxBatchSize"/>) and appends it with one round trip. While a batch
/// is in flight the next one builds up, so batches grow with load without any timer-based delay.
/// </para>
/// <para>
/// There is exactly one flush loop, so the pings of one stream reach the log in the order they were
/// queued. When the log slows down the queue fills, <see cref="AppendAsync"/> stops returning, the
/// stream stops reading from the device, and HTTP/2 flow control pushes back to the device itself.
/// </para>
/// </remarks>
public sealed partial class PingAppender : BackgroundService
{
    private readonly ILocationLog _log;
    private readonly IngestMetrics _metrics;
    private readonly ILogger<PingAppender> _logger;
    private readonly int _maxBatchSize;
    private readonly Channel<PendingAppend> _queue;

    public PingAppender(ILocationLog log, IngestMetrics metrics, IOptions<IngestOptions> options, ILogger<PingAppender> logger)
    {
        ArgumentNullException.ThrowIfNull(options);
        _log = log;
        _metrics = metrics;
        _logger = logger;
        _maxBatchSize = options.Value.MaxBatchSize;
        _queue = Channel.CreateBounded<PendingAppend>(new BoundedChannelOptions(options.Value.AppendQueueCapacity)
        {
            SingleReader = true,
            FullMode = BoundedChannelFullMode.Wait,
        });
    }

    /// <summary>Pings queued and not yet handed to the log.</summary>
    public int Queued => _queue.Reader.Count;

    /// <summary>
    /// Queues <paramref name="pings"/> and waits until each has been appended. Returns one entry id
    /// per ping, or null for a ping whose append failed and may be retried by the device.
    /// </summary>
    public async Task<IReadOnlyList<string?>> AppendAsync(IReadOnlyList<LocationPing> pings, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(pings);

        var results = new Task<string?>[pings.Count];
        for (var i = 0; i < pings.Count; i++)
        {
            var pending = new PendingAppend(pings[i]);
            try
            {
                await _queue.Writer.WriteAsync(pending, cancellationToken).ConfigureAwait(false);
                results[i] = pending.Result.Task;
            }
            catch (ChannelClosedException)
            {
                // The instance is shutting down; the device will resend this ping elsewhere.
                results[i] = Task.FromResult<string?>(null);
            }
        }

        // Once queued, a ping is appended even if this caller goes away; only the wait is cancellable.
        return await Task.WhenAll(results).WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var batch = new List<PendingAppend>(_maxBatchSize);
        var pings = new List<LocationPing>(_maxBatchSize);

        // Not cancelled by stoppingToken: the loop ends when the queue is completed and empty, so
        // pings already queued at shutdown are still appended and acknowledged.
        while (await _queue.Reader.WaitToReadAsync(CancellationToken.None).ConfigureAwait(false))
        {
            batch.Clear();
            pings.Clear();
            while (batch.Count < _maxBatchSize && _queue.Reader.TryRead(out var pending))
            {
                batch.Add(pending);
                pings.Add(pending.Ping);
            }

            await FlushAsync(batch, pings).ConfigureAwait(false);
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _queue.Writer.TryComplete();
        await base.StopAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task FlushAsync(List<PendingAppend> batch, List<LocationPing> pings)
    {
        var started = Stopwatch.GetTimestamp();
        IReadOnlyList<string?> ids;
        try
        {
            ids = await _log.AppendBatchAsync(pings, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is RedisException or TimeoutException)
        {
            LogBatchFailed(ex, batch.Count);
            ids = new string?[batch.Count];
        }

        _metrics.BatchAppended(batch.Count, Stopwatch.GetElapsedTime(started));
        for (var i = 0; i < batch.Count; i++)
        {
            batch[i].Result.TrySetResult(i < ids.Count ? ids[i] : null);
        }
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "Could not append a batch of {Count} pings")]
    private partial void LogBatchFailed(Exception exception, int count);

    private sealed class PendingAppend(LocationPing ping)
    {
        public LocationPing Ping { get; } = ping;

        public TaskCompletionSource<string?> Result { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }
}
