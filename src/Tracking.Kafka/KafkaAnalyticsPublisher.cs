using System.Diagnostics.Metrics;
using System.Text;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Tracking.Core.Abstractions;
using Tracking.Infrastructure.Observability;

namespace Tracking.Kafka;

/// <summary>Kafka message headers set on every analytics event.</summary>
public static class AnalyticsHeaders
{
    public const string EventId = "event-id";
    public const string EventKind = "event-kind";
}

/// <summary>
/// Publishes analytics events with one long-lived idempotent producer. Messages are keyed by device
/// id, so each device's events land on one partition, in the order they were handed over.
/// </summary>
public sealed partial class KafkaAnalyticsPublisher : IAnalyticsPublisher, IDisposable
{
    private static readonly TimeSpan FlushTimeout = TimeSpan.FromSeconds(10);

    private readonly IProducer<string, string> _producer;
    private readonly KafkaOptions _options;
    private readonly ILogger<KafkaAnalyticsPublisher> _logger;
    private readonly Counter<long> _produced;
    private readonly Counter<long> _errors;

    public KafkaAnalyticsPublisher(IOptions<KafkaOptions> options, IMeterFactory meterFactory, ILogger<KafkaAnalyticsPublisher> logger)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(meterFactory);

        _options = options.Value;
        _logger = logger;

        var meter = meterFactory.Create(TrackingMeters.Processor);
        _produced = meter.CreateCounter<long>(
            "tracking.kafka.produced",
            unit: "{message}",
            description: "Analytics events acknowledged by Kafka, by topic.");
        _errors = meter.CreateCounter<long>(
            "tracking.kafka.produce.errors",
            unit: "{message}",
            description: "Analytics events Kafka did not acknowledge, by topic and error code.");

        _producer = new ProducerBuilder<string, string>(_options.CreateProducerConfig())
            .SetErrorHandler((_, error) => LogClientError(error.Code, error.Reason, error.IsFatal))
            .SetLogHandler((_, message) => LogClientMessage(message.Level, message.Facility, message.Message))
            .Build();
    }

    public async Task<IReadOnlyList<bool>> PublishAsync(IReadOnlyList<OutboundEvent> events, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(events);

        var deliveries = new Task<DeliveryResult<string, string>>[events.Count];
        for (var i = 0; i < events.Count; i++)
        {
            var outbound = events[i];
            var message = new Message<string, string>
            {
                Key = outbound.Key,
                Value = outbound.Payload,
                Headers =
                [
                    new Header(AnalyticsHeaders.EventId, Encoding.UTF8.GetBytes(outbound.EventId)),
                    new Header(AnalyticsHeaders.EventKind, Encoding.UTF8.GetBytes(outbound.Kind.ToString())),
                ],
            };

            // ProduceAsync enqueues synchronously, so calling it in a loop keeps the events in order.
            deliveries[i] = _producer.ProduceAsync(_options.TopicFor(outbound.Kind), message, cancellationToken);
        }

        try
        {
            await Task.WhenAll(deliveries).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is ProduceException<string, string> or KafkaException or OperationCanceledException)
        {
            // Reported per event below.
        }

        var results = new bool[events.Count];
        for (var i = 0; i < events.Count; i++)
        {
            var topic = _options.TopicFor(events[i].Kind);
            if (deliveries[i].IsCompletedSuccessfully)
            {
                results[i] = true;
                _produced.Add(1, new KeyValuePair<string, object?>("topic", topic));
                continue;
            }

            var code = deliveries[i].Exception?.InnerException is KafkaException kafka ? kafka.Error.Code.ToString() : "Cancelled";
            _errors.Add(1, [new("topic", topic), new("error", code)]);
            LogProduceFailed(events[i].EventId, topic, code);
        }

        return results;
    }

    public void Dispose()
    {
        // Give in-flight messages a chance to be acknowledged before the producer goes away.
        _producer.Flush(FlushTimeout);
        _producer.Dispose();
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "Kafka did not acknowledge event {EventId} on {Topic}: {Error}")]
    private partial void LogProduceFailed(string eventId, string topic, string error);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Kafka client error {Code}: {Reason} (fatal: {IsFatal})")]
    private partial void LogClientError(ErrorCode code, string reason, bool isFatal);

    [LoggerMessage(Level = LogLevel.Debug, Message = "librdkafka {Level} {Facility}: {Message}")]
    private partial void LogClientMessage(SyslogLevel level, string facility, string message);
}
