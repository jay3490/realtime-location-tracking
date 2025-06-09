using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Tracking.Core.Abstractions;
using Tracking.Core.Analytics;
using Tracking.Kafka;
using Tracking.Testing;

namespace Tracking.Processor.Tests;

public sealed class KafkaPublisherTests
{
    [Fact]
    public void The_producer_is_idempotent_keyed_and_waits_for_all_replicas_by_default()
    {
        var options = new KafkaOptions { BootstrapServers = "kafka-0:9092,kafka-1:9092", ClientId = "processor-0" };
        options.Properties["security.protocol"] = "SASL_SSL";

        var config = options.CreateProducerConfig();

        Assert.Equal("kafka-0:9092,kafka-1:9092", config.BootstrapServers);
        Assert.True(config.EnableIdempotence);
        Assert.Equal(Acks.All, config.Acks);
        Assert.Equal(Partitioner.Murmur2Random, config.Partitioner);
        Assert.Equal(CompressionType.Lz4, config.CompressionType);
        Assert.Equal(30_000, config.MessageTimeoutMs);
        Assert.Equal("processor-0", config.ClientId);
        Assert.Equal("SASL_SSL", config.Get("security.protocol"));
    }

    [Theory]
    [InlineData(AnalyticsEventKind.Position, "tracking.positions.v1")]
    [InlineData(AnalyticsEventKind.Geofence, "tracking.geofence-events.v1")]
    public void Each_event_kind_has_its_own_topic(AnalyticsEventKind kind, string topic)
    {
        Assert.Equal(topic, new KafkaOptions().TopicFor(kind));
    }

    [Theory]
    [InlineData("Leader", "true", false)]
    [InlineData("Leader", "false", true)]
    [InlineData("All", "true", true)]
    public void Weaker_acks_are_only_accepted_without_idempotence(string acks, string idempotence, bool valid)
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Kafka:Acks"] = acks,
                ["Kafka:EnableIdempotence"] = idempotence,
            })
            .Build();
        using var services = new ServiceCollection().AddKafkaOptions(configuration).BuildServiceProvider();

        var resolve = () => services.GetRequiredService<IOptions<KafkaOptions>>().Value;

        if (valid)
        {
            Assert.NotNull(resolve());
        }
        else
        {
            Assert.Throws<OptionsValidationException>(resolve);
        }
    }

    [Fact]
    public async Task Unreachable_brokers_report_each_event_as_not_acknowledged_and_count_the_errors()
    {
        using var meters = new TestMeterFactory();
        using var recorder = new MetricRecorder(meters);
        var options = Options.Create(new KafkaOptions
        {
            BootstrapServers = "127.0.0.1:1",
            DeliveryTimeout = TimeSpan.FromSeconds(1),
            LingerMs = 0,
        });
        using var publisher = new KafkaAnalyticsPublisher(options, meters, NullLogger<KafkaAnalyticsPublisher>.Instance);

        var results = await publisher.PublishAsync(
        [
            new OutboundEvent("1-0", AnalyticsEventKind.Position, "van", "van/1", "{}"),
            new OutboundEvent("2-0", AnalyticsEventKind.Geofence, "van", "van/dam/enter/1", "{}"),
        ]).WaitAsync(TimeSpan.FromSeconds(30));

        Assert.Equal([false, false], results);
        Assert.Equal(1, recorder.Sum("tracking.kafka.produce.errors", "topic", "tracking.positions.v1"));
        Assert.Equal(1, recorder.Sum("tracking.kafka.produce.errors", "topic", "tracking.geofence-events.v1"));
    }
}
