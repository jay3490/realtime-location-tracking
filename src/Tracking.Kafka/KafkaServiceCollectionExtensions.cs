using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Tracking.Core.Abstractions;

namespace Tracking.Kafka;

public static class KafkaServiceCollectionExtensions
{
    /// <summary>Binds and validates <see cref="KafkaOptions"/> from the "Kafka" section.</summary>
    public static IServiceCollection AddKafkaOptions(this IServiceCollection services, IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        services.AddOptions<KafkaOptions>()
            .Bind(configuration.GetSection(KafkaOptions.SectionName))
            .ValidateDataAnnotations()
            .Validate(o => !o.EnableIdempotence || o.Acks == Acks.All, "The idempotent producer requires Kafka:Acks to be All.")
            .Validate(o => o.PositionsTopic != o.GeofenceEventsTopic, "Positions and geofence events need separate topics.")
            .ValidateOnStart();
        return services;
    }

    /// <summary>Registers the idempotent Kafka producer as the analytics publisher.</summary>
    public static IServiceCollection AddKafkaAnalyticsPublisher(this IServiceCollection services, IConfiguration configuration)
    {
        services.AddKafkaOptions(configuration);
        services.AddSingleton<KafkaAnalyticsPublisher>();
        services.AddSingleton<IAnalyticsPublisher>(sp => sp.GetRequiredService<KafkaAnalyticsPublisher>());
        return services;
    }
}
