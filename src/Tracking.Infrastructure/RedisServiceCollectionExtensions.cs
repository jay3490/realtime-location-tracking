using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;
using Tracking.Core.Abstractions;
using Tracking.Infrastructure.Redis;

namespace Tracking.Infrastructure;

public static class RedisServiceCollectionExtensions
{
    /// <summary>
    /// Registers the shared Redis connection and every Redis-backed store. Services resolve only
    /// the abstractions they use, so registering all of them costs nothing.
    /// </summary>
    public static IServiceCollection AddRedisTracking(this IServiceCollection services, IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        services.AddOptions<RedisOptions>()
            .Bind(configuration.GetSection(RedisOptions.SectionName))
            .ValidateDataAnnotations()
            .Validate(o => o.ReconnectBaseDelay <= o.ReconnectMaxDelay, "ReconnectBaseDelay must not exceed ReconnectMaxDelay.")
            .ValidateOnStart();

        services.AddSingleton(sp => RedisConnection.Connect(
            sp.GetRequiredService<IOptions<RedisOptions>>().Value,
            sp.GetRequiredService<ILoggerFactory>().CreateLogger(typeof(RedisConnection).FullName!),
            sp.GetRequiredService<IMeterFactory>()));

        services.AddSingleton<RedisKeys>();
        services.AddSingleton<ILocationLog, RedisLocationLog>();
        services.AddSingleton<ILocationConsumerGroup, RedisLocationConsumerGroup>();
        services.AddSingleton<IAnalyticsOutbox, RedisAnalyticsOutbox>();
        services.AddSingleton<IDeviceStateStore, RedisDeviceStateStore>();
        services.AddSingleton<IGeofenceStore, RedisGeofenceStore>();
        services.AddSingleton<IAlertFeed, RedisAlertFeed>();
        services.AddSingleton<IAlertLog, RedisAlertLog>();

        return services;
    }

    /// <summary>Adds a readiness check that pings Redis.</summary>
    public static IHealthChecksBuilder AddRedisCheck(this IHealthChecksBuilder builder, params string[] tags) =>
        builder.AddCheck<RedisHealthCheck>("redis", HealthStatus.Unhealthy, tags);
}

internal sealed class RedisHealthCheck(IConnectionMultiplexer redis) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        if (!redis.IsConnected)
        {
            return HealthCheckResult.Unhealthy("Redis is disconnected; reconnecting in the background.");
        }

        try
        {
            var latency = await redis.GetDatabase().PingAsync().WaitAsync(TimeSpan.FromSeconds(2), cancellationToken).ConfigureAwait(false);
            return HealthCheckResult.Healthy($"PING {latency.TotalMilliseconds:F1} ms");
        }
        catch (Exception ex) when (ex is RedisException or TimeoutException)
        {
            return HealthCheckResult.Unhealthy("Redis is unreachable.", ex);
        }
    }
}
