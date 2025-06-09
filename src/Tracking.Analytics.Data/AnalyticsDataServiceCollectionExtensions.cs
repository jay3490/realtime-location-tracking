using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Tracking.Analytics.Data;

public static class AnalyticsDataServiceCollectionExtensions
{
    public const string ConnectionStringName = "Analytics";

    /// <summary>
    /// Registers the analytics database (PostgreSQL, connection string "Analytics"), the aggregator and
    /// the report queries.
    /// </summary>
    public static IServiceCollection AddAnalyticsData(this IServiceCollection services, IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        var connectionString = configuration.GetConnectionString(ConnectionStringName)
            ?? throw new InvalidOperationException($"ConnectionStrings:{ConnectionStringName} is required.");

        services.AddDbContext<AnalyticsDbContext>(options => options.UseNpgsql(connectionString));
        services.AddOptions<AnalyticsDataOptions>()
            .Bind(configuration.GetSection(AnalyticsDataOptions.SectionName))
            .ValidateDataAnnotations()
            .ValidateOnStart();
        services.AddScoped<AnalyticsAggregator>();
        services.AddScoped<AnalyticsReports>();
        return services;
    }

    /// <summary>Adds a readiness check that opens a connection to the analytics database.</summary>
    public static IHealthChecksBuilder AddAnalyticsDatabaseCheck(this IHealthChecksBuilder builder, params string[] tags) =>
        builder.AddCheck<AnalyticsDatabaseHealthCheck>("analytics-db", HealthStatus.Unhealthy, tags);
}

internal sealed class AnalyticsDatabaseHealthCheck(AnalyticsDbContext db) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default) =>
        await db.Database.CanConnectAsync(cancellationToken).ConfigureAwait(false)
            ? HealthCheckResult.Healthy()
            : HealthCheckResult.Unhealthy("The analytics database is unreachable.");
}
