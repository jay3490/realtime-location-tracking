using System.ComponentModel.DataAnnotations;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using Tracking.Analytics.Data;

namespace Tracking.Analytics;

public sealed class AnalyticsServiceOptions
{
    public const string SectionName = "AnalyticsService";

    /// <summary>
    /// Apply pending EF Core migrations at startup. Turn it off where migrations run as a separate
    /// deployment step (for example a Kubernetes Job with <c>dotnet ef database update</c> or a bundle).
    /// </summary>
    public bool ApplyMigrations { get; set; } = true;

    /// <summary>How long event ids stay in the idempotency ledger; replays older than this are not detected.</summary>
    [Range(typeof(TimeSpan), "01:00:00", "365.00:00:00")]
    public TimeSpan LedgerRetention { get; set; } = TimeSpan.FromDays(7);

    [Range(typeof(TimeSpan), "00:01:00", "1.00:00:00")]
    public TimeSpan LedgerPruneInterval { get; set; } = TimeSpan.FromHours(1);

    /// <summary>First retry delay after a database or Kafka error; doubles up to <see cref="MaxErrorDelay"/>.</summary>
    [Range(typeof(TimeSpan), "00:00:00.010", "00:05:00")]
    public TimeSpan ErrorDelay { get; set; } = TimeSpan.FromSeconds(1);

    [Range(typeof(TimeSpan), "00:00:00.010", "00:10:00")]
    public TimeSpan MaxErrorDelay { get; set; } = TimeSpan.FromSeconds(30);
}

/// <summary>
/// Brings the schema up to date before the consumer starts (it is registered first, and hosted
/// services start in order), waiting for the database if it is not reachable yet.
/// </summary>
public sealed partial class DatabaseMigrator(
    IServiceScopeFactory scopes,
    IOptions<AnalyticsServiceOptions> options,
    TimeProvider clock,
    ILogger<DatabaseMigrator> logger) : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (!options.Value.ApplyMigrations)
        {
            return;
        }

        var delay = options.Value.ErrorDelay;
        while (true)
        {
            try
            {
                await using var scope = scopes.CreateAsyncScope();
                var db = scope.ServiceProvider.GetRequiredService<AnalyticsDbContext>();
                var pending = (await db.Database.GetPendingMigrationsAsync(cancellationToken).ConfigureAwait(false)).ToArray();
                await db.Database.MigrateAsync(cancellationToken).ConfigureAwait(false);
                LogMigrated(pending.Length);
                return;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                LogMigrationFailed(ex, delay);
                await Task.Delay(delay, clock, cancellationToken).ConfigureAwait(false);
                delay = TimeSpan.FromTicks(Math.Min(delay.Ticks * 2, options.Value.MaxErrorDelay.Ticks));
            }
        }
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    [LoggerMessage(Level = LogLevel.Information, Message = "Analytics schema is up to date ({Count} migrations applied)")]
    private partial void LogMigrated(int count);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Could not migrate the analytics database; retrying in {Delay}")]
    private partial void LogMigrationFailed(Exception exception, TimeSpan delay);
}

/// <summary>Deletes idempotency ledger entries older than the retention period, periodically.</summary>
public sealed partial class LedgerPruner(
    IServiceScopeFactory scopes,
    IOptions<AnalyticsServiceOptions> options,
    TimeProvider clock,
    ILogger<LedgerPruner> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(options.Value.LedgerPruneInterval, clock);
        while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false))
        {
            try
            {
                await using var scope = scopes.CreateAsyncScope();
                var pruned = await scope.ServiceProvider.GetRequiredService<AnalyticsAggregator>()
                    .PruneLedgerAsync(options.Value.LedgerRetention, stoppingToken)
                    .ConfigureAwait(false);
                LogPruned(pruned);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                LogPruneFailed(ex);
            }
        }
    }

    [LoggerMessage(Level = LogLevel.Debug, Message = "Pruned {Count} idempotency ledger entries")]
    private partial void LogPruned(int count);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Could not prune the idempotency ledger")]
    private partial void LogPruneFailed(Exception exception);
}
