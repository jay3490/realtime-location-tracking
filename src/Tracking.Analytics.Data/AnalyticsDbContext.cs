using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace Tracking.Analytics.Data;

public sealed class AnalyticsDbContext(DbContextOptions<AnalyticsDbContext> options) : DbContext(options)
{
    public DbSet<ProcessedEvent> ProcessedEvents => Set<ProcessedEvent>();

    public DbSet<DeviceDailyDistance> DailyDistances => Set<DeviceDailyDistance>();

    public DbSet<GeofenceVisit> GeofenceVisits => Set<GeofenceVisit>();

    public DbSet<DwellEvent> DwellEvents => Set<DwellEvent>();

    protected override void ConfigureConventions(ModelConfigurationBuilder configurationBuilder)
    {
        ArgumentNullException.ThrowIfNull(configurationBuilder);

        // Every timestamp is UTC. PostgreSQL's timestamptz requires DateTimeKind.Utc on write, and values
        // read back are marked UTC whichever provider stored them.
        configurationBuilder.Properties<DateTime>().HaveConversion<UtcDateTimeConverter>();
        configurationBuilder.Properties<DateTime?>().HaveConversion<UtcDateTimeConverter>();
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        ArgumentNullException.ThrowIfNull(modelBuilder);

        modelBuilder.Entity<ProcessedEvent>(e =>
        {
            e.ToTable("processed_events");
            e.HasKey(x => x.EventId);
            e.Property(x => x.EventId).HasMaxLength(200);
            e.HasIndex(x => x.ProcessedAt);
        });

        modelBuilder.Entity<DeviceDailyDistance>(e =>
        {
            e.ToTable("device_daily_distance");
            e.HasKey(x => new { x.DeviceId, x.Day });
            e.Property(x => x.DeviceId).HasMaxLength(64);
        });

        modelBuilder.Entity<GeofenceVisit>(e =>
        {
            e.ToTable("geofence_visits");
            e.HasKey(x => x.Id);
            e.Property(x => x.DeviceId).HasMaxLength(64);
            e.Property(x => x.GeofenceId).HasMaxLength(64);
            e.Property(x => x.GeofenceName).HasMaxLength(200);
            e.HasIndex(x => new { x.DeviceId, x.GeofenceId, x.EnteredAt });
            e.HasIndex(x => new { x.DeviceId, x.GeofenceId, x.ExitedAt });
        });

        modelBuilder.Entity<DwellEvent>(e =>
        {
            e.ToTable("dwell_events");
            e.HasKey(x => x.Id);
            e.Property(x => x.DeviceId).HasMaxLength(64);
            e.Property(x => x.GeofenceId).HasMaxLength(64);
            e.Property(x => x.GeofenceName).HasMaxLength(200);
            e.HasIndex(x => new { x.DeviceId, x.GeofenceId, x.EnteredAt }).IsUnique();
            e.HasIndex(x => x.ExitedAt);
        });
    }
}

internal sealed class UtcDateTimeConverter() : ValueConverter<DateTime, DateTime>(
    v => v.Kind == DateTimeKind.Utc ? v : DateTime.SpecifyKind(v.ToUniversalTime(), DateTimeKind.Utc),
    v => DateTime.SpecifyKind(v, DateTimeKind.Utc));

/// <summary>Lets <c>dotnet ef</c> build the model for migrations without starting a host or a database.</summary>
public sealed class AnalyticsDbContextDesignTimeFactory : IDesignTimeDbContextFactory<AnalyticsDbContext>
{
    public AnalyticsDbContext CreateDbContext(string[] args) =>
        new(new DbContextOptionsBuilder<AnalyticsDbContext>()
            .UseNpgsql("Host=localhost;Database=tracking_analytics")
            .Options);
}
