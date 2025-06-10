using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Tracking.Analytics.Data;

namespace Tracking.Analytics.Tests;

/// <summary>
/// A private in-memory SQLite database that lives as long as this object, with the analytics schema.
/// The production provider is PostgreSQL; the queries used are portable across both.
/// </summary>
public sealed class SqliteAnalyticsDatabase : IDisposable
{
    private readonly SqliteConnection _connection = new("DataSource=:memory:");

    public SqliteAnalyticsDatabase()
    {
        _connection.Open();
        using var db = CreateContext();
        db.Database.EnsureCreated();
    }

    public SqliteConnection Connection => _connection;

    public DbContextOptions<AnalyticsDbContext> Options =>
        new DbContextOptionsBuilder<AnalyticsDbContext>().UseSqlite(_connection).Options;

    public AnalyticsDbContext CreateContext() => new(Options);

    public void Dispose() => _connection.Dispose();
}
