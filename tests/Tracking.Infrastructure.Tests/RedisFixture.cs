using Microsoft.Extensions.Options;
using StackExchange.Redis;
using Tracking.Infrastructure.Redis;

namespace Tracking.Infrastructure.Tests;

/// <summary>
/// Runs only when <c>TRACKING_TEST_REDIS</c> points at a disposable Redis instance
/// (for example <c>localhost:6379</c>); otherwise the test is reported as skipped.
/// </summary>
public sealed class RedisFactAttribute : FactAttribute
{
    public const string EnvironmentVariable = "TRACKING_TEST_REDIS";

    public RedisFactAttribute()
    {
        if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(EnvironmentVariable)))
        {
            Skip = $"Set {EnvironmentVariable} to run tests against a real Redis.";
        }
    }
}

public sealed class RedisFixture : IAsyncLifetime
{
    private ConnectionMultiplexer? _connection;

    public IConnectionMultiplexer Connection =>
        _connection ?? throw new InvalidOperationException($"{RedisFactAttribute.EnvironmentVariable} is not set.");

    public async Task InitializeAsync()
    {
        var connectionString = Environment.GetEnvironmentVariable(RedisFactAttribute.EnvironmentVariable);
        if (!string.IsNullOrWhiteSpace(connectionString))
        {
            _connection = await ConnectionMultiplexer.ConnectAsync(connectionString);
        }
    }

    public async Task DisposeAsync()
    {
        if (_connection is not null)
        {
            await _connection.DisposeAsync();
        }
    }

    /// <summary>Options with a key prefix unique to one test, so tests never see each other's data.</summary>
    public static IOptions<RedisOptions> UniqueOptions(Action<RedisOptions>? configure = null)
    {
        var options = new RedisOptions { KeyPrefix = $"test-{Guid.NewGuid():N}" };
        configure?.Invoke(options);
        return Options.Create(options);
    }

    public async Task DeleteKeysAsync(string prefix)
    {
        foreach (var endpoint in Connection.GetEndPoints())
        {
            var server = Connection.GetServer(endpoint);
            if (server.IsReplica)
            {
                continue;
            }

            await foreach (var key in server.KeysAsync(pattern: $"{prefix}:*"))
            {
                await Connection.GetDatabase().KeyDeleteAsync(key);
            }
        }
    }
}

[CollectionDefinition(Name)]
public sealed class RedisTestGroup : ICollectionFixture<RedisFixture>
{
    public const string Name = "redis";
}
