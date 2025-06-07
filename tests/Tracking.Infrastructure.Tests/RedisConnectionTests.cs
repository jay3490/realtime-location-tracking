using System.Net;
using Microsoft.Extensions.Logging.Abstractions;
using StackExchange.Redis;
using Tracking.Infrastructure.Redis;
using Tracking.Testing;

namespace Tracking.Infrastructure.Tests;

public sealed class RedisConnectionTests
{
    [Fact]
    public void Resilience_settings_come_from_the_options()
    {
        var config = RedisConnection.CreateConfiguration(new RedisOptions
        {
            ConnectionString = "redis-a:6379,redis-b:6380,password=secret",
            ConnectRetry = 7,
            ConnectTimeout = TimeSpan.FromSeconds(3),
            OperationTimeout = TimeSpan.FromMilliseconds(750),
            KeepAlive = TimeSpan.FromSeconds(15),
            ReconnectBaseDelay = TimeSpan.FromMilliseconds(200),
            ReconnectMaxDelay = TimeSpan.FromSeconds(5),
        });

        Assert.Equal(2, config.EndPoints.Count);
        Assert.Equal("secret", config.Password);
        Assert.False(config.AbortOnConnectFail);
        Assert.Equal(7, config.ConnectRetry);
        Assert.Equal(3_000, config.ConnectTimeout);
        Assert.Equal(750, config.SyncTimeout);
        Assert.Equal(750, config.AsyncTimeout);
        Assert.Equal(15, config.KeepAlive);
        Assert.IsType<ExponentialRetry>(config.ReconnectRetryPolicy);
        Assert.Same(BacklogPolicy.FailFast, config.BacklogPolicy);
        Assert.False(string.IsNullOrEmpty(config.ClientName));
    }

    [Fact]
    public void Queueing_while_disconnected_can_be_chosen_instead_of_failing_fast()
    {
        var config = RedisConnection.CreateConfiguration(new RedisOptions { FailFastWhenDisconnected = false });

        Assert.Same(BacklogPolicy.Default, config.BacklogPolicy);
    }

    [Fact]
    public void An_explicit_client_name_in_the_connection_string_is_kept()
    {
        var config = RedisConnection.CreateConfiguration(new RedisOptions { ConnectionString = "localhost,name=ingest-7" });

        Assert.Equal("ingest-7", config.ClientName);
    }

    [Fact]
    public void Connection_failures_and_restorations_are_counted()
    {
        using var meters = new TestMeterFactory();
        using var recorder = new MetricRecorder(meters);
        var options = new RedisOptions { ConnectionString = "127.0.0.1:1", ConnectTimeout = TimeSpan.FromMilliseconds(100), ConnectRetry = 1 };
        using var multiplexer = ConnectionMultiplexer.Connect(RedisConnection.CreateConfiguration(options));
        var events = RedisConnectionEvents.Attach(multiplexer, NullLogger.Instance, meters);
        var endPoint = new IPEndPoint(IPAddress.Loopback, 1);

        events.OnConnectionFailed(new ConnectionFailedEventArgs(
            multiplexer, endPoint, ConnectionType.Interactive, ConnectionFailureType.SocketFailure, new IOException("reset"), "test"));
        events.OnConnectionRestored(new ConnectionFailedEventArgs(
            multiplexer, endPoint, ConnectionType.Interactive, ConnectionFailureType.None, null!, "test"));
        recorder.Observe();

        Assert.Equal(1, recorder.Sum("tracking.redis.connection.events", "event", "failed"));
        Assert.Equal(1, recorder.Sum("tracking.redis.connection.events", "event", "restored"));
        Assert.Equal(0, recorder.Last("tracking.redis.connected"));
    }
}
