using System.Diagnostics.Metrics;
using System.Reflection;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using Tracking.Infrastructure.Observability;

namespace Tracking.Infrastructure.Redis;

/// <summary>
/// Builds the shared multiplexer from <see cref="RedisOptions"/> and reports what happens to it.
/// </summary>
public static partial class RedisConnection
{
    /// <summary>
    /// Translates the options into a StackExchange.Redis configuration. The resilience settings here
    /// take precedence over the same keys in the connection string, so they are set in one place.
    /// </summary>
    public static ConfigurationOptions CreateConfiguration(RedisOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        var config = ConfigurationOptions.Parse(options.ConnectionString);

        // Never fail startup because Redis is not up yet; keep retrying in the background.
        config.AbortOnConnectFail = false;
        config.ConnectRetry = options.ConnectRetry;
        config.ConnectTimeout = ToMilliseconds(options.ConnectTimeout);
        config.SyncTimeout = ToMilliseconds(options.OperationTimeout);
        config.AsyncTimeout = ToMilliseconds(options.OperationTimeout);
        config.KeepAlive = (int)Math.Ceiling(options.KeepAlive.TotalSeconds);
        config.ReconnectRetryPolicy = new ExponentialRetry(
            ToMilliseconds(options.ReconnectBaseDelay),
            ToMilliseconds(options.ReconnectMaxDelay));

        // Queueing commands while disconnected hides an outage behind timeouts. Failing fast lets ingest
        // answer "retryable" at once and lets the processor back off, which is what callers here want.
        config.BacklogPolicy = options.FailFastWhenDisconnected ? BacklogPolicy.FailFast : BacklogPolicy.Default;
        config.ClientName ??= Assembly.GetEntryAssembly()?.GetName().Name;

        return config;
    }

    /// <summary>Connects (without waiting for Redis to be reachable) and attaches logging and metrics.</summary>
    public static IConnectionMultiplexer Connect(RedisOptions options, ILogger logger, IMeterFactory meterFactory)
    {
        var multiplexer = ConnectionMultiplexer.Connect(CreateConfiguration(options));
        RedisConnectionEvents.Attach(multiplexer, logger, meterFactory);
        return multiplexer;
    }

    private static int ToMilliseconds(TimeSpan value) => (int)Math.Clamp(value.TotalMilliseconds, 1, int.MaxValue);
}

/// <summary>
/// Logs connection failures and restorations and exposes them as metrics: a counter of connection
/// events and a gauge that is 1 while the multiplexer is connected.
/// </summary>
public sealed partial class RedisConnectionEvents
{
    private readonly ILogger _logger;
    private readonly Counter<long> _events;

    private RedisConnectionEvents(IConnectionMultiplexer multiplexer, ILogger logger, IMeterFactory meterFactory)
    {
        _logger = logger;
        var meter = meterFactory.Create(TrackingMeters.Redis);
        _events = meter.CreateCounter<long>(
            "tracking.redis.connection.events",
            unit: "{event}",
            description: "Redis connection failures and restorations, by event and failure type.");
        meter.CreateObservableGauge(
            "tracking.redis.connected",
            () => multiplexer.IsConnected ? 1 : 0,
            description: "1 while the Redis connection is up, 0 otherwise.");
    }

    public static RedisConnectionEvents Attach(IConnectionMultiplexer multiplexer, ILogger logger, IMeterFactory meterFactory)
    {
        ArgumentNullException.ThrowIfNull(multiplexer);
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(meterFactory);

        var events = new RedisConnectionEvents(multiplexer, logger, meterFactory);
        multiplexer.ConnectionFailed += (_, args) => events.OnConnectionFailed(args);
        multiplexer.ConnectionRestored += (_, args) => events.OnConnectionRestored(args);
        multiplexer.ErrorMessage += (_, args) => events.OnErrorMessage(args);
        return events;
    }

    public void OnConnectionFailed(ConnectionFailedEventArgs args)
    {
        ArgumentNullException.ThrowIfNull(args);
        _events.Add(1, [new("event", "failed"), new("failure", args.FailureType.ToString())]);
        LogConnectionFailed(args.Exception, args.EndPoint?.ToString(), args.ConnectionType, args.FailureType);
    }

    public void OnConnectionRestored(ConnectionFailedEventArgs args)
    {
        ArgumentNullException.ThrowIfNull(args);
        _events.Add(1, [new("event", "restored"), new("failure", args.FailureType.ToString())]);
        LogConnectionRestored(args.EndPoint?.ToString(), args.ConnectionType);
    }

    public void OnErrorMessage(RedisErrorEventArgs args)
    {
        ArgumentNullException.ThrowIfNull(args);
        LogServerError(args.EndPoint?.ToString(), args.Message);
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "Redis {ConnectionType} connection to {EndPoint} failed ({FailureType}); reconnecting in the background")]
    private partial void LogConnectionFailed(Exception? exception, string? endPoint, ConnectionType connectionType, ConnectionFailureType failureType);

    [LoggerMessage(Level = LogLevel.Information, Message = "Redis {ConnectionType} connection to {EndPoint} restored")]
    private partial void LogConnectionRestored(string? endPoint, ConnectionType connectionType);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Redis server {EndPoint} reported an error: {Message}")]
    private partial void LogServerError(string? endPoint, string message);
}
