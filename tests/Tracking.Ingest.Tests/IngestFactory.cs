using System.Collections.Concurrent;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis;
using Tracking.Core.Abstractions;
using Tracking.Testing;
using GeofenceAlert = Tracking.Core.Geofencing.GeofenceAlert;
using DomainPing = Tracking.Core.Devices.LocationPing;

namespace Tracking.Ingest.Tests;

/// <summary>
/// Hosts the real ingest app on an in-memory test server, with the Redis-backed log and alert
/// feed replaced by in-process fakes.
/// </summary>
public sealed class IngestFactory : WebApplicationFactory<Program>
{
    public static readonly DateTimeOffset Now = new(2026, 6, 1, 12, 0, 0, TimeSpan.Zero);

    public InMemoryLocationLog Log { get; } = new();

    public ManualAlertFeed Alerts { get; } = new();

    public Contracts.V1.LocationIngest.LocationIngestClient CreateIngestClient()
    {
        var channel = GrpcChannel.ForAddress(Server.BaseAddress, new GrpcChannelOptions
        {
            HttpHandler = Server.CreateHandler(),
        });
        return new Contracts.V1.LocationIngest.LocationIngestClient(channel);
    }

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.UseEnvironment("Testing");
        builder.ConfigureTestServices(services =>
        {
            services.AddSingleton<ILocationLog>(Log);
            services.AddSingleton<IAlertFeed>(Alerts);
            services.AddSingleton<TimeProvider>(new ManualClock(Now));
        });
    }
}

public sealed class InMemoryLocationLog : ILocationLog
{
    private long _sequence;

    public ConcurrentQueue<(string Id, DomainPing Ping)> Entries { get; } = new();

    /// <summary>When set, appends fail as if Redis were unreachable.</summary>
    public bool Unavailable { get; set; }

    public Task<string> AppendAsync(DomainPing ping, CancellationToken cancellationToken = default)
    {
        if (Unavailable)
        {
            throw new RedisConnectionException(
                ConnectionFailureType.UnableToConnect, CommandFlags.None, "test: Redis is down", innerException: null, CommandStatus.Unknown);
        }

        var id = $"{Interlocked.Increment(ref _sequence)}-0";
        Entries.Enqueue((id, ping));
        return Task.FromResult(id);
    }
}

/// <summary>An alert feed the test publishes to directly.</summary>
public sealed class ManualAlertFeed : IAlertFeed
{
    private readonly TaskCompletionSource<Func<GeofenceAlert, Task>> _handler =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    public Task<IAsyncDisposable> SubscribeAsync(Func<GeofenceAlert, Task> handler, CancellationToken cancellationToken = default)
    {
        _handler.TrySetResult(handler);
        return Task.FromResult<IAsyncDisposable>(new NoopSubscription());
    }

    public async Task PublishAsync(GeofenceAlert alert)
    {
        var handler = await _handler.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await handler(alert);
    }

    private sealed class NoopSubscription : IAsyncDisposable
    {
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
