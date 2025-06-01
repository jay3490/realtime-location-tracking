using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Tracking.Contracts;
using Tracking.Contracts.V1;
using Tracking.Core.Geo;
using Tracking.Ingest.Services;
using DomainAlert = Tracking.Core.Geofencing.GeofenceAlert;
using DomainTransition = Tracking.Core.Geofencing.GeofenceTransition;

namespace Tracking.Ingest.Tests;

public sealed class LocationIngestServiceTests : IDisposable
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    private readonly IngestFactory _factory = new();

    public void Dispose() => _factory.Dispose();

    private AsyncDuplexStreamingCall<LocationPing, ServerMessage> Open(string? deviceId)
    {
        var headers = new Metadata();
        if (deviceId is not null)
        {
            headers.Add(ContractMapping.DeviceIdHeader, deviceId);
        }

        return _factory.CreateIngestClient().StreamLocations(headers);
    }

    private static LocationPing Ping(ulong sequence, double lat = 51.5, double lon = -0.12, DateTimeOffset? at = null) => new()
    {
        Sequence = sequence,
        Latitude = lat,
        Longitude = lon,
        RecordedAt = Timestamp.FromDateTimeOffset(at ?? IngestFactory.Now.AddSeconds(-5)),
    };

    private static async Task<ServerMessage> NextAsync(AsyncDuplexStreamingCall<LocationPing, ServerMessage> call)
    {
        Assert.True(await call.ResponseStream.MoveNext().WaitAsync(Timeout), "the server closed the stream");
        return call.ResponseStream.Current;
    }

    [Fact]
    public async Task Valid_pings_are_appended_and_acknowledged_in_order()
    {
        using var call = Open("van-1");

        for (ulong seq = 1; seq <= 3; seq++)
        {
            await call.RequestStream.WriteAsync(Ping(seq, lat: 51.5 + (seq / 1000.0)));
        }

        var acks = new List<PingAck>();
        for (var i = 0; i < 3; i++)
        {
            acks.Add((await NextAsync(call)).Ack);
        }

        await call.RequestStream.CompleteAsync();
        Assert.False(await call.ResponseStream.MoveNext().WaitAsync(Timeout));

        Assert.Equal([1UL, 2UL, 3UL], acks.Select(a => a.Sequence));
        Assert.All(acks, a => Assert.True(a.Accepted));
        Assert.Equal(_factory.Log.Entries.Select(e => e.Id), acks.Select(a => a.EntryId));
        Assert.All(_factory.Log.Entries, e => Assert.Equal("van-1", e.Ping.DeviceId));
        Assert.Equal(51.503, _factory.Log.Entries.Last().Ping.Position.Latitude, precision: 9);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("bad id")]
    public async Task Streams_without_a_valid_device_header_are_refused(string? deviceId)
    {
        using var call = Open(deviceId);

        var error = await Assert.ThrowsAsync<RpcException>(() => call.ResponseStream.MoveNext().WaitAsync(Timeout));

        Assert.Equal(StatusCode.InvalidArgument, error.StatusCode);
        Assert.Contains(ContractMapping.DeviceIdHeader, error.Status.Detail, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Invalid_pings_are_rejected_without_closing_the_stream()
    {
        using var call = Open("van-1");

        await call.RequestStream.WriteAsync(Ping(1, lat: 123));
        await call.RequestStream.WriteAsync(Ping(2, at: IngestFactory.Now.AddHours(1)));
        await call.RequestStream.WriteAsync(Ping(3, at: IngestFactory.Now.AddDays(-3)));
        await call.RequestStream.WriteAsync(Ping(4));

        var rejectedLat = (await NextAsync(call)).Ack;
        var rejectedFuture = (await NextAsync(call)).Ack;
        var rejectedOld = (await NextAsync(call)).Ack;
        var accepted = (await NextAsync(call)).Ack;
        await call.RequestStream.CompleteAsync();

        Assert.False(rejectedLat.Accepted);
        Assert.Contains("latitude", rejectedLat.Reason, StringComparison.Ordinal);
        Assert.Contains("future", rejectedFuture.Reason, StringComparison.Ordinal);
        Assert.Contains("too old", rejectedOld.Reason, StringComparison.Ordinal);
        Assert.False(rejectedLat.Retryable);
        Assert.True(accepted.Accepted);
        Assert.Equal(4UL, accepted.Sequence);
        Assert.Single(_factory.Log.Entries);
    }

    [Fact]
    public async Task Missing_timestamp_defaults_to_the_server_receive_time()
    {
        using var call = Open("van-1");

        await call.RequestStream.WriteAsync(new LocationPing { Sequence = 9, Latitude = 1, Longitude = 2, AccuracyMeters = 3.5 });
        var ack = (await NextAsync(call)).Ack;
        await call.RequestStream.CompleteAsync();

        Assert.True(ack.Accepted);
        var (_, ping) = Assert.Single(_factory.Log.Entries);
        Assert.Equal(IngestFactory.Now, ping.RecordedAt);
        Assert.Equal(3.5, ping.AccuracyMeters);
    }

    [Fact]
    public async Task Storage_outages_are_reported_as_retryable()
    {
        _factory.Log.Unavailable = true;
        using var call = Open("van-1");

        await call.RequestStream.WriteAsync(Ping(1));
        var ack = (await NextAsync(call)).Ack;
        await call.RequestStream.CompleteAsync();

        Assert.False(ack.Accepted);
        Assert.True(ack.Retryable);
        Assert.Empty(ack.EntryId);
    }

    [Fact]
    public async Task Alerts_are_pushed_to_the_connected_device_only()
    {
        using var mine = Open("van-1");
        using var other = Open("van-2");

        // An ack proves each stream is registered before the alert is published.
        await mine.RequestStream.WriteAsync(Ping(1));
        await other.RequestStream.WriteAsync(Ping(1));
        await NextAsync(mine);
        await NextAsync(other);

        var alert = new DomainAlert("van-1", "depot", "Depot", DomainTransition.Enter, new GeoPoint(51.5, -0.12), IngestFactory.Now);
        await _factory.Alerts.PublishAsync(alert);

        var pushed = (await NextAsync(mine)).Alert;
        Assert.Equal("van-1", pushed.DeviceId);
        Assert.Equal("depot", pushed.GeofenceId);
        Assert.Equal("Depot", pushed.GeofenceName);
        Assert.Equal(GeofenceTransition.Enter, pushed.Transition);
        Assert.Equal(IngestFactory.Now, pushed.OccurredAt.ToDateTimeOffset());

        await other.RequestStream.CompleteAsync();
        Assert.False(await other.ResponseStream.MoveNext().WaitAsync(Timeout));
        await mine.RequestStream.CompleteAsync();
    }

    [Fact]
    public async Task Closed_streams_are_removed_from_the_session_registry()
    {
        var registry = _factory.Services.GetRequiredService<DeviceSessionRegistry>();
        using (var call = Open("van-1"))
        {
            await call.RequestStream.WriteAsync(Ping(1));
            await NextAsync(call);
            Assert.Equal(1, registry.Count);

            await call.RequestStream.CompleteAsync();
            Assert.False(await call.ResponseStream.MoveNext().WaitAsync(Timeout));
        }

        Assert.Equal(0, registry.Count);
    }

    [Fact]
    public async Task Ping_outcomes_are_exported_for_prometheus()
    {
        using (var call = Open("van-1"))
        {
            await call.RequestStream.WriteAsync(Ping(1));
            await call.RequestStream.WriteAsync(Ping(2, lat: 123));
            await NextAsync(call);
            await NextAsync(call);
            await call.RequestStream.CompleteAsync();
            Assert.False(await call.ResponseStream.MoveNext().WaitAsync(Timeout));
        }

        using var http = _factory.CreateClient();
        var text = await http.GetStringAsync(new Uri("/metrics", UriKind.Relative));

        Assert.Contains("tracking_ingest_pings_total{", text, StringComparison.Ordinal);
        Assert.Contains("outcome=\"accepted\"", text, StringComparison.Ordinal);
        Assert.Contains("outcome=\"rejected\"", text, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Pings_waiting_on_a_slow_log_are_acknowledged_in_order_once_it_recovers()
    {
        _factory.Settings["Ingest:InboundCapacity"] = "4";
        _factory.Settings["Ingest:AppendQueueCapacity"] = "4";
        _factory.Log.Close();
        using var call = Open("van-1");

        for (ulong seq = 1; seq <= 20; seq++)
        {
            await call.RequestStream.WriteAsync(Ping(seq, lat: 51.5 + (seq / 10_000.0)));
        }

        await Task.Delay(200);
        Assert.Empty(_factory.Log.Entries);
        _factory.Log.Open();

        var acks = new List<PingAck>();
        for (var i = 0; i < 20; i++)
        {
            acks.Add((await NextAsync(call)).Ack);
        }

        await call.RequestStream.CompleteAsync();

        Assert.Equal(Enumerable.Range(1, 20).Select(i => (ulong)i), acks.Select(a => a.Sequence));
        Assert.All(acks, a => Assert.True(a.Accepted));
        Assert.Equal(_factory.Log.Entries.Select(e => e.Id), acks.Select(a => a.EntryId));
        Assert.True(_factory.Log.BatchSizes.Count < 20, "pings that waited should have been appended in batches");
    }

    [Fact]
    public async Task Only_the_pings_whose_append_failed_are_retryable()
    {
        _factory.Log.FailWhere = ping => ping.Position.Latitude > 51.6;
        using var call = Open("van-1");

        await call.RequestStream.WriteAsync(Ping(1, lat: 51.55));
        await call.RequestStream.WriteAsync(Ping(2, lat: 51.65));
        var first = (await NextAsync(call)).Ack;
        var second = (await NextAsync(call)).Ack;
        await call.RequestStream.CompleteAsync();

        Assert.True(first.Accepted);
        Assert.False(second.Accepted);
        Assert.True(second.Retryable);
        Assert.Single(_factory.Log.Entries);
    }

    [Fact]
    public async Task Draining_acknowledges_what_was_read_then_asks_the_device_to_reconnect()
    {
        var drain = _factory.Services.GetRequiredService<StreamDrain>();
        using var call = Open("van-1");
        await call.RequestStream.WriteAsync(Ping(1));
        Assert.True((await NextAsync(call)).Ack.Accepted);

        _factory.Log.Close();
        await call.RequestStream.WriteAsync(Ping(2));
        await call.RequestStream.WriteAsync(Ping(3));
        await Task.Delay(200); // let the server read both while their appends are held

        drain.Begin();
        _factory.Log.Open();

        var acks = new[] { (await NextAsync(call)).Ack, (await NextAsync(call)).Ack };
        var ended = await Assert.ThrowsAsync<RpcException>(() => call.ResponseStream.MoveNext().WaitAsync(Timeout));

        Assert.Equal([2UL, 3UL], acks.Select(a => a.Sequence));
        Assert.All(acks, a => Assert.True(a.Accepted));
        Assert.Equal(StatusCode.Unavailable, ended.StatusCode);
        Assert.Equal(3, _factory.Log.Entries.Count);
        Assert.Equal(0, drain.ActiveStreams);
    }

    [Fact]
    public async Task New_streams_are_refused_while_draining()
    {
        _factory.Services.GetRequiredService<StreamDrain>().Begin();
        using var call = Open("van-1");

        var error = await Assert.ThrowsAsync<RpcException>(() => call.ResponseStream.MoveNext().WaitAsync(Timeout));

        Assert.Equal(StatusCode.Unavailable, error.StatusCode);
    }

    [Fact]
    public async Task Stopping_the_host_starts_draining_and_fails_readiness()
    {
        var drain = _factory.Services.GetRequiredService<StreamDrain>();
        var check = new StreamDrainHealthCheck(drain);
        Assert.Equal(HealthStatus.Healthy, (await check.CheckHealthAsync(new HealthCheckContext())).Status);

        _factory.Services.GetRequiredService<IHostApplicationLifetime>().StopApplication();

        Assert.True(drain.IsDraining);
        Assert.Equal(HealthStatus.Unhealthy, (await check.CheckHealthAsync(new HealthCheckContext())).Status);
    }
}
