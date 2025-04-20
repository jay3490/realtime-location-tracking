using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Tracking.Core.Abstractions;
using Tracking.Core.Devices;
using Tracking.Core.Geo;
using Tracking.Core.Geofencing;
using Tracking.Testing;

namespace Tracking.Api.Tests;

public sealed class ApiFactory : WebApplicationFactory<Program>
{
    public static readonly DateTimeOffset Now = new(2026, 8, 1, 9, 0, 0, TimeSpan.Zero);

    public InMemoryDeviceStateStore States { get; } = new();

    public InMemoryGeofenceStore Geofences { get; } = new();

    public InMemoryAlertLog Alerts { get; } = new();

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.UseEnvironment("Testing");
        builder.ConfigureTestServices(services =>
        {
            services.AddSingleton<IDeviceStateStore>(States);
            services.AddSingleton<IGeofenceStore>(Geofences);
            services.AddSingleton<IAlertLog>(Alerts);
            services.AddSingleton<TimeProvider>(new ManualClock(Now));
        });
    }
}

public sealed class ApiTests : IClassFixture<ApiFactory>
{
    private static readonly GeoPoint Dam = new(52.3731, 4.8926);

    private readonly ApiFactory _factory;
    private readonly HttpClient _client;

    public ApiTests(ApiFactory factory)
    {
        _factory = factory;
        _client = factory.CreateClient();
    }

    private static async Task<JsonElement> JsonAsync(HttpResponseMessage response) =>
        JsonDocument.Parse(await response.Content.ReadAsStringAsync()).RootElement;

    [Fact]
    public async Task Position_returns_the_latest_device_state()
    {
        _factory.States.Put(new DeviceState("pos-1", Dam, ApiFactory.Now, 4.2, 90, ["dam"]));

        var response = await _client.GetAsync(new Uri("/api/devices/pos-1/position", UriKind.Relative));
        var body = await JsonAsync(response);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("pos-1", body.GetProperty("deviceId").GetString());
        Assert.Equal(Dam.Latitude, body.GetProperty("latitude").GetDouble());
        Assert.Equal(4.2, body.GetProperty("speedMetersPerSecond").GetDouble());
        Assert.Equal("dam", body.GetProperty("insideGeofenceIds")[0].GetString());
    }

    [Theory]
    [InlineData("/api/devices/nobody/position", HttpStatusCode.NotFound)]
    [InlineData("/api/devices/bad%20id/position", HttpStatusCode.BadRequest)]
    public async Task Position_rejects_unknown_or_invalid_devices(string url, HttpStatusCode expected)
    {
        var response = await _client.GetAsync(new Uri(url, UriKind.Relative));

        Assert.Equal(expected, response.StatusCode);
    }

    [Fact]
    public async Task Nearby_lists_devices_nearest_first_and_filters_by_age()
    {
        _factory.States.Put(new DeviceState("nb-far", GeoMath.Destination(Dam, 0, 800), ApiFactory.Now, null, null, []));
        _factory.States.Put(new DeviceState("nb-near", GeoMath.Destination(Dam, 0, 200), ApiFactory.Now, null, null, []));
        _factory.States.Put(new DeviceState("nb-old", GeoMath.Destination(Dam, 0, 300), ApiFactory.Now.AddHours(-1), null, null, []));

        var all = await JsonAsync(await _client.GetAsync(
            new Uri("/api/devices/nearby?lat=52.3731&lon=4.8926&radiusMeters=1000", UriKind.Relative)));
        var fresh = await JsonAsync(await _client.GetAsync(
            new Uri("/api/devices/nearby?lat=52.3731&lon=4.8926&radiusMeters=1000&maxAgeSeconds=60", UriKind.Relative)));

        var ids = all.EnumerateArray().Select(d => d.GetProperty("deviceId").GetString()).Where(id => id!.StartsWith("nb-", StringComparison.Ordinal));
        Assert.Equal(["nb-near", "nb-old", "nb-far"], ids);
        Assert.DoesNotContain(fresh.EnumerateArray(), d => d.GetProperty("deviceId").GetString() == "nb-old");
        Assert.InRange(all[0].GetProperty("distanceMeters").GetDouble(), 0, 1_000);
    }

    [Theory]
    [InlineData("/api/devices/nearby?lon=4.9")]
    [InlineData("/api/devices/nearby?lat=95&lon=4.9")]
    [InlineData("/api/devices/nearby?lat=52&lon=4.9&radiusMeters=0")]
    [InlineData("/api/devices/nearby?lat=52&lon=4.9&radiusMeters=500000")]
    [InlineData("/api/devices/nearby?lat=52&lon=4.9&limit=0")]
    [InlineData("/api/alerts?limit=5000")]
    public async Task Query_parameters_are_validated(string url)
    {
        var response = await _client.GetAsync(new Uri(url, UriKind.Relative));

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Equal("application/problem+json", response.Content.Headers.ContentType?.MediaType);
    }

    [Fact]
    public async Task Geofence_crud_round_trip()
    {
        var created = await _client.PostAsJsonAsync("/api/geofences", new
        {
            type = "circle",
            name = "Dam Square",
            center = new { latitude = Dam.Latitude, longitude = Dam.Longitude },
            radiusMeters = 150,
        });
        Assert.Equal(HttpStatusCode.Created, created.StatusCode);
        var id = (await JsonAsync(created)).GetProperty("id").GetString()!;
        Assert.StartsWith("gf-", id, StringComparison.Ordinal);
        Assert.EndsWith($"/api/geofences/{id}", created.Headers.Location!.ToString(), StringComparison.Ordinal);

        var replaced = await _client.PutAsJsonAsync($"/api/geofences/{id}", new
        {
            type = "polygon",
            name = "Dam block",
            vertices = new[]
            {
                new { latitude = 52.374, longitude = 4.891 },
                new { latitude = 52.374, longitude = 4.894 },
                new { latitude = 52.372, longitude = 4.894 },
            },
        });
        Assert.Equal(HttpStatusCode.OK, replaced.StatusCode);

        var fetched = await JsonAsync(await _client.GetAsync(new Uri($"/api/geofences/{id}", UriKind.Relative)));
        Assert.Equal("polygon", fetched.GetProperty("type").GetString());
        Assert.Equal(3, fetched.GetProperty("vertices").GetArrayLength());
        Assert.IsType<PolygonGeofence>(await _factory.Geofences.GetAsync(id));

        var list = await JsonAsync(await _client.GetAsync(new Uri("/api/geofences", UriKind.Relative)));
        Assert.Contains(list.EnumerateArray(), g => g.GetProperty("id").GetString() == id);

        Assert.Equal(HttpStatusCode.NoContent, (await _client.DeleteAsync(new Uri($"/api/geofences/{id}", UriKind.Relative))).StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, (await _client.DeleteAsync(new Uri($"/api/geofences/{id}", UriKind.Relative))).StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, (await _client.GetAsync(new Uri($"/api/geofences/{id}", UriKind.Relative))).StatusCode);
    }

    [Fact]
    public async Task Put_creates_when_absent_and_post_conflicts_on_duplicate_ids()
    {
        var body = new { type = "circle", id = "hq", name = "HQ", center = new { latitude = 1.0, longitude = 2.0 }, radiusMeters = 50 };

        var put = await _client.PutAsJsonAsync("/api/geofences/hq", body);
        var post = await _client.PostAsJsonAsync("/api/geofences", body);

        Assert.Equal(HttpStatusCode.Created, put.StatusCode);
        Assert.Equal(HttpStatusCode.Conflict, post.StatusCode);
    }

    [Theory]
    [InlineData("""{"name":"no type","radiusMeters":5}""")]
    [InlineData("""{"type":"hexagon","name":"x"}""")]
    [InlineData("""{"type":"circle","name":"x","center":{"latitude":0,"longitude":0},"radiusMeters":-1}""")]
    [InlineData("""{"type":"polygon","name":"x","vertices":[{"latitude":0,"longitude":0}]}""")]
    [InlineData("""{"type":"circle","id":"other","name":"x","center":{"latitude":0,"longitude":0},"radiusMeters":5}""")]
    [InlineData("not json")]
    public async Task Invalid_geofence_bodies_are_rejected(string json)
    {
        using var content = new StringContent(json, System.Text.Encoding.UTF8, "application/json");

        var response = await _client.PutAsync(new Uri("/api/geofences/target", UriKind.Relative), content);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Null(await _factory.Geofences.GetAsync("target"));
    }

    [Fact]
    public async Task Alerts_are_returned_newest_first()
    {
        _factory.Alerts.Add(new GeofenceAlert("a-1", "dam", "Dam", GeofenceTransition.Enter, Dam, ApiFactory.Now));
        _factory.Alerts.Add(new GeofenceAlert("a-1", "dam", "Dam", GeofenceTransition.Exit, Dam, ApiFactory.Now.AddMinutes(1)));

        var alerts = await JsonAsync(await _client.GetAsync(new Uri("/api/alerts?limit=2", UriKind.Relative)));

        Assert.Equal(2, alerts.GetArrayLength());
        Assert.Equal("exit", alerts[0].GetProperty("transition").GetString());
        Assert.Equal("enter", alerts[1].GetProperty("transition").GetString());
    }

    [Fact]
    public async Task Liveness_does_not_depend_on_redis()
    {
        var response = await _client.GetAsync(new Uri("/healthz/live", UriKind.Relative));

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }
}
