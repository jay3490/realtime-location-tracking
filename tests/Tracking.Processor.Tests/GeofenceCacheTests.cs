using Microsoft.Extensions.Options;
using Tracking.Core.Geo;
using Tracking.Core.Geofencing;
using Tracking.Processor.Processing;
using Tracking.Testing;

namespace Tracking.Processor.Tests;

public sealed class GeofenceCacheTests
{
    private readonly ManualClock _clock = new(new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero));
    private readonly InMemoryGeofenceStore _store = new();

    private GeofenceCache Cache() =>
        new(_store, _clock, Options.Create(new ProcessorOptions { GeofenceRefreshInterval = TimeSpan.FromSeconds(5) }));

    [Fact]
    public async Task Reloads_only_when_the_version_changes()
    {
        await _store.UpsertAsync(new CircleGeofence("a", "A", new GeoPoint(0, 0), 10));
        var cache = Cache();

        Assert.Single(await cache.GetAsync());
        _clock.Advance(TimeSpan.FromSeconds(6));
        Assert.Single(await cache.GetAsync());

        Assert.Equal(1, _store.FullReads);
    }

    [Fact]
    public async Task Changes_become_visible_after_the_refresh_interval()
    {
        await _store.UpsertAsync(new CircleGeofence("a", "A", new GeoPoint(0, 0), 10));
        var cache = Cache();
        await cache.GetAsync();

        await _store.UpsertAsync(new CircleGeofence("b", "B", new GeoPoint(1, 1), 10));
        Assert.Single(await cache.GetAsync());

        _clock.Advance(TimeSpan.FromSeconds(5));
        Assert.Equal(["a", "b"], (await cache.GetAsync()).Select(g => g.Id));
        Assert.Equal(2, _store.FullReads);
    }
}
