using Microsoft.Extensions.Options;
using Tracking.Core.Abstractions;
using Tracking.Core.Geofencing;

namespace Tracking.Processor.Processing;

/// <summary>
/// Keeps the geofence set in memory and reloads it only when the store's version counter moves.
/// The counter is checked at most once per <see cref="ProcessorOptions.GeofenceRefreshInterval"/>,
/// which bounds how long a geofence change takes to reach this processor.
/// </summary>
public sealed class GeofenceCache(IGeofenceStore store, TimeProvider clock, IOptions<ProcessorOptions> options) : IDisposable
{
    private readonly TimeSpan _refreshInterval = options.Value.GeofenceRefreshInterval;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private IReadOnlyList<Geofence> _geofences = [];
    private long? _version;
    private DateTimeOffset _checkedAt = DateTimeOffset.MinValue;

    public async Task<IReadOnlyList<Geofence>> GetAsync(CancellationToken cancellationToken = default)
    {
        if (_version is not null && clock.GetUtcNow() - _checkedAt < _refreshInterval)
        {
            return _geofences;
        }

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var now = clock.GetUtcNow();
            if (_version is not null && now - _checkedAt < _refreshInterval)
            {
                return _geofences;
            }

            var version = await store.GetVersionAsync(cancellationToken).ConfigureAwait(false);
            if (version != _version)
            {
                // Reading the set after the version means a concurrent change can only make the
                // cached set newer than its recorded version, which triggers a harmless extra reload.
                _geofences = await store.GetAllAsync(cancellationToken).ConfigureAwait(false);
                _version = version;
            }

            _checkedAt = now;
            return _geofences;
        }
        finally
        {
            _gate.Release();
        }
    }

    public void Dispose() => _gate.Dispose();
}
