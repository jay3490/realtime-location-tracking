using Tracking.Core.Geofencing;

namespace Tracking.Core.Abstractions;

/// <summary>
/// Live geofence alerts as they are committed. Delivery is best effort: subscribers that are not
/// connected when an alert is published do not receive it; <see cref="IAlertLog"/> keeps history.
/// </summary>
public interface IAlertFeed
{
    /// <summary>
    /// Invokes <paramref name="handler"/> for every alert, one at a time and in publish order,
    /// until the returned subscription is disposed.
    /// </summary>
    Task<IAsyncDisposable> SubscribeAsync(Func<GeofenceAlert, Task> handler, CancellationToken cancellationToken = default);
}

/// <summary>Recent alerts, newest first.</summary>
public interface IAlertLog
{
    Task<IReadOnlyList<GeofenceAlert>> GetRecentAsync(int count, CancellationToken cancellationToken = default);
}
