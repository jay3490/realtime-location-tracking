using Microsoft.EntityFrameworkCore;

namespace Tracking.Analytics.Data;

public sealed record DailyDistanceReport(DateOnly Day, double DistanceMeters, int Positions, DateTime FirstRecordedAt, DateTime LastRecordedAt);

/// <param name="SecondsInside">Time inside the geofence that falls within the requested window.</param>
/// <param name="Visits">Visits that overlap the window.</param>
/// <param name="Ongoing">True when the device is still inside (an open visit overlaps the window).</param>
public sealed record GeofenceTimeReport(string GeofenceId, string GeofenceName, double SecondsInside, int Visits, bool Ongoing);

public sealed record DwellReport(string DeviceId, string GeofenceId, string GeofenceName, DateTime EnteredAt, DateTime ExitedAt, double DurationSeconds);

/// <summary>Read-only queries over the aggregated analytics tables.</summary>
public sealed class AnalyticsReports(AnalyticsDbContext db)
{
    public async Task<IReadOnlyList<DailyDistanceReport>> GetDailyDistanceAsync(
        string deviceId, DateOnly from, DateOnly to, CancellationToken cancellationToken = default) =>
        await db.DailyDistances.AsNoTracking()
            .Where(d => d.DeviceId == deviceId && d.Day >= from && d.Day <= to)
            .OrderBy(d => d.Day)
            .Select(d => new DailyDistanceReport(d.Day, d.DistanceMeters, d.Positions, d.FirstRecordedAt, d.LastRecordedAt))
            .ToListAsync(cancellationToken)
            .ConfigureAwait(false);

    /// <summary>
    /// Time a device spent inside each geofence within [<paramref name="from"/>, <paramref name="to"/>).
    /// Visits are clipped to the window; a visit still open counts up to <paramref name="now"/>.
    /// Visits whose enter has not been seen yet are left out, since their start is unknown.
    /// </summary>
    public async Task<IReadOnlyList<GeofenceTimeReport>> GetGeofenceTimeAsync(
        string deviceId, DateTime from, DateTime to, DateTime now, CancellationToken cancellationToken = default)
    {
        var visits = await db.GeofenceVisits.AsNoTracking()
            .Where(v => v.DeviceId == deviceId && v.EnteredAt != null && v.EnteredAt < to && (v.ExitedAt == null || v.ExitedAt > from))
            .ToListAsync(cancellationToken)
            .ConfigureAwait(false);

        return visits
            .GroupBy(v => v.GeofenceId, StringComparer.Ordinal)
            .Select(g =>
            {
                var seconds = g.Sum(v =>
                {
                    var start = Max(v.EnteredAt!.Value, from);
                    var end = Min(v.ExitedAt ?? now, to);
                    return end > start ? (end - start).TotalSeconds : 0;
                });
                var latest = g.MaxBy(v => v.EnteredAt)!;
                return new GeofenceTimeReport(g.Key, latest.GeofenceName, seconds, g.Count(), g.Any(v => v.ExitedAt is null));
            })
            .OrderByDescending(r => r.SecondsInside)
            .ThenBy(r => r.GeofenceId, StringComparer.Ordinal)
            .ToArray();
    }

    /// <summary>Dwell events that ended within [<paramref name="from"/>, <paramref name="to"/>), newest first.</summary>
    public async Task<IReadOnlyList<DwellReport>> GetDwellEventsAsync(
        string? deviceId, string? geofenceId, DateTime from, DateTime to, int limit, CancellationToken cancellationToken = default)
    {
        var query = db.DwellEvents.AsNoTracking().Where(d => d.ExitedAt >= from && d.ExitedAt < to);
        if (deviceId is not null)
        {
            query = query.Where(d => d.DeviceId == deviceId);
        }

        if (geofenceId is not null)
        {
            query = query.Where(d => d.GeofenceId == geofenceId);
        }

        return await query
            .OrderByDescending(d => d.ExitedAt)
            .Take(limit)
            .Select(d => new DwellReport(d.DeviceId, d.GeofenceId, d.GeofenceName, d.EnteredAt, d.ExitedAt, d.DurationSeconds))
            .ToListAsync(cancellationToken)
            .ConfigureAwait(false);
    }

    private static DateTime Max(DateTime a, DateTime b) => a > b ? a : b;

    private static DateTime Min(DateTime a, DateTime b) => a < b ? a : b;
}
