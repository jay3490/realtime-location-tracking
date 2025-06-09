using System.ComponentModel.DataAnnotations;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using Tracking.Core.Analytics;
using Tracking.Core.Geofencing;

namespace Tracking.Analytics.Data;

public sealed class AnalyticsDataOptions
{
    public const string SectionName = "Analytics";

    /// <summary>A completed visit at least this long is recorded as a dwell event.</summary>
    [Range(typeof(TimeSpan), "00:00:01", "30.00:00:00")]
    public TimeSpan DwellThreshold { get; set; } = TimeSpan.FromMinutes(5);
}

/// <summary>What applying a batch of events did.</summary>
public sealed record AggregationResult(int Applied, int Duplicates);

/// <summary>
/// Folds analytics events into the reporting tables. Every batch is one transaction, and each event
/// is applied only if its id is not in the ledger yet, so replaying a batch (after a crash before the
/// Kafka offset was committed, or a rebalance) changes nothing.
/// </summary>
/// <remarks>
/// Nothing here relies on the order events arrive in: distances are carried on each position event
/// and summed, and enter/exit events are paired by time whichever arrives first.
/// </remarks>
public sealed class AnalyticsAggregator(AnalyticsDbContext db, TimeProvider clock, IOptions<AnalyticsDataOptions> options)
{
    private readonly TimeSpan _dwellThreshold = options.Value.DwellThreshold;

    public async Task<AggregationResult> ApplyAsync(IReadOnlyList<AnalyticsEvent> events, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(events);
        if (events.Count == 0)
        {
            return new AggregationResult(0, 0);
        }

        var ids = events.Select(e => e.EventId).Distinct(StringComparer.Ordinal).ToArray();
        var seen = (await db.ProcessedEvents
                .Where(p => ids.Contains(p.EventId))
                .Select(p => p.EventId)
                .ToListAsync(cancellationToken)
                .ConfigureAwait(false))
            .ToHashSet(StringComparer.Ordinal);

        var fresh = new List<AnalyticsEvent>(events.Count);
        foreach (var analyticsEvent in events)
        {
            // Add returns false for ids already applied, or repeated within this batch.
            if (seen.Add(analyticsEvent.EventId))
            {
                fresh.Add(analyticsEvent);
            }
        }

        if (fresh.Count == 0)
        {
            return new AggregationResult(0, events.Count);
        }

        await using var transaction = await db.Database.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);

        var now = clock.GetUtcNow().UtcDateTime;
        db.ProcessedEvents.AddRange(fresh.Select(e => new ProcessedEvent { EventId = e.EventId, ProcessedAt = now }));

        await ApplyPositionsAsync(fresh.OfType<PositionEvent>().ToArray(), cancellationToken).ConfigureAwait(false);
        foreach (var geofenceEvent in fresh.OfType<GeofenceEvent>().OrderBy(e => e.OccurredAt))
        {
            await ApplyTransitionAsync(geofenceEvent, cancellationToken).ConfigureAwait(false);
        }

        await db.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        db.ChangeTracker.Clear();

        return new AggregationResult(fresh.Count, events.Count - fresh.Count);
    }

    /// <summary>Removes ledger entries older than <paramref name="age"/>; returns how many.</summary>
    public Task<int> PruneLedgerAsync(TimeSpan age, CancellationToken cancellationToken = default)
    {
        var cutoff = clock.GetUtcNow().UtcDateTime - age;
        return db.ProcessedEvents.Where(p => p.ProcessedAt < cutoff).ExecuteDeleteAsync(cancellationToken);
    }

    private async Task ApplyPositionsAsync(PositionEvent[] positions, CancellationToken cancellationToken)
    {
        if (positions.Length == 0)
        {
            return;
        }

        var devices = positions.Select(p => p.DeviceId).Distinct(StringComparer.Ordinal).ToArray();
        var days = positions.Select(p => DayOf(p.RecordedAt)).Distinct().ToArray();
        var rows = await db.DailyDistances
            .Where(d => devices.Contains(d.DeviceId) && days.Contains(d.Day))
            .ToDictionaryAsync(d => (d.DeviceId, d.Day), cancellationToken)
            .ConfigureAwait(false);

        foreach (var position in positions)
        {
            var key = (position.DeviceId, DayOf(position.RecordedAt));
            var recordedAt = position.RecordedAt.UtcDateTime;
            if (!rows.TryGetValue(key, out var row))
            {
                row = new DeviceDailyDistance
                {
                    DeviceId = key.DeviceId,
                    Day = key.Item2,
                    FirstRecordedAt = recordedAt,
                    LastRecordedAt = recordedAt,
                };
                rows[key] = row;
                db.DailyDistances.Add(row);
            }

            row.DistanceMeters += position.DistanceMeters;
            row.Positions++;
            if (recordedAt < row.FirstRecordedAt)
            {
                row.FirstRecordedAt = recordedAt;
            }

            if (recordedAt > row.LastRecordedAt)
            {
                row.LastRecordedAt = recordedAt;
            }
        }
    }

    /// <summary>
    /// Pairs a transition with the matching opposite end of a visit. Transitions for one device and
    /// geofence strictly alternate in time, so an enter belongs with the earliest exit after it that
    /// has no enter yet, and an exit with the latest open visit entered before it.
    /// </summary>
    private async Task ApplyTransitionAsync(GeofenceEvent transition, CancellationToken cancellationToken)
    {
        var at = transition.OccurredAt.UtcDateTime;
        var visits = db.GeofenceVisits.Where(v => v.DeviceId == transition.DeviceId && v.GeofenceId == transition.GeofenceId);

        if (transition.Transition == GeofenceTransition.Enter)
        {
            var exitOnly = await FindAsync(
                    visits.Where(v => v.EnteredAt == null && v.ExitedAt > at).OrderBy(v => v.ExitedAt),
                    v => v.DeviceId == transition.DeviceId && v.GeofenceId == transition.GeofenceId && v.EnteredAt == null && v.ExitedAt > at,
                    candidates => candidates.OrderBy(v => v.ExitedAt),
                    cancellationToken)
                .ConfigureAwait(false);
            if (exitOnly is null)
            {
                db.GeofenceVisits.Add(NewVisit(transition, enteredAt: at, exitedAt: null));
                return;
            }

            exitOnly.EnteredAt = at;
            exitOnly.GeofenceName = transition.GeofenceName;
            RecordDwellIfLongEnough(exitOnly);
        }
        else
        {
            var open = await FindAsync(
                    visits.Where(v => v.ExitedAt == null && v.EnteredAt < at).OrderByDescending(v => v.EnteredAt),
                    v => v.DeviceId == transition.DeviceId && v.GeofenceId == transition.GeofenceId && v.ExitedAt == null && v.EnteredAt < at,
                    candidates => candidates.OrderByDescending(v => v.EnteredAt),
                    cancellationToken)
                .ConfigureAwait(false);
            if (open is null)
            {
                db.GeofenceVisits.Add(NewVisit(transition, enteredAt: null, exitedAt: at));
                return;
            }

            open.ExitedAt = at;
            RecordDwellIfLongEnough(open);
        }
    }

    /// <summary>
    /// Finds the best match among stored visits and visits added earlier in this batch (not saved yet).
    /// Stored rows are re-checked in memory because an earlier event in the batch may already have
    /// changed them.
    /// </summary>
    private async Task<GeofenceVisit?> FindAsync(
        IQueryable<GeofenceVisit> stored,
        Func<GeofenceVisit, bool> matches,
        Func<IEnumerable<GeofenceVisit>, IOrderedEnumerable<GeofenceVisit>> order,
        CancellationToken cancellationToken)
    {
        var candidates = db.ChangeTracker.Entries<GeofenceVisit>()
            .Where(e => e.State == EntityState.Added)
            .Select(e => e.Entity)
            .ToList();
        candidates.AddRange(await stored.Take(8).ToListAsync(cancellationToken).ConfigureAwait(false));

        return order(candidates.Where(matches).Distinct()).FirstOrDefault();
    }

    private void RecordDwellIfLongEnough(GeofenceVisit visit)
    {
        if (visit is not { EnteredAt: { } entered, ExitedAt: { } exited })
        {
            return;
        }

        var duration = exited - entered;
        if (duration < _dwellThreshold)
        {
            return;
        }

        db.DwellEvents.Add(new DwellEvent
        {
            DeviceId = visit.DeviceId,
            GeofenceId = visit.GeofenceId,
            GeofenceName = visit.GeofenceName,
            EnteredAt = entered,
            ExitedAt = exited,
            DurationSeconds = duration.TotalSeconds,
        });
    }

    private static GeofenceVisit NewVisit(GeofenceEvent transition, DateTime? enteredAt, DateTime? exitedAt) => new()
    {
        DeviceId = transition.DeviceId,
        GeofenceId = transition.GeofenceId,
        GeofenceName = transition.GeofenceName,
        EnteredAt = enteredAt,
        ExitedAt = exitedAt,
    };

    private static DateOnly DayOf(DateTimeOffset at) => DateOnly.FromDateTime(at.UtcDateTime);
}
