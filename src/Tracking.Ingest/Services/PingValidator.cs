using Microsoft.Extensions.Options;
using Tracking.Core.Devices;

namespace Tracking.Ingest.Services;

/// <summary>Decides whether a ping is acceptable before it is written to the log.</summary>
public sealed class PingValidator(IOptions<IngestOptions> options, TimeProvider clock)
{
    private readonly IngestOptions _options = options.Value;

    /// <returns>Null when the ping is acceptable, otherwise the reason it is not.</returns>
    public string? Validate(LocationPing ping)
    {
        ArgumentNullException.ThrowIfNull(ping);

        if (!ping.Position.IsValid)
        {
            return "latitude must be in [-90, 90] and longitude in [-180, 180]";
        }

        if (ping.AccuracyMeters is { } accuracy && (!double.IsFinite(accuracy) || accuracy < 0))
        {
            return "accuracy_meters must be a non-negative number";
        }

        var now = clock.GetUtcNow();
        if (ping.RecordedAt > now + _options.MaxClockSkew)
        {
            return "recorded_at is in the future";
        }

        if (ping.RecordedAt < now - _options.MaxPingAge)
        {
            return "recorded_at is too old";
        }

        return null;
    }
}
