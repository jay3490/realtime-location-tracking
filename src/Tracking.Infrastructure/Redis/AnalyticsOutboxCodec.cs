using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using StackExchange.Redis;
using Tracking.Core;
using Tracking.Core.Analytics;

namespace Tracking.Infrastructure.Redis;

/// <summary>
/// The layout of an analytics outbox entry: the event kind, the device id (the Kafka message key),
/// the event id, and the event itself as JSON, exactly as it will be published.
/// </summary>
public static class AnalyticsOutboxCodec
{
    public const string KindField = "kind";
    public const string DeviceField = "device";
    public const string EventIdField = "id";
    public const string PayloadField = "payload";

    public static NameValueEntry[] Encode(AnalyticsEvent analyticsEvent)
    {
        ArgumentNullException.ThrowIfNull(analyticsEvent);
        return
        [
            new(KindField, analyticsEvent.Kind.ToString()),
            new(DeviceField, analyticsEvent.DeviceId),
            new(EventIdField, analyticsEvent.EventId),
            new(PayloadField, JsonSerializer.Serialize(analyticsEvent, analyticsEvent.GetType(), TrackingJson.Options)),
        ];
    }

    public static bool TryDecode(
        IReadOnlyDictionary<string, string> fields,
        out AnalyticsEventKind kind,
        [NotNullWhen(true)] out string? deviceId,
        [NotNullWhen(true)] out string? eventId,
        [NotNullWhen(true)] out string? payload)
    {
        ArgumentNullException.ThrowIfNull(fields);
        kind = default;
        deviceId = eventId = payload = null;

        return fields.TryGetValue(KindField, out var kindText) &&
               Enum.TryParse(kindText, ignoreCase: false, out kind) &&
               Enum.IsDefined(kind) &&
               fields.TryGetValue(DeviceField, out deviceId) && !string.IsNullOrEmpty(deviceId) &&
               fields.TryGetValue(EventIdField, out eventId) && !string.IsNullOrEmpty(eventId) &&
               fields.TryGetValue(PayloadField, out payload) && !string.IsNullOrEmpty(payload);
    }
}
