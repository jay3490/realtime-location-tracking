using Google.Protobuf.WellKnownTypes;
using Tracking.Core.Geo;
using DomainPing = Tracking.Core.Devices.LocationPing;
using DomainAlert = Tracking.Core.Geofencing.GeofenceAlert;
using DomainTransition = Tracking.Core.Geofencing.GeofenceTransition;

namespace Tracking.Contracts;

/// <summary>
/// Conversions between the wire contract and the domain model.
/// </summary>
public static class ContractMapping
{
    /// <summary>The request header that identifies the device on a location stream.</summary>
    public const string DeviceIdHeader = "x-device-id";

    public static DomainPing ToDomain(this V1.LocationPing ping, string deviceId, DateTimeOffset receivedAt)
    {
        ArgumentNullException.ThrowIfNull(ping);

        var recordedAt = ping.RecordedAt is null ? receivedAt : ping.RecordedAt.ToDateTimeOffset();
        return new DomainPing(
            deviceId,
            new GeoPoint(ping.Latitude, ping.Longitude),
            recordedAt,
            ping.HasAccuracyMeters ? ping.AccuracyMeters : null);
    }

    public static V1.GeofenceAlert ToContract(this DomainAlert alert)
    {
        ArgumentNullException.ThrowIfNull(alert);

        return new V1.GeofenceAlert
        {
            DeviceId = alert.DeviceId,
            GeofenceId = alert.GeofenceId,
            GeofenceName = alert.GeofenceName,
            Transition = alert.Transition switch
            {
                DomainTransition.Enter => V1.GeofenceTransition.Enter,
                DomainTransition.Exit => V1.GeofenceTransition.Exit,
                _ => V1.GeofenceTransition.Unspecified,
            },
            Latitude = alert.Position.Latitude,
            Longitude = alert.Position.Longitude,
            OccurredAt = Timestamp.FromDateTimeOffset(alert.OccurredAt),
        };
    }

    public static V1.LocationPing ToContract(this DomainPing ping, ulong sequence)
    {
        ArgumentNullException.ThrowIfNull(ping);

        var message = new V1.LocationPing
        {
            Sequence = sequence,
            Latitude = ping.Position.Latitude,
            Longitude = ping.Position.Longitude,
            RecordedAt = Timestamp.FromDateTimeOffset(ping.RecordedAt),
        };
        if (ping.AccuracyMeters is { } accuracy)
        {
            message.AccuracyMeters = accuracy;
        }

        return message;
    }
}
