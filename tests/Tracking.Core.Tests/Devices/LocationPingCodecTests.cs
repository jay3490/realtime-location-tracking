using Tracking.Core.Devices;
using Tracking.Core.Geo;

namespace Tracking.Core.Tests.Devices;

public class LocationPingCodecTests
{
    private static Dictionary<string, string> Fields(LocationPing ping) =>
        LocationPingCodec.Encode(ping).ToDictionary(kv => kv.Key, kv => kv.Value);

    [Fact]
    public void Encoded_pings_decode_to_the_same_value()
    {
        var ping = new LocationPing("truck-7", new GeoPoint(-33.868820123456789, 151.209295987654321),
            DateTimeOffset.FromUnixTimeMilliseconds(1_767_225_600_123), AccuracyMeters: 4.5);

        Assert.True(LocationPingCodec.TryDecode(Fields(ping), out var decoded, out var error));
        Assert.Null(error);
        Assert.Equal(ping, decoded);
    }

    [Fact]
    public void Accuracy_is_optional()
    {
        var ping = new LocationPing("d", new GeoPoint(1, 2), DateTimeOffset.FromUnixTimeMilliseconds(5));
        var fields = Fields(ping);

        Assert.DoesNotContain(LocationPingCodec.AccuracyField, fields.Keys);
        Assert.True(LocationPingCodec.TryDecode(fields, out var decoded, out _));
        Assert.Null(decoded.AccuracyMeters);
    }

    [Theory]
    [InlineData("device")]
    [InlineData("lat")]
    [InlineData("lon")]
    [InlineData("ts")]
    public void Missing_required_fields_are_reported(string field)
    {
        var fields = Fields(new LocationPing("d", new GeoPoint(1, 2), DateTimeOffset.UnixEpoch));
        fields.Remove(field);

        Assert.False(LocationPingCodec.TryDecode(fields, out var ping, out var error));
        Assert.Null(ping);
        Assert.Contains(field, error, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("lat", "north")]
    [InlineData("lat", "95")]
    [InlineData("lon", "NaN")]
    [InlineData("ts", "-1")]
    [InlineData("ts", "12.5")]
    public void Malformed_values_are_rejected(string field, string value)
    {
        var fields = Fields(new LocationPing("d", new GeoPoint(1, 2), DateTimeOffset.UnixEpoch));
        fields[field] = value;

        Assert.False(LocationPingCodec.TryDecode(fields, out _, out var error));
        Assert.False(string.IsNullOrEmpty(error));
    }
}
