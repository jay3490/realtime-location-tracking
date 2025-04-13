using Tracking.Core.Devices;

namespace Tracking.Core.Tests.Devices;

public class DeviceIdTests
{
    [Theory]
    [InlineData("truck-17", true)]
    [InlineData("fleet:eu.van_3", true)]
    [InlineData("", false)]
    [InlineData(null, false)]
    [InlineData("has space", false)]
    [InlineData("star*", false)]
    [InlineData("tracking:device:x}", false)]
    public void Only_a_key_safe_alphabet_is_accepted(string? id, bool expected)
    {
        Assert.Equal(expected, DeviceId.IsValid(id));
    }

    [Fact]
    public void Length_is_capped()
    {
        Assert.True(DeviceId.IsValid(new string('a', DeviceId.MaxLength)));
        Assert.False(DeviceId.IsValid(new string('a', DeviceId.MaxLength + 1)));
    }
}
