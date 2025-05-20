using Tracking.Core.Devices;

namespace Tracking.Core.Tests.Devices;

public sealed class StreamEntryIdTests
{
    [Theory]
    [InlineData("1767225600000-0")]
    [InlineData("1767225600000-17")]
    [InlineData("1767225600000")]
    public void The_millisecond_part_is_the_append_time(string id)
    {
        Assert.True(StreamEntryId.TryGetTimestamp(id, out var appendedAt));
        Assert.Equal(new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero), appendedAt);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("abc-0")]
    [InlineData("-5-0")]
    [InlineData("999999999999999999-0")]
    public void Malformed_ids_have_no_timestamp(string? id)
    {
        Assert.False(StreamEntryId.TryGetTimestamp(id, out _));
    }
}
