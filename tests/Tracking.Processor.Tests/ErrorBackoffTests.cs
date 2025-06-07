using Tracking.Processor.Processing;

namespace Tracking.Processor.Tests;

public sealed class ErrorBackoffTests
{
    [Fact]
    public void Delays_double_within_the_jitter_band_and_stop_at_the_cap()
    {
        var backoff = new ErrorBackoff(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(10), new Random(7));

        var delays = Enumerable.Range(0, 8).Select(_ => backoff.NextDelay().TotalSeconds).ToArray();

        double[] ceilings = [1, 2, 4, 8, 10, 10, 10, 10];
        for (var i = 0; i < delays.Length; i++)
        {
            Assert.InRange(delays[i], ceilings[i] / 2, ceilings[i]);
        }

        Assert.Equal(8, backoff.ConsecutiveFailures);
    }

    [Fact]
    public void Reset_starts_over_from_the_initial_delay()
    {
        var backoff = new ErrorBackoff(TimeSpan.FromMilliseconds(100), TimeSpan.FromSeconds(30), new Random(1));
        for (var i = 0; i < 10; i++)
        {
            backoff.NextDelay();
        }

        backoff.Reset();

        Assert.InRange(backoff.NextDelay().TotalMilliseconds, 50, 100);
    }

    [Fact]
    public void Many_failures_do_not_overflow()
    {
        var backoff = new ErrorBackoff(TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(1), new Random(3));
        var delays = Enumerable.Range(0, 200).Select(_ => backoff.NextDelay()).ToArray();

        Assert.All(delays.Skip(10), d => Assert.InRange(d, TimeSpan.FromSeconds(30), TimeSpan.FromMinutes(1)));
    }
}
