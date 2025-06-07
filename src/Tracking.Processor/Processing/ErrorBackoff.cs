namespace Tracking.Processor.Processing;

/// <summary>
/// Exponential back-off with jitter for consecutive failures: the n-th delay is drawn uniformly
/// from the upper half of <c>min(max, initial * 2^n)</c>, so a fleet of processors that lost Redis at
/// the same moment does not hammer it in lockstep when it returns.
/// </summary>
public sealed class ErrorBackoff(TimeSpan initial, TimeSpan max, Random random)
{
    private int _failures;

    public int ConsecutiveFailures => _failures;

    /// <summary>Records a failure and returns how long to wait before trying again.</summary>
    public TimeSpan NextDelay()
    {
        var exponent = Math.Min(_failures, 30);
        _failures++;
        var ceiling = Math.Min(max.Ticks, initial.Ticks * Math.Pow(2, exponent));
        var jittered = (ceiling / 2) + (random.NextDouble() * ceiling / 2);
        return TimeSpan.FromTicks((long)jittered);
    }

    public void Reset() => _failures = 0;
}
