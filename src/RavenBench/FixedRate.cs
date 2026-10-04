using System.Globalization;

namespace RavenBench;

/// <summary>The rate a fixed-rate step targets after a closed loop found the ceiling, for every runner that pairs the two.</summary>
public static class FixedRate
{
    /// <summary>The share of the closed-loop ceiling the fixed-rate step targets. Below 1, so the fixed-rate latency is that of a server with headroom, not of a saturated queue.</summary>
    public const double Fraction = 0.8;

    /// <exception cref="InvalidOperationException">The closed loop held too little throughput for a whole rate below it.</exception>
    internal static int For(string run, double closedThroughput)
    {
        var rate = (int)Math.Floor(closedThroughput * Fraction);
        if (rate < 1)
            throw new InvalidOperationException($"The {run} closed loop held {closedThroughput.ToString("F2", CultureInfo.InvariantCulture)} q/s, too little for a whole fixed rate below it.");
        return rate;
    }
}
