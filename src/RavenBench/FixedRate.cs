namespace RavenBench;

/// <summary>The rate a fixed-rate step targets after a closed loop found the ceiling, for every runner that pairs the two.</summary>
public static class FixedRate
{
    /// <summary>The share of the closed-loop ceiling the fixed-rate step targets. Below 1, so the fixed-rate latency is that of a server with headroom, not of a saturated queue.</summary>
    public const double Fraction = 0.8;

    /// <summary>The whole rate below the ceiling, or null when the closed loop held too little for one: the run then has no fixed-rate step.</summary>
    internal static int? For(double closedThroughput)
    {
        var rate = (int)Math.Floor(closedThroughput * Fraction);
        return rate < 1 ? null : rate;
    }
}
