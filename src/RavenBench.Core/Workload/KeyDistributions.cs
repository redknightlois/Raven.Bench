namespace RavenBench.Core.Workload;

public interface IKeyDistribution
{
    int NextKey(Random rng, int maxKeyInclusive);
}

/// <summary>The one rule that names a key distribution and builds it; the CLI, the ycsb scenario and every runner use it.</summary>
public static class KeyDistributions
{
    public const string ValidNames = "uniform, zipfian, latest";

    /// <summary>Parses a distribution name, ignoring case and surrounding whitespace. A number is not a name.</summary>
    public static bool TryParse(string? name, out KeyDistributionKind kind)
    {
        switch (name?.Trim().ToLowerInvariant())
        {
            case "uniform": kind = KeyDistributionKind.Uniform; return true;
            case "zipfian": kind = KeyDistributionKind.Zipfian; return true;
            case "latest": kind = KeyDistributionKind.Latest; return true;
            default: kind = default; return false;
        }
    }

    public static IKeyDistribution Create(KeyDistributionKind kind) => kind switch
    {
        KeyDistributionKind.Uniform => new UniformDistribution(),
        KeyDistributionKind.Zipfian => new ZipfianDistribution(),
        KeyDistributionKind.Latest => new LatestDistribution(),
        _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, null)
    };
}

/// <summary>
/// Uniform distribution where all keys have equal probability of selection.
/// </summary>
public sealed class UniformDistribution : IKeyDistribution
{
    public int NextKey(Random rng, int maxKeyInclusive)
    {
        if (maxKeyInclusive <= 0) return 1;
        maxKeyInclusive = Math.Min(maxKeyInclusive, int.MaxValue - 1);
        return rng.Next(1, maxKeyInclusive + 1);
    }
}

/// <summary>
/// Bounded Zipfian distribution (Gray et al., YCSB-style) over popularity ranks, with each rank
/// mapped to a key by a fixed permutation of 1..n. The hottest keys are therefore spread over the
/// keyspace instead of packed onto ids 1, 2, 3, and the k-th hottest key keeps the k-th rank's frequency.
/// </summary>
public sealed class ZipfianDistribution : IKeyDistribution
{
    // A prime above int.MaxValue is coprime to every keyspace size, so rank * Spread mod n is a bijection of 1..n.
    private const long Spread = 2_654_435_761;

    private readonly double _theta;
    private readonly double _zeta2;
    private readonly object _sync = new();

    // Zeta of the last keyspace size, moved term by term to the next size in either direction,
    // so a keyspace that changes by a few keys costs a few terms.
    private long _zetaN;
    private double _zetan;

    /// <summary>Zeta terms added or removed so far.</summary>
    internal long ZetaTermsComputed { get; private set; }

    public ZipfianDistribution(double theta = 0.99)
    {
        _theta = theta;
        _zeta2 = 1.0 + Math.Pow(0.5, theta);
    }

    public int NextKey(Random rng, int maxKeyInclusive)
    {
        if (maxKeyInclusive <= 1) return 1;
        int n = Math.Min(maxKeyInclusive, int.MaxValue - 1);
        return (int)(NextRank(rng, n) * Spread % n) + 1;
    }

    private long NextRank(Random rng, int n)
    {
        double zetan = GetZetan(n);

        double alpha = 1.0 / (1.0 - _theta);
        double eta = (1.0 - Math.Pow(2.0 / n, 1.0 - _theta)) / (1.0 - _zeta2 / zetan);

        double u = rng.NextDouble();
        double uz = u * zetan;
        if (uz < 1.0) return 1;
        if (uz < _zeta2) return 2;

        int rank = 1 + (int)(n * Math.Pow(eta * u - eta + 1.0, alpha));
        return Math.Clamp(rank, 1, n);
    }

    private double GetZetan(long n)
    {
        lock (_sync)
        {
            // Re-summing from zero is cheaper than walking down more terms than the new size holds.
            if (_zetaN - n > n)
                (_zetaN, _zetan) = (0, 0.0);

            for (; _zetaN < n; _zetaN++, ZetaTermsComputed++)
                _zetan += 1.0 / Math.Pow(_zetaN + 1, _theta);
            for (; _zetaN > n; _zetaN--, ZetaTermsComputed++)
                _zetan -= 1.0 / Math.Pow(_zetaN, _theta);
            return _zetan;
        }
    }
}

/// <summary>
/// Latest distribution: 80% of samples hit the most recent 20% of the keyspace, the rest are uniform.
/// </summary>
public sealed class LatestDistribution : IKeyDistribution
{
    public int NextKey(Random rng, int maxKeyInclusive)
    {
        if (maxKeyInclusive <= 1) return 1;
        maxKeyInclusive = Math.Min(maxKeyInclusive, int.MaxValue - 1);

        if (rng.NextDouble() < 0.8)
        {
            int hotRange = Math.Max(1, (int)(maxKeyInclusive * 0.2));
            int hotStart = Math.Max(1, maxKeyInclusive - hotRange + 1);
            return rng.Next(hotStart, maxKeyInclusive + 1);
        }

        return rng.Next(1, maxKeyInclusive + 1);
    }
}
