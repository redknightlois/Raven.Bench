namespace RavenBench.Core.Workload;

public interface IKeyDistribution
{
    int NextKey(Random rng, int maxKeyInclusive);
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
/// Bounded Zipfian distribution (Gray et al., YCSB-style); key 1 is the hottest.
/// </summary>
public sealed class ZipfianDistribution : IKeyDistribution
{
    private readonly double _theta;
    private readonly double _zeta2;
    private readonly object _sync = new();

    // Zeta cache extends incrementally as the keyspace grows: O(1) amortized per sample.
    private long _zetaN;
    private double _zetan;

    public ZipfianDistribution(double theta = 0.99)
    {
        _theta = theta;
        _zeta2 = 1.0 + Math.Pow(0.5, theta);
    }

    public int NextKey(Random rng, int maxKeyInclusive)
    {
        if (maxKeyInclusive <= 1) return 1;
        int n = Math.Min(maxKeyInclusive, int.MaxValue - 1);
        double zetan = GetZetan(n);

        double alpha = 1.0 / (1.0 - _theta);
        double eta = (1.0 - Math.Pow(2.0 / n, 1.0 - _theta)) / (1.0 - _zeta2 / zetan);

        double u = rng.NextDouble();
        double uz = u * zetan;
        if (uz < 1.0) return 1;
        if (uz < _zeta2) return 2;

        int key = 1 + (int)(n * Math.Pow(eta * u - eta + 1.0, alpha));
        return Math.Clamp(key, 1, n);
    }

    private double GetZetan(long n)
    {
        lock (_sync)
        {
            if (n < _zetaN)
            {
                _zetaN = 0;
                _zetan = 0.0;
            }

            for (long i = _zetaN + 1; i <= n; i++)
                _zetan += 1.0 / Math.Pow(i, _theta);
            _zetaN = n;
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
