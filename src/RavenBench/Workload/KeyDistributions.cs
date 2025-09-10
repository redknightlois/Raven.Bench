namespace RavenBench.Workload;

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
        return rng.Next(1, maxKeyInclusive + 1);
    }
}

/// <summary>
/// Zipfian distribution where lower keys are accessed more frequently than higher keys.
/// Models real-world access patterns where some items are much more popular than others.
/// </summary>
public sealed class ZipfianDistribution : IKeyDistribution
{
    private readonly double _theta;

    public ZipfianDistribution(double theta = 0.99)
    {
        _theta = theta;
    }

    public int NextKey(Random rng, int maxKeyInclusive)
    {
        if (maxKeyInclusive <= 1) return 1;
        
        // Use rejection sampling for Zipfian distribution
        // This is a simplified implementation for benchmark purposes
        double u = rng.NextDouble();
        double x = Math.Pow(1.0 - u, -1.0 / _theta);
        int key = Math.Min((int)Math.Ceiling(x), maxKeyInclusive);
        return Math.Max(1, key);
    }
}

/// <summary>
/// Latest distribution where recently inserted keys are more likely to be accessed.
/// Models temporal locality in access patterns.
/// </summary>
public sealed class LatestDistribution : IKeyDistribution
{
    private readonly double _hotPortion;

    public LatestDistribution(double hotPortion = 0.2)
    {
        _hotPortion = Math.Clamp(hotPortion, 0.01, 1.0);
    }

    public int NextKey(Random rng, int maxKeyInclusive)
    {
        if (maxKeyInclusive <= 1) return 1;
        
        // 80% chance to access the "hot" recent portion of keys
        if (rng.NextDouble() < 0.8)
        {
            int hotRange = Math.Max(1, (int)(maxKeyInclusive * _hotPortion));
            int hotStart = Math.Max(1, maxKeyInclusive - hotRange + 1);
            return rng.Next(hotStart, maxKeyInclusive + 1);
        }
        else
        {
            // 20% chance to access any key uniformly
            return rng.Next(1, maxKeyInclusive + 1);
        }
    }
}

