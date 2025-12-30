using System;
using RavenBench.Core;

namespace RavenBench.Core.Workload;

/// <summary>
/// A read-only warmup workload that samples from preloaded keyspace.
/// Used for write/mixed workloads to ensure warmup only reads existing data,
/// accessing database pages without modifying the database.
/// </summary>
public sealed class WarmupWorkload : IWorkload
{
    private readonly IKeyDistribution _distribution;
    private readonly long _maxKey;
    private readonly string _keyPrefix;

    /// <summary>
    /// Creates a warmup workload that generates read operations from the preloaded keyspace.
    /// This ensures warmup accesses database pages without writing.
    /// </summary>
    /// <param name="distribution">Distribution for sampling keys (uniform, zipfian, latest)</param>
    /// <param name="maxKey">Maximum key value (typically the preload count)</param>
    /// <param name="keyPrefix">Key prefix for document IDs</param>
    public WarmupWorkload(
        IKeyDistribution distribution,
        long maxKey,
        string keyPrefix = "bench/")
    {
        _distribution = distribution ?? throw new ArgumentNullException(nameof(distribution));

        if (maxKey <= 0)
            throw new ArgumentException("maxKey must be > 0 for warmup", nameof(maxKey));

        _maxKey = maxKey;
        _keyPrefix = keyPrefix ?? throw new ArgumentNullException(nameof(keyPrefix));
    }

    /// <summary>
    /// Generates read operations from the preloaded keyspace using the configured distribution.
    /// </summary>
    public OperationBase NextOperation(Random rng)
    {
        var k = _distribution.NextKey(rng, (int)Math.Min(_maxKey, int.MaxValue));
        return new ReadOperation { Id = IdFor(k) };
    }

    public IWorkload? CreateWarmupWorkload(long preloadCount, IKeyDistribution distribution)
    {
        // WarmupWorkload is already a read-only warmup workload
        return null;
    }

    private string IdFor(long i) => $"{_keyPrefix}{i:D8}";
}
