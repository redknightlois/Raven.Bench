namespace RavenBench.Core.Workload;

/// <summary>
/// Workload that reads from a pre-sampled list of actual document IDs from the database.
/// Used during warmup to ensure we're accessing real database pages, not synthetic keys.
/// </summary>
public sealed class SampledKeysWorkload : IWorkload
{
    private readonly List<string> _sampledIds;

    public SampledKeysWorkload(List<string> sampledIds)
    {
        if (sampledIds == null || sampledIds.Count == 0)
            throw new ArgumentException("SampledKeysWorkload requires at least one document ID", nameof(sampledIds));

        _sampledIds = sampledIds;
    }

    public OperationBase NextOperation(Random rng)
    {
        // Randomly select from the sampled IDs
        var index = rng.Next(_sampledIds.Count);
        return new ReadOperation { Id = _sampledIds[index] };
    }

    public IWorkload? CreateWarmupWorkload(long preloadCount, IKeyDistribution distribution)
    {
        // This workload is already optimized for warmup (reads from actual keys)
        return null;
    }
}
