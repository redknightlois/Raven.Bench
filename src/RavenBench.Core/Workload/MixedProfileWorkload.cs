using RavenBench.Core;

namespace RavenBench.Core.Workload;

public sealed class MixedProfileWorkload : IWorkload
{
    private readonly WorkloadMix _mix;
    private readonly IKeyDistribution _distribution;
    private readonly int _docSizeBytes;

    // Keyspace starts at preload count and grows with inserts
    private long _maxKey;

    public MixedProfileWorkload(WorkloadMix mix, IKeyDistribution distribution, int docSizeBytes, long initialKeyspace = 0)
    {
        _mix = mix;
        _distribution = distribution;
        _docSizeBytes = docSizeBytes;
        _maxKey = initialKeyspace;
    }

    public OperationBase NextOperation(Random rng)
    {
        var p = rng.Next(0, 100);
        if (p < _mix.ReadPercent && _maxKey > 0)
        {
            var k = _distribution.NextKey(rng, (int)Math.Min(_maxKey, int.MaxValue));
            return new ReadOperation { Id = IdFor(k) };
        }
        if (p < _mix.ReadPercent + _mix.WritePercent)
        {
            var keyValue = Interlocked.Increment(ref _maxKey);
            var id = IdFor(keyValue);
            var payload = PayloadGenerator.Generate(_docSizeBytes, rng);
            return new InsertOperation<string> { Id = id, Payload = payload };
        }

        // Update; if no key yet, insert first
        if (_maxKey == 0)
        {
            var keyValue = Interlocked.Increment(ref _maxKey);
            var id = IdFor(keyValue);
            var payload = PayloadGenerator.Generate(_docSizeBytes, rng);
            return new InsertOperation<string> { Id = id, Payload = payload };
        }

        var id2 = IdFor(_distribution.NextKey(rng, (int)Math.Min(_maxKey, int.MaxValue)));
        var payload2 = PayloadGenerator.Generate(_docSizeBytes, rng);
        return new UpdateOperation<string> { Id = id2, Payload = payload2 };
    }

    public IWorkload? CreateWarmupWorkload(long preloadCount, IKeyDistribution distribution)
    {
        // MixedProfileWorkload includes writes/updates; for warmup, read from preloaded keyspace
        return preloadCount > 0 ? new ReadWorkload(distribution, preloadCount) : null;
    }

    private static string IdFor(long i) => $"bench/{i:D8}";
}
