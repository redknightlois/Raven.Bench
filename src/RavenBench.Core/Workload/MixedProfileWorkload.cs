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
        // Reads and updates may target keys from in-flight inserts; the overshoot is bounded by concurrency.
        var p = rng.Next(0, 100);
        long maxKey = Volatile.Read(ref _maxKey);
        if (p < _mix.ReadPercent && maxKey > 0)
        {
            var k = _distribution.NextKey(rng, (int)Math.Min(maxKey, int.MaxValue));
            return new ReadOperation { Id = BenchIds.IdFor(k) };
        }
        if (p < _mix.ReadPercent + _mix.WritePercent)
        {
            var keyValue = Interlocked.Increment(ref _maxKey);
            var id = BenchIds.IdFor(keyValue);
            var payload = PayloadGenerator.Generate(_docSizeBytes, rng);
            return new InsertOperation<string> { Id = id, Payload = payload };
        }

        if (maxKey == 0)
        {
            var keyValue = Interlocked.Increment(ref _maxKey);
            var id = BenchIds.IdFor(keyValue);
            var payload = PayloadGenerator.Generate(_docSizeBytes, rng);
            return new InsertOperation<string> { Id = id, Payload = payload };
        }

        var id2 = BenchIds.IdFor(_distribution.NextKey(rng, (int)Math.Min(maxKey, int.MaxValue)));
        var payload2 = PayloadGenerator.Generate(_docSizeBytes, rng);
        return new UpdateOperation<string> { Id = id2, Payload = payload2 };
    }
}
