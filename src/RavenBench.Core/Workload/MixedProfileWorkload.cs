using RavenBench.Core;

namespace RavenBench.Core.Workload;

public sealed class MixedProfileWorkload : IWorkload
{
    private readonly WorkloadMix _mix;
    private readonly IKeyDistribution _distribution;
    private readonly int _docSizeBytes;
    private readonly PayloadKind _payload;

    // Keyspace starts at preload count and grows with inserts
    private long _maxKey;

    public MixedProfileWorkload(WorkloadMix mix, IKeyDistribution distribution, int docSizeBytes, long initialKeyspace = 0, PayloadKind payload = PayloadKind.Json)
    {
        _mix = mix;
        _distribution = distribution;
        _docSizeBytes = docSizeBytes;
        _payload = payload;
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
        if (p < _mix.ReadPercent + _mix.WritePercent || maxKey == 0)
        {
            var keyValue = Interlocked.Increment(ref _maxKey);
            return PayloadGenerator.InsertOperationFor(_payload, _docSizeBytes, rng, BenchIds.IdFor(keyValue));
        }

        var updateId = BenchIds.IdFor(_distribution.NextKey(rng, (int)Math.Min(maxKey, int.MaxValue)));
        return PayloadGenerator.UpdateOperationFor(_payload, _docSizeBytes, rng, updateId);
    }
}
