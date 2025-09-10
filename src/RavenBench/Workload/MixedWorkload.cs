using RavenBench.Util;

namespace RavenBench.Workload;

public sealed class MixedWorkload : IWorkload
{
    private readonly WorkloadMix _mix;
    private readonly IKeyDistribution _distribution;
    private readonly int _docSizeBytes;

    // Keyspace grows with preload; otherwise we keep inserting sequentially
    private int _maxKey = 0;

    public MixedWorkload(WorkloadMix mix, IKeyDistribution distribution, int docSizeBytes)
    {
        _mix = mix;
        _distribution = distribution;
        _docSizeBytes = docSizeBytes;
    }

    public Operation NextOperation(Random rng)
    {
        var p = rng.Next(0, 100);
        if (p < _mix.ReadPercent && _maxKey > 0)
        {
            var k = _distribution.NextKey(rng, _maxKey);
            return new Operation(OperationType.ReadById, IdFor(k), payload: null);
        }
        if (p < _mix.ReadPercent + _mix.WritePercent)
        {
            var id = IdFor(++_maxKey);
            var payload = PayloadGenerator.Generate(_docSizeBytes, rng);
            return new Operation(OperationType.Insert, id, payload);
        }
        
        // Update; if no key yet, insert first
        if (_maxKey == 0)
        {
            var id = IdFor(++_maxKey);
            var payload = PayloadGenerator.Generate(_docSizeBytes, rng);
            return new Operation(OperationType.Insert, id, payload);
        }

        var id2 = IdFor(_distribution.NextKey(rng, _maxKey));
        var payload2 = PayloadGenerator.Generate(_docSizeBytes, rng);
        return new Operation(OperationType.Update, id2, payload2);
    }

    private static string IdFor(int i) => $"bench/{i:D8}";
}

