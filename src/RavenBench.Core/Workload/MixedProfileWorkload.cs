using RavenBench.Core;

namespace RavenBench.Core.Workload;

public sealed class MixedProfileWorkload : IWorkload
{
    private readonly WorkloadMix _mix;
    private readonly IKeyDistribution _distribution;
    private readonly int _docSizeBytes;
    private readonly int _seed;

    // Keyspace starts at preload count and grows with inserts
    private long _maxKey;

    public MixedProfileWorkload(WorkloadMix mix, IKeyDistribution distribution, int docSizeBytes, int seed, long initialKeyspace = 0)
    {
        _mix = mix;
        _distribution = distribution;
        _docSizeBytes = docSizeBytes;
        _seed = seed;
        _maxKey = initialKeyspace;
    }

    public OperationBase NextOperation(Random rng)
    {
        // The mix alone decides the operation kind: an empty or short keyspace never substitutes
        // another kind. A run that needs a non-empty keyspace checks it before the run starts.
        // Reads and updates may target keys from in-flight inserts; the overshoot is bounded by concurrency.
        var p = rng.Next(0, 100);
        var bound = (int)Math.Min(Volatile.Read(ref _maxKey), int.MaxValue);
        if (p < _mix.ReadPercent)
            return new ReadOperation { Id = BenchIds.IdFor(_distribution.NextKey(rng, bound)) };

        if (p < _mix.ReadPercent + _mix.WritePercent)
            return NextInsert();

        var id = BenchIds.IdFor(_distribution.NextKey(rng, bound));
        var fieldName = PayloadGenerator.FieldName(rng.Next(PayloadGenerator.FieldCount));
        // The replacement has the width of the field it replaces, so a long run of updates does
        // not drift the document size.
        var value = PayloadGenerator.GenerateFieldValue(PayloadGenerator.FieldWidth(_docSizeBytes), rng);
        return new UpdateFieldOperation { Id = id, FieldName = fieldName, Value = value };
    }

    private OperationBase NextInsert()
    {
        var keyValue = Interlocked.Increment(ref _maxKey);
        var id = BenchIds.IdFor(keyValue);
        var payload = PayloadGenerator.Generate(_seed, id, _docSizeBytes);
        return new InsertOperation<string> { Id = id, Payload = payload };
    }
}
