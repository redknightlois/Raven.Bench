using RavenBench.Core;

namespace RavenBench.Core.Workload;

public sealed class WriteWorkload : IWorkload
{
    private readonly int _docSizeBytes;
    private readonly int _seed;
    private readonly PayloadKind _payload;
    private long _maxKey;

    public WriteWorkload(int docSizeBytes, int seed, long startingKey = 0, PayloadKind payload = PayloadKind.Json)
    {
        _docSizeBytes = docSizeBytes;
        _seed = seed;
        _payload = payload;
        _maxKey = startingKey;
    }

    /// <summary>
    /// The highest key this workload has published. A later insert stream starts above it, so two
    /// insert streams of one invocation never address the same id.
    /// </summary>
    public long HighestKeyIssued => Interlocked.Read(ref _maxKey);

    public OperationBase NextOperation(Random rng)
    {
        var keyValue = Interlocked.Increment(ref _maxKey);
        var id = BenchIds.IdFor(keyValue);
        return PayloadGenerator.InsertOperationFor(_payload, _seed, id, _docSizeBytes);
    }
}
