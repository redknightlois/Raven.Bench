using RavenBench.Core;

namespace RavenBench.Core.Workload;

public sealed class WriteWorkload : IWorkload
{
    private readonly int _docSizeBytes;
    private readonly PayloadKind _payload;
    private long _maxKey;

    public WriteWorkload(int docSizeBytes, long startingKey = 0, PayloadKind payload = PayloadKind.Json)
    {
        _docSizeBytes = docSizeBytes;
        _payload = payload;
        _maxKey = startingKey;
    }

    public OperationBase NextOperation(Random rng)
    {
        var keyValue = Interlocked.Increment(ref _maxKey);
        var id = BenchIds.IdFor(keyValue);
        return PayloadGenerator.InsertOperationFor(_payload, _docSizeBytes, rng, id);
    }
}

