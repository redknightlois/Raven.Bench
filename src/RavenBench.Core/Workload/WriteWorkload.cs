using RavenBench.Core;

namespace RavenBench.Core.Workload;

public sealed class WriteWorkload : IWorkload
{
    private readonly int _docSizeBytes;
    private readonly int _seed;
    private long _maxKey;

    public WriteWorkload(int docSizeBytes, int seed, long startingKey = 0)
    {
        _docSizeBytes = docSizeBytes;
        _seed = seed;
        _maxKey = startingKey;
    }

    public OperationBase NextOperation(Random rng)
    {
        var keyValue = Interlocked.Increment(ref _maxKey);
        var id = BenchIds.IdFor(keyValue);
        var payload = PayloadGenerator.Generate(_seed, id, _docSizeBytes);
        return new InsertOperation<string> { Id = id, Payload = payload };
    }
}
