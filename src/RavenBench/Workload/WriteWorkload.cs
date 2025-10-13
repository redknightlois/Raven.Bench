using RavenBench.Util;

namespace RavenBench.Workload;

public sealed class WriteWorkload : IWorkload
{
    private readonly int _docSizeBytes;
    private long _maxKey;

    public WriteWorkload(int docSizeBytes, long startingKey = 0)
    {
        _docSizeBytes = docSizeBytes;
        _maxKey = startingKey;
    }

    public OperationBase NextOperation(Random rng)
    {
        var keyValue = Interlocked.Increment(ref _maxKey);
        var id = IdFor(keyValue);
        var payload = PayloadGenerator.Generate(_docSizeBytes, rng);
        return new InsertOperation<string> { Id = id, Payload = payload };
    }

    private static string IdFor(long i) => $"bench/{i:D8}";
}

