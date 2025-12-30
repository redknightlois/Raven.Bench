using RavenBench.Core;

namespace RavenBench.Core.Workload;

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

    public IWorkload? CreateWarmupWorkload(long preloadCount, IKeyDistribution distribution)
    {
        // WriteWorkload inserts new documents; for warmup, read from preloaded keyspace
        return preloadCount > 0 ? new ReadWorkload(distribution, preloadCount) : null;
    }

    private static string IdFor(long i) => $"bench/{i:D8}";
}
