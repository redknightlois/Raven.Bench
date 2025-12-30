
using RavenBench.Core;

namespace RavenBench.Core.Workload;

public sealed class BulkWriteWorkload : IWorkload
{
    private readonly int _docSizeBytes;
    private readonly int _batchSize;
    private long _maxKey;

    public BulkWriteWorkload(int docSizeBytes, int batchSize, long startingKey = 0)
    {
        _docSizeBytes = docSizeBytes;
        _batchSize = batchSize;
        _maxKey = startingKey;
    }

    public OperationBase NextOperation(Random rng)
    {
        var documents = new List<DocumentToWrite<string>>();
        for (int i = 0; i < _batchSize; i++)
        {
            var keyValue = Interlocked.Increment(ref _maxKey);
            var id = IdFor(keyValue);
            var payload = PayloadGenerator.Generate(_docSizeBytes, rng);
            documents.Add(new DocumentToWrite<string> { Id = id, Document = payload });
        }

        return new BulkInsertOperation<string> { Documents = documents };
    }

    public IWorkload? CreateWarmupWorkload(long preloadCount, IKeyDistribution distribution)
    {
        // BulkWriteWorkload inserts batches; for warmup, read from preloaded keyspace
        // Note: Warmup may not make sense for pure bulk write benchmarks with no preload
        return preloadCount > 0 ? new ReadWorkload(distribution, preloadCount) : null;
    }

    private static string IdFor(long i) => $"bench/{i:D8}";
}
