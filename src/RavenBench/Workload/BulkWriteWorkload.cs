
using RavenBench.Util;

namespace RavenBench.Workload;

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

    private static string IdFor(long i) => $"bench/{i:D8}";
}
