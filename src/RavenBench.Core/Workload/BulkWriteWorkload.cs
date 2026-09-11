
using RavenBench.Core;

namespace RavenBench.Core.Workload;

public sealed class BulkWriteWorkload : IWorkload
{
    private readonly int _docSizeBytes;
    private readonly int _batchSize;
    private readonly int _seed;
    private readonly PayloadKind _payload;
    private long _maxKey;

    public BulkWriteWorkload(int docSizeBytes, int batchSize, int seed, long startingKey = 0, PayloadKind payload = PayloadKind.Json)
    {
        _docSizeBytes = docSizeBytes;
        _batchSize = batchSize;
        _seed = seed;
        _payload = payload;
        _maxKey = startingKey;
    }

    public OperationBase NextOperation(Random rng)
    {
        var ids = new string[_batchSize];
        for (int i = 0; i < _batchSize; i++)
            ids[i] = BenchIds.IdFor(Interlocked.Increment(ref _maxKey));

        if (_payload == PayloadKind.Entity)
        {
            return new BulkInsertOperation<YcsbRecord>
            {
                Documents = ids.Select(id => new DocumentToWrite<YcsbRecord> { Id = id, Document = PayloadGenerator.GenerateRecord(_seed, id, _docSizeBytes) }).ToList()
            };
        }

        return new BulkInsertOperation<string>
        {
            Documents = ids.Select(id => new DocumentToWrite<string> { Id = id, Document = PayloadGenerator.Generate(_seed, id, _docSizeBytes) }).ToList()
        };
    }
}
