using RavenBench.Core;

namespace RavenBench.Core.Workload;

/// <summary>
/// Bulk insert workload that produces exactly <c>targetCount</c> documents, splitting them into
/// batches of at most <c>batchSize</c> and clamping the last batch. The ycsb load run uses it so
/// the keyspace holds the scenario's document count and no more.
/// </summary>
public sealed class BulkWriteWorkload : IWorkload
{
    private readonly int _docSizeBytes;
    private readonly int _batchSize;
    private readonly int _seed;
    private readonly long _targetCount;
    private long _maxKey;
    private long _produced;

    public BulkWriteWorkload(int docSizeBytes, int batchSize, int seed, long targetCount, long startingKey = 0)
    {
        _docSizeBytes = docSizeBytes;
        _batchSize = batchSize;
        _seed = seed;
        _targetCount = targetCount;
        _maxKey = startingKey;
    }

    public bool IsExhausted => Volatile.Read(ref _produced) >= _targetCount;

    public OperationBase NextOperation(Random rng)
    {
        var count = ReserveBatch();
        var documents = new List<DocumentToWrite<string>>(count);
        for (int i = 0; i < count; i++)
        {
            var keyValue = Interlocked.Increment(ref _maxKey);
            var id = BenchIds.IdFor(keyValue);
            var payload = PayloadGenerator.Generate(_seed, id, _docSizeBytes);
            documents.Add(new DocumentToWrite<string> { Id = id, Document = payload });
        }

        return new BulkInsertOperation<string> { Documents = documents };
    }

    /// <summary>
    /// Claims the next batch of ids by advancing the produced count atomically, so concurrent
    /// callers never claim the same batch or exceed the target.
    /// </summary>
    private int ReserveBatch()
    {
        while (true)
        {
            var produced = Volatile.Read(ref _produced);
            if (produced >= _targetCount)
                throw new InvalidOperationException($"The bulk write workload has produced all {_targetCount} of its documents.");

            var count = (int)Math.Min(_batchSize, _targetCount - produced);
            if (Interlocked.CompareExchange(ref _produced, produced + count, produced) == produced)
                return count;
        }
    }
}
