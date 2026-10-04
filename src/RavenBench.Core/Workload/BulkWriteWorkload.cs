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
    private readonly PayloadKind _payload;
    private readonly long _targetCount;
    private long _maxKey;
    private long _produced;

    public BulkWriteWorkload(int docSizeBytes, int batchSize, int seed, long targetCount, long startingKey = 0, PayloadKind payload = PayloadKind.Json)
    {
        _docSizeBytes = docSizeBytes;
        _batchSize = batchSize;
        _seed = seed;
        _payload = payload;
        _targetCount = targetCount;
        _maxKey = startingKey;
    }

    public bool IsExhausted => Volatile.Read(ref _produced) >= _targetCount;

    public OperationBase NextOperation(Random rng)
    {
        var count = ReserveBatch();
        var ids = new string[count];
        for (int i = 0; i < count; i++)
            ids[i] = BenchIds.IdFor(Interlocked.Increment(ref _maxKey));

        // The ids are reserved here; the payloads are built by whoever first reads the documents.
        if (_payload == PayloadKind.Entity)
        {
            return new BulkInsertOperation<YcsbRecord>(count,
                () => ids.Select(id => new DocumentToWrite<YcsbRecord> { Id = id, Document = PayloadGenerator.GenerateRecord(_seed, id, _docSizeBytes) }).ToList());
        }

        return new BulkInsertOperation<string>(count,
            () => ids.Select(id => new DocumentToWrite<string> { Id = id, Document = PayloadGenerator.Generate(_seed, id, _docSizeBytes) }).ToList());
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
