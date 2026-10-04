namespace RavenBench.Core.Aggregate;

/// <summary>The documents and the aggregate indexes of one product, as a run that loads aggregate documents adds and removes them.</summary>
public interface IAggregateStore
{
    /// <summary>The names of the indexes the run creates on this product.</summary>
    IReadOnlyList<string> AggregateIndexNames { get; }

    /// <summary>The ids among <paramref name="ids"/> that the product holds; a missing database or collection holds none.</summary>
    Task<IReadOnlyCollection<string>> ExistingIdsAsync(IReadOnlyList<string> ids, CancellationToken ct);

    /// <summary>The names of every index the product holds for the aggregate documents.</summary>
    Task<IReadOnlyCollection<string>> IndexNamesAsync(CancellationToken ct);

    /// <summary>Deletes the documents with these ids; an id the product does not hold is skipped.</summary>
    Task DeleteAsync(IReadOnlyList<string> ids, CancellationToken ct);

    Task DeleteIndexAsync(string name, CancellationToken ct);
}

/// <summary>
/// What one run added to a store: the ids it loads and the aggregate indexes that did not exist before it.
/// Removing the footprint removes exactly that, so documents and indexes that existed before the run stay.
/// </summary>
public sealed class AggregateFootprint
{
    private const int BatchSize = 500;

    private readonly IAggregateStore _store;
    private readonly IEnumerable<string> _ids;
    private readonly IReadOnlyList<string> _createdIndexes;

    private AggregateFootprint(IAggregateStore store, IEnumerable<string> ids, IReadOnlyList<string> createdIndexes)
    {
        _store = store;
        _ids = ids;
        _createdIndexes = createdIndexes;
    }

    /// <summary>Records the footprint before the load. <paramref name="ids"/> is enumerated again on removal.</summary>
    /// <exception cref="AggregateIdCollisionException">The store already holds a document with an id the run loads; the load would overwrite it.</exception>
    public static async Task<AggregateFootprint> BeforeLoadAsync(IAggregateStore store, IEnumerable<string> ids, CancellationToken ct)
    {
        foreach (var batch in ids.Chunk(BatchSize))
        {
            var existing = await store.ExistingIdsAsync(batch, ct);
            if (existing.Count > 0)
                throw new AggregateIdCollisionException(existing.Count, existing.First());
        }
        var before = await store.IndexNamesAsync(ct);
        return new AggregateFootprint(store, ids, store.AggregateIndexNames.Where(n => before.Contains(n) == false).ToList());
    }

    /// <summary>Deletes every id the run loads and every aggregate index the run created.</summary>
    public async Task RemoveAsync(CancellationToken ct)
    {
        foreach (var batch in _ids.Chunk(BatchSize))
            await _store.DeleteAsync(batch, ct);
        foreach (var name in _createdIndexes)
            await _store.DeleteIndexAsync(name, ct);
    }
}

/// <summary>Thrown before a load when the store already holds documents with ids the run loads.</summary>
public sealed class AggregateIdCollisionException(int count, string example)
    : InvalidOperationException($"The target already holds {count} document(s) with ids this run loads, for example '{example}'; the load would overwrite them and the cleanup would remove them. Remove them or use another database.");
