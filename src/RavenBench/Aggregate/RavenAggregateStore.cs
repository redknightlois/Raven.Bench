using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Exceptions.Database;
using RavenBench.Core.Aggregate;
using RavenBench.Core.Workload;

namespace RavenBench.Aggregate;

/// <summary>The aggregate documents and indexes of one RavenDB database. The caller owns the document store.</summary>
public sealed class RavenAggregateStore(IDocumentStore store) : IAggregateStore
{
    public IReadOnlyList<string> AggregateIndexNames { get; } = AggregateShapes.RavenDbIndexes().Select(i => i.GetProperty("Name").GetString()!).ToList();

    public async Task<IReadOnlyCollection<string>> ExistingIdsAsync(IReadOnlyList<string> ids, CancellationToken ct)
    {
        try
        {
            using var session = store.OpenAsyncSession(new Raven.Client.Documents.Session.SessionOptions { NoTracking = true });
            var loaded = await session.LoadAsync<object>(ids, ct);
            return loaded.Where(p => p.Value is not null).Select(p => p.Key).ToList();
        }
        catch (DatabaseDoesNotExistException)
        {
            return [];
        }
    }

    public async Task<IReadOnlyCollection<string>> IndexNamesAsync(CancellationToken ct)
    {
        try
        {
            return await store.Maintenance.SendAsync(new GetIndexNamesOperation(0, int.MaxValue), ct);
        }
        catch (DatabaseDoesNotExistException)
        {
            return [];
        }
    }

    public async Task DeleteAsync(IReadOnlyList<string> ids, CancellationToken ct)
    {
        using var session = store.OpenAsyncSession();
        foreach (var id in ids)
            session.Delete(id);
        await session.SaveChangesAsync(ct);
    }

    public Task DeleteIndexAsync(string name, CancellationToken ct) => store.Maintenance.SendAsync(new DeleteIndexOperation(name), ct);
}
