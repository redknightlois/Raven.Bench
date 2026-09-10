using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Indexes;

namespace RavenBench.Dataset;

// Shared tail of vector-index creation. Waits without a timeout: a large dataset can index for hours.
internal static class VectorIndexHelper
{
    public static async Task CreateAndWaitForIndexAsync(IDocumentStore store, IndexDefinition index, string logPrefix)
    {
        await store.Maintenance.SendAsync(new PutIndexesOperation(index));
        Console.WriteLine($"{logPrefix} Created index '{index.Name}'");

        Console.WriteLine($"{logPrefix} Waiting for index to become non-stale...");
        using (var session = store.OpenAsyncSession())
        {
            session.Advanced.MaxNumberOfRequestsPerSession = int.MaxValue;
            await session.Query<object>(index.Name)
                .Customize(x => x.WaitForNonStaleResults(TimeSpan.MaxValue))
                .Take(0)
                .ToListAsync();
        }
    }
}
