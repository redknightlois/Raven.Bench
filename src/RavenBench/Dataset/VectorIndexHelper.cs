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
        await WaitForNonStaleAsync(store, index.Name);
    }

    // Polls the statistics: a query that waits for non-stale results is cancelled by the server
    // after Databases.QueryTimeoutInSec, which a large index build outlasts.
    public static async Task WaitForNonStaleAsync(IDocumentStore store, string indexName)
    {
        while (true)
        {
            var stats = await store.Maintenance.SendAsync(new GetIndexStatisticsOperation(indexName));
            if (stats.State == IndexState.Error)
                throw new InvalidOperationException($"Index '{indexName}' is in the error state.");
            if (stats.IsStale == false)
                return;
            await Task.Delay(TimeSpan.FromSeconds(1));
        }
    }
}
