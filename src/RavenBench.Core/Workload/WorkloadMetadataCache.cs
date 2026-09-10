using Raven.Client.Documents;
using RavenBench.Core;

namespace RavenBench.Core.Workload;

// Shared "load cached workload metadata, else discover and store it" envelope.
internal static class WorkloadMetadataCache
{
    public const int DefaultSampleSize = 10000;

    public static async Task<T> DiscoverOrLoadAsync<T>(
        string serverUrl,
        string databaseName,
        string metadataDocId,
        Func<T, bool> isComplete,
        Action<T> logCached,
        Func<IDocumentStore, Task<T>> discover,
        Action<T> logStored)
        where T : class
    {
        using var store = HttpHelper.Create(serverUrl, databaseName, httpVersion: null);

        using var session = store.OpenAsyncSession();
        var cached = await session.LoadAsync<T>(metadataDocId);

        if (cached != null && isComplete(cached))
        {
            logCached(cached);
            return cached;
        }

        var metadata = await discover(store);

        await session.StoreAsync(metadata, metadataDocId);
        await session.SaveChangesAsync();
        logStored(metadata);

        return metadata;
    }
}
