using System.Globalization;
using Raven.Client.Documents;
using RavenBench.Core;

namespace RavenBench.Core.Workload;

// Shared "load cached workload metadata, else discover and store it" envelope.
internal static class WorkloadMetadataCache
{
    public const int DefaultSampleSize = 10000;

    /// <summary>
    /// The id of the metadata drawn from these sampling inputs. A sample is reused only by a run
    /// with the same inputs, so a run with another seed, sample size or id range draws its own.
    /// </summary>
    public static string DocumentId(string metadataDocId, params int[] samplingInputs) =>
        metadataDocId + "/" + string.Join("-", samplingInputs.Select(i => i.ToString(CultureInfo.InvariantCulture)));

    public static async Task<T> DiscoverOrLoadAsync<T>(
        string serverUrl,
        string databaseName,
        string documentId,
        Func<T, bool> isComplete,
        Action<T> logCached,
        Func<IDocumentStore, Task<T>> discover,
        Action<T> logStored)
        where T : class
    {
        using var store = HttpHelper.Create(serverUrl, databaseName, httpVersion: null);

        return await DiscoverOrLoadAsync(
            async id =>
            {
                using var session = store.OpenAsyncSession();
                return await session.LoadAsync<T>(id);
            },
            async (id, metadata) =>
            {
                // A fresh session tracks no loaded copy, so the store replaces an incomplete document.
                using var session = store.OpenAsyncSession();
                await session.StoreAsync(metadata, id);
                await session.SaveChangesAsync();
            },
            documentId, isComplete, logCached, () => discover(store), logStored);
    }

    internal static async Task<T> DiscoverOrLoadAsync<T>(
        Func<string, Task<T?>> load,
        Func<string, T, Task> save,
        string documentId,
        Func<T, bool> isComplete,
        Action<T> logCached,
        Func<Task<T>> discover,
        Action<T> logStored)
        where T : class
    {
        var cached = await load(documentId);
        if (cached != null && isComplete(cached))
        {
            logCached(cached);
            return cached;
        }

        var metadata = await discover();
        await save(documentId, metadata);
        logStored(metadata);
        return metadata;
    }
}
