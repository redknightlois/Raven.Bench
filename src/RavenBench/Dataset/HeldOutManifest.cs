using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using RavenBench.Dataset.Vectors;

namespace RavenBench.Dataset;

/// <summary>
/// Records in a RavenDB database which query selection was held out of its load, so a database loaded
/// under another selection, or with no held-out queries, is never measured against this selection's truth.
/// </summary>
internal static class HeldOutManifest
{
    private const string DocumentId = "benchmark/held-out-queries";

    private sealed class Manifest
    {
        public string Set { get; set; } = "";
        public string Files { get; set; } = "";
        public int Seed { get; set; }
        public int Count { get; set; }
    }

    /// <summary>
    /// True when the database already holds at least <paramref name="expectedDocuments"/> documents loaded under
    /// this selection. Throws when it holds documents loaded under another selection or none at all.
    /// </summary>
    public static async Task<bool> EnsureMatchesAsync(IDocumentStore store, VerifiedFiles files, QuerySelection selection, long expectedDocuments)
    {
        var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
        using var session = store.OpenAsyncSession();
        var manifest = await session.LoadAsync<Manifest>(DocumentId);

        if (manifest == null)
        {
            if (stats.CountOfDocuments > 0)
                throw new InvalidOperationException(
                    $"Database '{store.Database}' holds {stats.CountOfDocuments:N0} documents but no held-out query record, so its queries may be loaded as documents. Drop it and load again.");
            return false;
        }

        if (manifest.Set != files.SetName || manifest.Files != files.Fingerprint || manifest.Seed != selection.Seed || manifest.Count != selection.Count)
            throw new InvalidOperationException(
                $"Database '{store.Database}' was loaded from set '{manifest.Set}' holding out {manifest.Count} queries with seed {manifest.Seed}; this run selects {selection.Count} with seed {selection.Seed}. Drop it or use the same selection.");

        return stats.CountOfDocuments >= expectedDocuments;
    }

    public static async Task StoreAsync(IDocumentStore store, VerifiedFiles files, QuerySelection selection)
    {
        using var session = store.OpenAsyncSession();
        await session.StoreAsync(new Manifest { Set = files.SetName, Files = files.Fingerprint, Seed = selection.Seed, Count = selection.Count }, DocumentId);
        await session.SaveChangesAsync();
    }
}
