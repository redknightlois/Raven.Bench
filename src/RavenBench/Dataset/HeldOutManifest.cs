using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using RavenBench.Dataset.Vectors;

namespace RavenBench.Dataset;

/// <summary>
/// Records in a RavenDB database which set files it was loaded from and which query selection was held out of
/// the load, so a database loaded from other files or under another selection is never measured against this
/// selection's truth. A set with a published query split holds nothing out and records a null selection.
/// </summary>
internal static class HeldOutManifest
{
    private const string DocumentId = "benchmark/held-out-queries";

    private sealed class Manifest
    {
        public string Set { get; set; } = "";
        public string Files { get; set; } = "";
        public int? Seed { get; set; }
        public int? Count { get; set; }
    }

    /// <summary>
    /// True when the database already holds at least <paramref name="expectedDocuments"/> documents loaded under
    /// this record. Throws when it holds documents loaded under another record or under none at all.
    /// </summary>
    public static async Task<bool> EnsureMatchesAsync(IDocumentStore store, VerifiedFiles files, QuerySelection? selection, long expectedDocuments)
    {
        var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
        using var session = store.OpenAsyncSession();
        var manifest = await session.LoadAsync<Manifest>(DocumentId);

        if (manifest == null)
        {
            if (stats.CountOfDocuments > 0)
                throw new InvalidOperationException(
                    $"Database '{store.Database}' holds {stats.CountOfDocuments:N0} documents but no load record, so its load may be partial or hold its queries as documents. Drop it and load again.");
            return false;
        }

        if (manifest.Set != files.SetName || manifest.Files != files.Fingerprint || manifest.Seed != selection?.Seed || manifest.Count != selection?.Count)
            throw new InvalidOperationException(
                $"Database '{store.Database}' was loaded from set '{manifest.Set}' ({manifest.Files}) holding out {manifest.Count ?? 0} queries with seed {manifest.Seed}; this run loads '{files.SetName}' ({files.Fingerprint}) holding out {selection?.Count ?? 0} with seed {selection?.Seed}. Drop it or use the same set and selection.");

        return stats.CountOfDocuments >= expectedDocuments;
    }

    public static async Task StoreAsync(IDocumentStore store, VerifiedFiles files, QuerySelection? selection)
    {
        using var session = store.OpenAsyncSession();
        await session.StoreAsync(new Manifest { Set = files.SetName, Files = files.Fingerprint, Seed = selection?.Seed, Count = selection?.Count }, DocumentId);
        await session.SaveChangesAsync();
    }
}
