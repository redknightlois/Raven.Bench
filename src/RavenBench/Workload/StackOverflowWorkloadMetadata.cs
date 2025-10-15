using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using System.Linq;

namespace RavenBench.Workload;

/// <summary>
/// Workload-specific metadata for StackOverflow profiles.
/// Stores sampled document IDs to avoid requesting non-existent documents during benchmarks.
/// </summary>
public sealed class StackOverflowWorkloadMetadata
{
    public int[] QuestionIds { get; set; } = Array.Empty<int>();
    public int[] UserIds { get; set; } = Array.Empty<int>();
    public long QuestionCount { get; set; }
    public long UserCount { get; set; }
    public DateTime ComputedAt { get; set; }
}

/// <summary>
/// Helper for discovering and caching StackOverflow workload metadata (sampled document IDs).
/// This is workload-specific logic, not dataset-specific, per PRD architecture.
/// </summary>
public static class StackOverflowWorkloadHelper
{
    private const string MetadataDocId = "workload/stackoverflow-metadata";
    private const int DefaultSampleSize = 10000;

    /// <summary>
    /// Discovers actual document IDs by sampling the database and caches them for workload use.
    /// Returns sampled question and user IDs that exist in the database.
    /// </summary>
    public static async Task<StackOverflowWorkloadMetadata> DiscoverOrLoadMetadataAsync(
        string serverUrl,
        string databaseName,
        int sampleSize = DefaultSampleSize)
    {
        using var store = new DocumentStore
        {
            Urls = new[] { serverUrl },
            Database = databaseName
        };
        store.Initialize();

        // Check if we have cached metadata
        using var session = store.OpenAsyncSession();
        var cached = await session.LoadAsync<StackOverflowWorkloadMetadata>(MetadataDocId);

        if (cached != null && cached.QuestionIds.Length > 0 && cached.UserIds.Length > 0)
        {
            Console.WriteLine($"[Workload] Using cached StackOverflow metadata: {cached.QuestionIds.Length} questions, {cached.UserIds.Length} users");
            return cached;
        }

        Console.WriteLine("[Workload] Discovering StackOverflow document IDs by sampling database...");

        // Sample actual document IDs from the database
        var questionIds = await SampleDocumentIdsAsync(store, "questions", sampleSize);
        var userIds = await SampleDocumentIdsAsync(store, "users", sampleSize);

        if (questionIds.Count() == 0 || userIds.Count() == 0)
        {
            throw new InvalidOperationException(
                $"Failed to discover StackOverflow document IDs. Found {questionIds.Count} questions, {userIds.Count} users. " +
                "Ensure the StackOverflow dataset is imported before running benchmarks.");
        }

        Console.WriteLine($"[Workload] Sampled {questionIds.Count()} questions, {userIds.Count()} users");

        // Store metadata for future use
        var metadata = new StackOverflowWorkloadMetadata
        {
            QuestionIds = questionIds.ToArray(),
            UserIds = userIds.ToArray(),
            QuestionCount = questionIds.Count(),
            UserCount = userIds.Count(),
            ComputedAt = DateTime.UtcNow
        };

        await session.StoreAsync(metadata, MetadataDocId);
        await session.SaveChangesAsync();
        Console.WriteLine("[Workload] Stored StackOverflow workload metadata in database");

        return metadata;
    }

    /// <summary>
    /// Samples document IDs from a collection using RavenDB's random() ordering.
    /// Returns a list of actual document IDs that exist in the database.
    /// Uses a deterministic seed for reproducibility.
    /// </summary>
    private static async Task<HashSet<int>> SampleDocumentIdsAsync(
        IDocumentStore store,
        string collection,
        int sampleSize)
    {
        var ids = new HashSet<int>();

        using var session = store.OpenAsyncSession();

        // Use RavenDB's built-in random() ordering with a fixed seed for deterministic sampling
        // This is much more efficient than streaming all documents
        var seed = "ravenbench-sampling-seed";
        var query = session.Advanced.AsyncRawQuery<dynamic>($"from {collection} order by random('{seed}') limit {sampleSize} select id()")
            .WaitForNonStaleResults(); // Ensure we're reading from non-stale indexes

        await using var stream = await session.Advanced.StreamAsync(query);

        while (await stream.MoveNextAsync())
        {
            // The result is just the ID string
            var result = stream.Current.Document;
            string? docId = null;

            // Handle both direct ID strings and objects with Id property
            if (result is string idString)
            {
                docId = idString;
            }
            else if (result != null)
            {
                // Try to get Id property dynamically
                try
                {
                    docId = (result as dynamic)?.Id ?? stream.Current.Id;
                }
                catch
                {
                    docId = stream.Current.Id;
                }
            }

            if (string.IsNullOrEmpty(docId) == false && TryExtractNumericId(docId, collection, out var numericId))
            {
                ids.Add(numericId);
            }
        }

        return ids;
    }

    /// <summary>
    /// Extracts numeric ID from document ID format: "questions/123" -> 123
    /// </summary>
    private static bool TryExtractNumericId(string docId, string expectedPrefix, out int numericId)
    {
        numericId = 0;

        var prefix = expectedPrefix + "/";
        if (!docId.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
            return false;

        var idPart = docId.Substring(prefix.Length);
        return int.TryParse(idPart, out numericId);
    }
}
