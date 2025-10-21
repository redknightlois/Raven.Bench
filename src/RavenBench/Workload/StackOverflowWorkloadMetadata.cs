using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using System.Linq;

namespace RavenBench.Workload;

/// <summary>
/// Workload-specific metadata for StackOverflow profiles.
/// Stores sampled document IDs to avoid requesting non-existent documents during benchmarks,
/// plus title prefixes for text search queries.
/// </summary>
public sealed class StackOverflowWorkloadMetadata
{
    public int[] QuestionIds { get; set; } = Array.Empty<int>();
    public int[] UserIds { get; set; } = Array.Empty<int>();
    public long QuestionCount { get; set; }
    public long UserCount { get; set; }
    public DateTime ComputedAt { get; set; }

    // Text search metadata: title prefixes for startsWith queries
    // Mix of common and rare prefixes to test different selectivity
    public string[] TitlePrefixes { get; set; } = Array.Empty<string>();

    // Search terms for full-text search queries (rare and common terms)
    public string[] SearchTermsRare { get; set; } = Array.Empty<string>();
    public string[] SearchTermsCommon { get; set; } = Array.Empty<string>();
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
        int seed,
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

        if (cached != null && cached.QuestionIds.Length > 0 && cached.UserIds.Length > 0 &&
            cached.TitlePrefixes.Length > 0 && (cached.SearchTermsRare.Length > 0 || cached.SearchTermsCommon.Length > 0))
        {
            Console.WriteLine($"[Workload] Using cached StackOverflow metadata: {cached.QuestionIds.Length} questions, {cached.UserIds.Length} users, {cached.TitlePrefixes.Length} prefixes, {cached.SearchTermsRare.Length + cached.SearchTermsCommon.Length} search terms");
            return cached;
        }

        Console.WriteLine("[Workload] Discovering StackOverflow document IDs and text search terms by sampling database...");

        // Sample actual document IDs from the database
        var questionIds = await SampleDocumentIdsAsync(store, "questions", seed, sampleSize);
        var userIds = await SampleDocumentIdsAsync(store, "users", seed, sampleSize);

        if (questionIds.Count() == 0 || userIds.Count() == 0)
        {
            throw new InvalidOperationException(
                $"Failed to discover StackOverflow document IDs. Found {questionIds.Count} questions, {userIds.Count} users. " +
                "Ensure the StackOverflow dataset is imported before running benchmarks.");
        }

        // Sample title prefixes and search terms for text queries to ensure realistic selectivity
        var (titlePrefixes, searchTermsRare, searchTermsCommon) = await DiscoverTextSearchTermsAsync(store, seed);

        Console.WriteLine($"[Workload] Sampled {questionIds.Count()} questions, {userIds.Count()} users");
        Console.WriteLine($"[Workload] Discovered {titlePrefixes.Length} title prefixes, {searchTermsRare.Length} rare terms, {searchTermsCommon.Length} common terms");

        // Store metadata for future use
        var metadata = new StackOverflowWorkloadMetadata
        {
            QuestionIds = questionIds.ToArray(),
            UserIds = userIds.ToArray(),
            QuestionCount = questionIds.Count(),
            UserCount = userIds.Count(),
            TitlePrefixes = titlePrefixes,
            SearchTermsRare = searchTermsRare,
            SearchTermsCommon = searchTermsCommon,
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
        int seed,
        int sampleSize)
    {
        var ids = new HashSet<int>();

        using var session = store.OpenAsyncSession();

        // Use RavenDB's built-in random() ordering with a deterministic seed for reproducible sampling
        // This is much more efficient than streaming all documents
        // Note: WaitForNonStaleResults is not compatible with streaming queries
        // The caller must ensure indexes are non-stale before calling this method
        var samplingSeed = $"ravenbench-{seed}";
        var query = session.Advanced.AsyncRawQuery<dynamic>($"from {collection} order by random('{samplingSeed}') select id() limit {sampleSize}");

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

    /// <summary>
    /// Discovers title prefixes and search terms for text-based query workloads.
    /// Samples question titles and extracts prefixes (3-5 chars) and words to create varied selectivity:
    /// - Prefixes for startsWith queries (varying result set sizes)
    /// - Rare terms (low frequency) for selective search queries
    /// - Common terms (high frequency) for less selective search queries
    /// Uses deterministic sampling for reproducibility across runs.
    /// </summary>
    private static async Task<(string[] prefixes, string[] rareTerms, string[] commonTerms)> DiscoverTextSearchTermsAsync(
        IDocumentStore store,
        int seed)
    {
        using var session = store.OpenAsyncSession();

        // Sample question titles using deterministic random ordering.
        // 5000 titles provide sufficient diversity for prefix and term extraction
        // while being efficient for large datasets.
        const int sampleSize = 5000;
        var samplingSeed = $"ravenbench-title-{seed}";
        var query = session.Advanced.AsyncRawQuery<dynamic>(
            $"from questions order by random('{samplingSeed}') select Title limit {sampleSize}");

        var titles = new List<string>();
        await using var stream = await session.Advanced.StreamAsync(query);

        while (await stream.MoveNextAsync())
        {
            var result = stream.Current.Document;
            string? title = null;

            // Extract Title (handle different response formats)
            if (result is string titleStr)
            {
                title = titleStr;
            }
            else if (result != null)
            {
                try
                {
                    title = (result as dynamic)?.Title;
                }
                catch
                {
                    // Skip entries where we can't extract title
                }
            }

            if (string.IsNullOrWhiteSpace(title) == false)
            {
                titles.Add(title);
            }
        }

        if (titles.Count == 0)
        {
            // Fallback: provide default prefixes and terms
            return (
                new[] { "How", "What", "Why", "C#", "Java", "Python", "Error", "Fix" },
                new[] { "algorithm", "optimization", "async", "multithreading" },
                new[] { "error", "problem", "issue", "help" }
            );
        }

        // Extract prefixes (first 3-5 characters of titles) and words for analysis.
        // Prefixes enable startsWith queries with varying selectivity based on title distribution.
        var prefixCounts = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        var wordCounts = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

        foreach (var title in titles)
        {
            // Extract prefixes of 3-5 characters to capture common title beginnings
            for (int prefixLen = 3; prefixLen <= Math.Min(5, title.Length); prefixLen++)
            {
                var prefix = title.Substring(0, prefixLen);
                if (char.IsLetter(prefix[0])) // Only count letter-starting prefixes
                {
                    prefixCounts.TryGetValue(prefix, out var count);
                    prefixCounts[prefix] = count + 1;
                }
            }

            // Extract words for search terms, splitting on common punctuation
            var words = title.Split(new[] { ' ', ',', '.', '?', '!', ':', ';', '-', '(', ')', '[', ']' },
                StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

            foreach (var word in words)
            {
                if (word.Length >= 3) // Only meaningful words (skip short words)
                {
                    wordCounts.TryGetValue(word, out var count);
                    wordCounts[word] = count + 1;
                }
            }
        }

        // Select diverse prefixes: mix of common (frequent) and medium-frequency prefixes
        // to provide varying selectivity for startsWith queries
        var sortedPrefixes = prefixCounts.OrderByDescending(kvp => kvp.Value).ToList();
        var selectedPrefixes = new List<string>();

        // Take top 20 most common prefixes for broad coverage
        selectedPrefixes.AddRange(sortedPrefixes.Take(20).Select(kvp => kvp.Key));

        // Take 10 medium-frequency prefixes (skip top 25%) for variety in selectivity
        var mediumPrefixes = sortedPrefixes.Skip(sortedPrefixes.Count / 4).Take(10).Select(kvp => kvp.Key);
        selectedPrefixes.AddRange(mediumPrefixes);

        // Select search terms: rare (low frequency) and common (high frequency) words
        // to enable queries with different selectivity characteristics
        var sortedWords = wordCounts.OrderBy(kvp => kvp.Value).ToList();

        // Ensure we have at least some terms even for small datasets
        var sliceSize = Math.Max(1, sortedWords.Count / 5); // Divide into 5 slices

        // Rare terms: bottom slice by frequency (more selective queries, fewer results)
        var rareTerms = sortedWords
            .Take(sliceSize)
            .Where(kvp => kvp.Value >= 2) // At least 2 occurrences to ensure they exist
            .Take(20)
            .Select(kvp => kvp.Key)
            .ToArray();

        // Common terms: top slice by frequency (less selective queries, more results)
        var commonTerms = sortedWords
            .OrderByDescending(kvp => kvp.Value)
            .Take(sliceSize)
            .Take(20)
            .Select(kvp => kvp.Key)
            .ToArray();

        // Fallback for cases where we couldn't extract meaningful terms/prefixes
        // Use the same defaults as the runtime workload fallbacks to ensure cached metadata is considered complete
        if (selectedPrefixes.Count == 0)
        {
            selectedPrefixes.AddRange(new[] { "How", "What", "Why", "Can", "Is" });
        }

        if (rareTerms.Length == 0 && commonTerms.Length == 0)
        {
            rareTerms = new[] { "algorithm", "optimization", "async" };
            commonTerms = new[] { "error", "problem", "help" };
        }

        return (selectedPrefixes.ToArray(), rareTerms, commonTerms);
    }
}
