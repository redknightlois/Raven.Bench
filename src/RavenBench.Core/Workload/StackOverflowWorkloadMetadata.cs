using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using System.Linq;

namespace RavenBench.Core.Workload;

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

    // Sampled question tags for tag-filtered (stream) queries
    public string[] Tags { get; set; } = Array.Empty<string>();

    // Static index names, populated at runtime (include the engine suffix)
    public string? TitleIndexName { get; set; }
    public string? TitleSearchIndexName { get; set; }
    public string? TitleSuggestionsIndexName { get; set; }
    public string? TitleMoreLikeThisIndexName { get; set; }
    public string? ViewCountGroupedIndexName { get; set; }
    public string? TagsIndexName { get; set; }
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
        int maxQuestionId,
        int maxUserId,
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
            cached.TitlePrefixes.Length > 0 && (cached.SearchTermsRare.Length > 0 || cached.SearchTermsCommon.Length > 0) &&
            cached.Tags.Length > 0)
        {
            Console.WriteLine($"[Workload] Using cached StackOverflow metadata: {cached.QuestionIds.Length} questions, {cached.UserIds.Length} users, {cached.TitlePrefixes.Length} prefixes, {cached.SearchTermsRare.Length + cached.SearchTermsCommon.Length} search terms, {cached.Tags.Length} tags");
            return cached;
        }

        Console.WriteLine("[Workload] Discovering StackOverflow document IDs and text search terms by sampling database...");

        var questions = await SampleExistingDocsAsync(store, "questions", maxQuestionId, seed, sampleSize);
        var users = await SampleExistingDocsAsync(store, "users", maxUserId, seed + 1, sampleSize);

        if (questions.Count == 0 || users.Count == 0)
        {
            throw new InvalidOperationException(
                $"Failed to discover StackOverflow document IDs. Found {questions.Count} questions, {users.Count} users. " +
                "Ensure the StackOverflow dataset is imported before running benchmarks.");
        }

        var titles = new List<string>(questions.Count);
        var tagArrays = new List<dynamic>(questions.Count);
        foreach (var (_, doc) in questions)
        {
            try { string? t = (doc as dynamic)?.Title; if (string.IsNullOrWhiteSpace(t) == false) titles.Add(t!); } catch { }
            try { var tg = (doc as dynamic)?.Tags; if (tg != null) tagArrays.Add(tg); } catch { }
        }

        var (titlePrefixes, searchTermsRare, searchTermsCommon) = BuildTextSearchTerms(titles);
        var tags = BuildTags(tagArrays);

        Console.WriteLine($"[Workload] Sampled {questions.Count} questions, {users.Count} users");
        Console.WriteLine($"[Workload] Discovered {titlePrefixes.Length} title prefixes, {searchTermsRare.Length} rare terms, {searchTermsCommon.Length} common terms, {tags.Length} tags");

        var metadata = new StackOverflowWorkloadMetadata
        {
            QuestionIds = questions.Select(q => q.Id).ToArray(),
            UserIds = users.Select(u => u.Id).ToArray(),
            QuestionCount = questions.Count,
            UserCount = users.Count,
            TitlePrefixes = titlePrefixes,
            SearchTermsRare = searchTermsRare,
            SearchTermsCommon = searchTermsCommon,
            Tags = tags,
            ComputedAt = DateTime.UtcNow
        };

        await session.StoreAsync(metadata, MetadataDocId);
        await session.SaveChangesAsync();
        Console.WriteLine("[Workload] Stored StackOverflow workload metadata in database");

        return metadata;
    }

    /// <summary>
    /// Samples existing documents from a collection by bulk-loading random ids in [1, maxId].
    /// Point lookups only — never scans or sorts the collection, so it does not depend on the
    /// dataset fitting in RAM. Candidate ids are drawn deterministically from the seed; gaps in
    /// the id space are skipped. Returns the numeric id paired with the loaded document.
    /// </summary>
    public static async Task<List<(int Id, dynamic Doc)>> SampleExistingDocsAsync(
        IDocumentStore store,
        string collection,
        int maxId,
        int seed,
        int sampleSize)
    {
        if (maxId <= 0)
            throw new InvalidOperationException($"Cannot sample '{collection}': max id is {maxId}.");

        var rng = new Random(seed);
        var tried = new HashSet<int>();
        var results = new List<(int, dynamic)>(sampleSize);
        const int batchSize = 256;
        var attemptCap = Math.Min((long)maxId, Math.Max((long)sampleSize * 8, sampleSize + batchSize));

        while (results.Count < sampleSize && tried.Count < attemptCap)
        {
            var batch = new List<int>(batchSize);
            var ids = new List<string>(batchSize);
            while (batch.Count < batchSize && tried.Count < attemptCap)
            {
                var n = rng.Next(1, maxId + 1);
                if (tried.Add(n)) { batch.Add(n); ids.Add($"{collection}/{n}"); }
            }
            if (ids.Count == 0)
                break;

            using var session = store.OpenAsyncSession();
            var loaded = await session.LoadAsync<dynamic>(ids);
            for (int i = 0; i < batch.Count; i++)
            {
                if (loaded.TryGetValue(ids[i], out var doc) && doc != null)
                    results.Add((batch[i], doc));
            }
        }

        return results;
    }

    /// <summary>
    /// Selects a mix of common and medium-frequency tags from sampled question tag arrays.
    /// </summary>
    private static string[] BuildTags(List<dynamic> tagArrays)
    {
        var tagCounts = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        foreach (var tags in tagArrays)
        {
            try
            {
                foreach (var tag in tags)
                {
                    string? tagString = tag?.ToString();
                    if (string.IsNullOrWhiteSpace(tagString) == false)
                    {
                        tagCounts.TryGetValue(tagString!, out var count);
                        tagCounts[tagString!] = count + 1;
                    }
                }
            }
            catch
            {
            }
        }

        var sorted = tagCounts.OrderByDescending(kvp => kvp.Value).ToList();
        var selected = sorted.Take(20).Select(kvp => kvp.Key).ToList();
        selected.AddRange(sorted.Skip(sorted.Count / 4).Take(30).Select(kvp => kvp.Key));

        return selected.Distinct(StringComparer.OrdinalIgnoreCase).ToArray();
    }

    /// <summary>
    /// Builds title prefixes (3-5 chars, startsWith queries) and rare/common search terms from
    /// sampled question titles, giving varied selectivity for the text-query workloads.
    /// </summary>
    private static (string[] prefixes, string[] rareTerms, string[] commonTerms) BuildTextSearchTerms(
        List<string> titles)
    {
        if (titles.Count == 0)
        {
            return (
                new[] { "How", "What", "Why", "C#", "Java", "Python", "Error", "Fix" },
                new[] { "algorithm", "optimization", "async", "multithreading" },
                new[] { "error", "problem", "issue", "help" }
            );
        }

        // Prefixes enable startsWith queries with varying selectivity based on title distribution.
        var prefixCounts = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        var wordCounts = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

        foreach (var title in titles)
        {
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

        var (rareTerms, commonTerms) = SelectSearchTerms(wordCounts);

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

    // Bottom/top frequency slices of sampled title words. Words come from stored titles,
    // so any count proves existence — a >= 2 floor would empty the rare slice on corpora
    // where the least-frequent sampled words each occur once.
    public static (string[] Rare, string[] Common) SelectSearchTerms(Dictionary<string, int> wordCounts)
    {
        var sortedWords = wordCounts.OrderBy(kvp => kvp.Value).ToList();
        var sliceSize = Math.Max(1, sortedWords.Count / 5);

        var rare = sortedWords
            .Take(sliceSize)
            .Where(kvp => kvp.Value >= 1)
            .Take(20)
            .Select(kvp => kvp.Key)
            .ToArray();

        var common = sortedWords
            .OrderByDescending(kvp => kvp.Value)
            .Take(sliceSize)
            .Take(20)
            .Select(kvp => kvp.Key)
            .ToArray();

        return (rare, common);
    }
}
