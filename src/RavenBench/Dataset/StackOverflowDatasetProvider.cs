using System.Net;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.ServerWide.Operations;
using RavenBench.Core;

namespace RavenBench.Dataset;

/// <summary>
/// Dataset provider for StackOverflow data with questions and users collections.
/// </summary>
public class StackOverflowDatasetProvider : IDatasetProvider
{
    public string DatasetName => "stackoverflow";

    public DatasetInfo GetDatasetInfo(string? profile = null, int? customSize = null)
    {
        if (string.IsNullOrEmpty(profile) == false)
        {
            var parsedProfile = Enum.Parse<DatasetProfile>(profile, ignoreCase: true);
            var size = KnownDatasets.GetDatasetSize(parsedProfile);
            return KnownDatasets.StackOverflowPartial(size);
        }
        else if (customSize.HasValue)
        {
            return KnownDatasets.StackOverflowPartial(customSize.Value);
        }
        else
        {
            return KnownDatasets.StackOverflow;
        }
    }

    public string GetDatabaseName(string? profile = null, int? customSize = null)
    {
        if (string.IsNullOrEmpty(profile) == false)
        {
            var parsedProfile = Enum.Parse<DatasetProfile>(profile, ignoreCase: true);
            return KnownDatasets.GetDatabaseName(parsedProfile);
        }
        else if (customSize.HasValue)
        {
            return KnownDatasets.GetDatabaseNameForSize(customSize.Value);
        }
        else
        {
            return "StackOverflow";
        }
    }

    public async Task<bool> IsDatasetImportedAsync(string serverUrl, string databaseName, int expectedMinDocuments = 1000, Version? httpVersion = null)
    {
        try
        {
            using var store = new DocumentStore
            {
                Urls = new[] { serverUrl }
            };
            if (httpVersion != null)
                HttpHelper.ConfigureHttpVersion(store, httpVersion, HttpVersionPolicy.RequestVersionExact);
            store.Initialize();

            // First check if database exists
            var dbRecord = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
            if (dbRecord == null)
            {
                Console.WriteLine($"[Dataset] Database '{databaseName}' does not exist");
                return false;
            }

            var stats = await store.Maintenance.ForDatabase(databaseName).SendAsync(new GetStatisticsOperation());

            if (stats.CountOfDocuments < expectedMinDocuments)
            {
                Console.WriteLine($"[Dataset] Database '{databaseName}' exists but has only {stats.CountOfDocuments} documents (expected >= {expectedMinDocuments})");
                return false;
            }

            // Verify StackOverflow-specific collections: questions and users
            using var session = store.OpenAsyncSession(databaseName);
            var questionsExist = await session.Advanced.AsyncRawQuery<object>("from questions")
                .Take(1)
                .AnyAsync();

            var usersExist = await session.Advanced.AsyncRawQuery<object>("from users")
                .Take(1)
                .AnyAsync();

            if (questionsExist == false || usersExist == false)
            {
                Console.WriteLine($"[Dataset] Database '{databaseName}' exists but missing expected collections (questions: {questionsExist}, users: {usersExist})");
                return false;
            }

            Console.WriteLine($"[Dataset] Database '{databaseName}' already has {stats.CountOfDocuments} documents with expected collections");
            return true;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Dataset] Error checking if dataset exists: {ex.Message}");
            return false;
        }
    }

    /// <summary>
    /// Result of creating static indexes for StackOverflow workloads.
    /// Contains the actual index names with engine suffix for use by workloads.
    /// Extra indexes are null unless requested at creation time.
    /// </summary>
    public sealed class StaticIndexNames
    {
        public required string UsersDisplayNameIndex { get; init; }
        public required string UsersReputationIndex { get; init; }
        public required string QuestionsTitleIndex { get; init; }
        public required string QuestionsTitleSearchIndex { get; init; }
        public string? UsersSpatialIndex { get; init; }
        public string? QuestionsTitleSuggestionsIndex { get; init; }
        public string? QuestionsTitleMoreLikeThisIndex { get; init; }
        public string? QuestionsViewCountGroupedIndex { get; init; }
        public string? QuestionsTagsIndex { get; init; }
    }

    /// <summary>
    /// Creates static indexes for StackOverflow workloads to eliminate auto-index usage.
    /// Index names include the search engine suffix (e.g., "Users/ByDisplayName-corax").
    /// Always creates the four base indexes plus any requested extras.
    /// Waits for the created indexes to become non-stale before returning.
    /// </summary>
    public async Task<StaticIndexNames> CreateStaticIndexesAsync(
        string serverUrl,
        string databaseName,
        IndexingEngine searchEngine,
        Version? httpVersion = null,
        IReadOnlyCollection<StackOverflowIndex>? extraIndexes = null)
    {
        using var store = new DocumentStore
        {
            Urls = new[] { serverUrl },
            Database = databaseName
        };
        if (httpVersion != null)
            HttpHelper.ConfigureHttpVersion(store, httpVersion, HttpVersionPolicy.RequestVersionExact);
        store.Initialize();

        var baseIndexes = new[]
        {
            StackOverflowIndex.UsersByDisplayName,
            StackOverflowIndex.UsersByReputation,
            StackOverflowIndex.QuestionsByTitle,
            StackOverflowIndex.QuestionsByTitleSearch
        };
        var indexes = baseIndexes.Concat(extraIndexes ?? Array.Empty<StackOverflowIndex>()).Distinct().ToArray();

        string? NameIf(StackOverflowIndex index) =>
            indexes.Contains(index) ? StackOverflowIndexes.GetName(index, searchEngine) : null;

        var indexNames = new StaticIndexNames
        {
            UsersDisplayNameIndex = StackOverflowIndexes.GetName(StackOverflowIndex.UsersByDisplayName, searchEngine),
            UsersReputationIndex = StackOverflowIndexes.GetName(StackOverflowIndex.UsersByReputation, searchEngine),
            QuestionsTitleIndex = StackOverflowIndexes.GetName(StackOverflowIndex.QuestionsByTitle, searchEngine),
            QuestionsTitleSearchIndex = StackOverflowIndexes.GetName(StackOverflowIndex.QuestionsByTitleSearch, searchEngine),
            UsersSpatialIndex = NameIf(StackOverflowIndex.UsersBySpatial),
            QuestionsTitleSuggestionsIndex = NameIf(StackOverflowIndex.QuestionsByTitleSuggestions),
            QuestionsTitleMoreLikeThisIndex = NameIf(StackOverflowIndex.QuestionsByTitleMoreLikeThis),
            QuestionsViewCountGroupedIndex = NameIf(StackOverflowIndex.QuestionsByViewCountGrouped),
            QuestionsTagsIndex = NameIf(StackOverflowIndex.QuestionsByTags)
        };

        Console.WriteLine($"[Dataset] Creating static indexes for StackOverflow workloads (engine: {searchEngine})...");

        var definitions = indexes.Select(i => StackOverflowIndexes.GetDefinition(i, searchEngine)).ToArray();
        await PutIndexesWithRetryAsync(store, definitions);

        var createdNames = definitions.Select(d => d.Name!).ToArray();
        Console.WriteLine($"[Dataset] Created indexes: {string.Join(", ", createdNames)}");

        await WaitForIndexesNonStaleAsync(store, createdNames);
        return indexNames;
    }

    // Index creation commits through Rachis; right after a large import the cluster write can
    // exceed the server's 15s operation timeout on small/IO-bound SKUs. Transient once the
    // import settles, so retry with escalating backoff before giving up.
    private static async Task PutIndexesWithRetryAsync(IDocumentStore store, IndexDefinition[] definitions)
    {
        const int maxAttempts = 8;
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                await store.Maintenance.SendAsync(new PutIndexesOperation(definitions));
                return;
            }
            catch (Exception e) when (attempt < maxAttempts && IsTransientClusterError(e))
            {
                var delay = TimeSpan.FromSeconds(attempt * 5);
                Console.WriteLine($"[Dataset] Index creation attempt {attempt} failed ({e.GetType().Name}); cluster busy, retrying in {delay.TotalSeconds:F0}s...");
                await Task.Delay(delay);
            }
        }
    }

    private static bool IsTransientClusterError(Exception e)
    {
        for (var cur = e; cur != null; cur = cur.InnerException)
        {
            if (cur is TimeoutException)
                return true;
            if (cur.Message.Contains("Cluster is probably down", StringComparison.OrdinalIgnoreCase)
                || cur.Message.Contains("was not applied in this time", StringComparison.OrdinalIgnoreCase))
                return true;
        }
        return false;
    }

    /// <summary>
    /// Polls until the named indexes are non-stale, up to 10 minutes.
    /// </summary>
    public static async Task WaitForIndexesNonStaleAsync(IDocumentStore store, IReadOnlyCollection<string> indexNames)
    {
        Console.WriteLine("[Dataset] Waiting for static indexes to become non-stale...");
        var maxWait = TimeSpan.FromMinutes(10);
        var sw = System.Diagnostics.Stopwatch.StartNew();

        while (sw.Elapsed < maxWait)
        {
            var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
            var staleCount = stats.Indexes.Count(i => indexNames.Contains(i.Name) && i.IsStale);

            if (staleCount == 0)
            {
                Console.WriteLine($"[Dataset] All static indexes are non-stale (waited {sw.Elapsed.TotalSeconds:F1}s)");
                return;
            }

            Console.WriteLine($"[Dataset] {staleCount} static index(es) still stale, waiting... ({sw.Elapsed.TotalSeconds:F0}s elapsed)");
            await Task.Delay(2000);
        }

        Console.WriteLine($"[Dataset] WARNING: Static indexes still stale after {maxWait.TotalMinutes} minutes");
    }
}
