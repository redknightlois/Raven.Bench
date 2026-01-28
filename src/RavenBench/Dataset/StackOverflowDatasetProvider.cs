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

            // Check document counts using ForDatabase() since store is already initialized
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
    /// </summary>
    public sealed class StaticIndexNames
    {
        public required string UsersDisplayNameIndex { get; init; }
        public required string UsersReputationIndex { get; init; }
        public required string QuestionsTitleIndex { get; init; }
        public required string QuestionsTitleSearchIndex { get; init; }
    }

    /// <summary>
    /// Creates static indexes for StackOverflow workloads to eliminate auto-index usage.
    /// Index names include the search engine suffix (e.g., "Users/ByDisplayName-corax").
    /// Waits for indexes to become non-stale before returning.
    /// </summary>
    /// <param name="serverUrl">RavenDB server URL</param>
    /// <param name="databaseName">Target database name</param>
    /// <param name="searchEngine">Search engine type (Corax or Lucene)</param>
    /// <param name="httpVersion">Optional HTTP version for requests</param>
    /// <returns>Created index names for use by workloads</returns>
    public async Task<StaticIndexNames> CreateStaticIndexesAsync(
        string serverUrl,
        string databaseName,
        IndexingEngine searchEngine,
        Version? httpVersion = null)
    {
        using var store = new DocumentStore
        {
            Urls = new[] { serverUrl },
            Database = databaseName
        };
        if (httpVersion != null)
            HttpHelper.ConfigureHttpVersion(store, httpVersion, HttpVersionPolicy.RequestVersionExact);
        store.Initialize();

        var engineName = searchEngine == IndexingEngine.Lucene ? "Lucene" : "Corax";
        var engineSuffix = searchEngine == IndexingEngine.Lucene ? "-lucene" : "-corax";

        var indexNames = new StaticIndexNames
        {
            UsersDisplayNameIndex = $"Users/ByDisplayName{engineSuffix}",
            UsersReputationIndex = $"Users/ByReputation{engineSuffix}",
            QuestionsTitleIndex = $"Questions/ByTitle{engineSuffix}",
            QuestionsTitleSearchIndex = $"Questions/ByTitleSearch{engineSuffix}"
        };

        Console.WriteLine($"[Dataset] Creating static indexes for StackOverflow workloads (engine: {engineName})...");

        // 1. Users/ByDisplayName - equality queries on DisplayName
        var usersDisplayNameIndex = new IndexDefinition
        {
            Name = indexNames.UsersDisplayNameIndex,
            Maps = new HashSet<string> { "from u in docs.Users select new { u.DisplayName }" },
            Configuration = new IndexConfiguration { { "Indexing.Static.SearchEngineType", engineName } }
        };

        // 2. Users/ByReputation - range queries on Reputation
        var usersReputationIndex = new IndexDefinition
        {
            Name = indexNames.UsersReputationIndex,
            Maps = new HashSet<string> { "from u in docs.Users select new { u.Reputation }" },
            Configuration = new IndexConfiguration { { "Indexing.Static.SearchEngineType", engineName } }
        };

        // 3. Questions/ByTitle - startsWith queries on Title
        var questionsTitleIndex = new IndexDefinition
        {
            Name = indexNames.QuestionsTitleIndex,
            Maps = new HashSet<string> { "from q in docs.Questions select new { q.Title }" },
            Configuration = new IndexConfiguration { { "Indexing.Static.SearchEngineType", engineName } }
        };

        // 4. Questions/ByTitleSearch - full-text search queries on Title
        var questionsTitleSearchIndex = new IndexDefinition
        {
            Name = indexNames.QuestionsTitleSearchIndex,
            Maps = new HashSet<string> { "from q in docs.Questions select new { q.Title }" },
            Fields = new Dictionary<string, IndexFieldOptions>
            {
                ["Title"] = new IndexFieldOptions { Indexing = FieldIndexing.Search }
            },
            Configuration = new IndexConfiguration { { "Indexing.Static.SearchEngineType", engineName } }
        };

        // Create all indexes
        await store.Maintenance.SendAsync(new PutIndexesOperation(
            usersDisplayNameIndex,
            usersReputationIndex,
            questionsTitleIndex,
            questionsTitleSearchIndex));

        Console.WriteLine($"[Dataset] Created indexes: {indexNames.UsersDisplayNameIndex}, {indexNames.UsersReputationIndex}, {indexNames.QuestionsTitleIndex}, {indexNames.QuestionsTitleSearchIndex}");

        // Wait for all indexes to become non-stale
        Console.WriteLine("[Dataset] Waiting for static indexes to become non-stale...");
        var maxWait = TimeSpan.FromMinutes(10);
        var sw = System.Diagnostics.Stopwatch.StartNew();

        while (sw.Elapsed < maxWait)
        {
            var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
            var ourIndexes = new[] { indexNames.UsersDisplayNameIndex, indexNames.UsersReputationIndex, indexNames.QuestionsTitleIndex, indexNames.QuestionsTitleSearchIndex };
            var staleIndexes = stats.Indexes
                .Where(i => ourIndexes.Contains(i.Name) && i.IsStale)
                .ToList();

            if (staleIndexes.Count == 0)
            {
                Console.WriteLine($"[Dataset] All static indexes are non-stale (waited {sw.Elapsed.TotalSeconds:F1}s)");
                return indexNames;
            }

            Console.WriteLine($"[Dataset] {staleIndexes.Count} static index(es) still stale, waiting... ({sw.Elapsed.TotalSeconds:F0}s elapsed)");
            await Task.Delay(2000);
        }

        Console.WriteLine($"[Dataset] WARNING: Static indexes still stale after {maxWait.TotalMinutes} minutes");
        return indexNames;
    }
}
