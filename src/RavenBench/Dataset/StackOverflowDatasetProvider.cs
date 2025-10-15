using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.ServerWide.Operations;

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

    public async Task<bool> IsDatasetImportedAsync(string serverUrl, string databaseName, int expectedMinDocuments = 1000)
    {
        try
        {
            using var store = new DocumentStore
            {
                Urls = new[] { serverUrl }
            };
            store.Initialize();

            // First check if database exists
            var dbRecord = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
            if (dbRecord == null)
            {
                Console.WriteLine($"[Dataset] Database '{databaseName}' does not exist");
                return false;
            }

            // Check document counts
            store.Database = databaseName;
            var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());

            if (stats.CountOfDocuments < expectedMinDocuments)
            {
                Console.WriteLine($"[Dataset] Database '{databaseName}' exists but has only {stats.CountOfDocuments} documents (expected >= {expectedMinDocuments})");
                return false;
            }

            // Verify StackOverflow-specific collections: questions and users
            using var session = store.OpenAsyncSession();
            var questionsExist = await session.Advanced.AsyncRawQuery<object>("from questions")
                .Take(1)
                .AnyAsync();

            var usersExist = await session.Advanced.AsyncRawQuery<object>("from users")
                .Take(1)
                .AnyAsync();

            if (!questionsExist || !usersExist)
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
}
