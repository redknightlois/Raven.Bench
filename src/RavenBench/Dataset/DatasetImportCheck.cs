using Raven.Client.Documents.Operations;
using Raven.Client.ServerWide.Operations;
using RavenBench.Core;

namespace RavenBench.Dataset;

// Shared already-imported probe. Any failure to reach the server reports "not imported".
internal static class DatasetImportCheck
{
    public static async Task<bool> RunAsync(
        string serverUrl,
        string databaseName,
        Version? httpVersion,
        string logPrefix,
        string collectionName,
        int expectedMinDocuments)
    {
        try
        {
            using var store = HttpHelper.Create(serverUrl, databaseName, httpVersion);

            var dbRecord = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
            if (dbRecord == null)
            {
                Console.WriteLine($"{logPrefix} Database '{databaseName}' does not exist");
                return false;
            }

            var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
            if (stats.CountOfDocuments < expectedMinDocuments)
            {
                Console.WriteLine($"{logPrefix} Database '{databaseName}' exists but has only {stats.CountOfDocuments} documents (expected >= {expectedMinDocuments})");
                return false;
            }

            using var session = store.OpenAsyncSession();
            var collectionExists = await session.Advanced.AsyncRawQuery<object>($"from {collectionName}")
                .Take(1)
                .AnyAsync();

            if (collectionExists == false)
            {
                Console.WriteLine($"{logPrefix} Database '{databaseName}' exists but '{collectionName}' collection is missing");
                return false;
            }

            Console.WriteLine($"{logPrefix} Database '{databaseName}' already has {stats.CountOfDocuments:N0} documents - skipping import");
            return true;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"{logPrefix} Skip check failed: {ex.Message}");
            return false;
        }
    }
}
