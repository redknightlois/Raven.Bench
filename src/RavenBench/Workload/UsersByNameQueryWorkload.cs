using Raven.Client.Documents;
using Raven.Client.Documents.Session;

namespace RavenBench.Workload;

/// <summary>
/// Metadata for Users collection, containing sampled names for parameterized equality queries.
/// </summary>
public sealed class UsersWorkloadMetadata
{
    public string[] SampleNames { get; set; } = Array.Empty<string>();
    public long SampleCount { get; set; }
    public long TotalUserCount { get; set; }
    public DateTime ComputedAt { get; set; }
}

/// <summary>
/// Helper for discovering and caching Users workload metadata (sampled names).
/// </summary>
public static class UsersWorkloadHelper
{
    private const string MetadataDocId = "workload/users-metadata";
    private const int DefaultSampleSize = 10000;

    /// <summary>
    /// Discovers actual user names by sampling the database and caches them for workload use.
    /// Returns sampled names that exist in the database.
    /// </summary>
    public static async Task<UsersWorkloadMetadata> DiscoverOrLoadMetadataAsync(
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
        var cached = await session.LoadAsync<UsersWorkloadMetadata>(MetadataDocId);

        if (cached != null && cached.SampleNames.Length > 0)
        {
            Console.WriteLine($"[Workload] Using cached Users metadata: {cached.SampleNames.Length} sampled names");
            return cached;
        }

        Console.WriteLine("[Workload] Discovering Users names by sampling database...");

        // Sample actual names from the database
        var sampleNames = await SampleUserNamesAsync(store, sampleSize);

        if (sampleNames.Count == 0)
        {
            throw new InvalidOperationException(
                $"Failed to discover Users names. Found {sampleNames.Count} names. " +
                "Ensure the Users dataset is imported before running benchmarks.");
        }

        // Get actual total count of Users documents
        var totalUserCount = await GetTotalUserCountAsync(store);

        Console.WriteLine($"[Workload] Sampled {sampleNames.Count} unique user names from {totalUserCount} total users");

        // Store metadata for future use
        var metadata = new UsersWorkloadMetadata
        {
            SampleNames = sampleNames.ToArray(),
            SampleCount = sampleNames.Count,
            TotalUserCount = totalUserCount,
            ComputedAt = DateTime.UtcNow
        };

        await session.StoreAsync(metadata, MetadataDocId);
        await session.SaveChangesAsync();
        Console.WriteLine("[Workload] Stored Users workload metadata in database");

        return metadata;
    }

    /// <summary>
    /// Samples user names from the Users collection using RavenDB's random() ordering.
    /// Returns a list of actual names that exist in the database.
    /// Uses a deterministic seed for reproducibility.
    /// </summary>
    private static async Task<HashSet<string>> SampleUserNamesAsync(
        IDocumentStore store,
        int sampleSize)
    {
        var names = new HashSet<string>();

        using var session = store.OpenAsyncSession();

        // Use RavenDB's built-in random() ordering with a fixed seed for deterministic sampling
        var seed = "ravenbench-users-sampling-seed";
        var query = session.Advanced.AsyncRawQuery<dynamic>($"from Users order by random('{seed}') select Name limit {sampleSize}");

        await using var stream = await session.Advanced.StreamAsync(query);

        while (await stream.MoveNextAsync())
        {
            // The result is the Name field
            var result = stream.Current.Document;
            string? name = null;

            // Handle both direct string values and objects with Name property
            if (result is string nameString)
            {
                name = nameString;
            }
            else if (result != null)
            {
                // Try to get Name property dynamically
                try
                {
                    name = (result as dynamic)?.Name;
                }
                catch
                {
                    // Fallback: skip this entry
                }
            }

            if (string.IsNullOrWhiteSpace(name) == false)
            {
                names.Add(name);
            }
        }

        return names;
    }

    /// <summary>
    /// Gets the total count of documents in the Users collection.
    /// </summary>
    private static async Task<long> GetTotalUserCountAsync(IDocumentStore store)
    {
        using var session = store.OpenAsyncSession();

        // Use simple streaming count - efficient for any collection size
        var count = 0L;
        await using var stream = await session.Advanced.StreamAsync<object>(startsWith: "users/");
        while (await stream.MoveNextAsync())
        {
            count++;
        }

        return count;
    }
}

/// <summary>
/// Workload that exercises parameterized equality queries against the Users collection.
/// Queries use the pattern: FROM Users WHERE Name = $name
/// </summary>
public sealed class UsersByNameQueryWorkload : IWorkload
{
    private readonly string[] _sampleNames;
    private const string ExpectedIndexName = "Auto/Users/ByName";

    /// <summary>
    /// Creates a Users query workload using sampled names.
    /// </summary>
    /// <param name="metadata">Workload metadata containing sampled user names</param>
    public UsersByNameQueryWorkload(UsersWorkloadMetadata metadata)
    {
        if (metadata.SampleNames.Length == 0)
        {
            throw new ArgumentException("Metadata must contain sampled user names");
        }

        _sampleNames = metadata.SampleNames;
    }

    public OperationBase NextOperation(Random rng)
    {
        var name = _sampleNames[rng.Next(_sampleNames.Length)];

        return new QueryOperation
        {
            QueryText = "from Users where Name = $name",
            Parameters = new Dictionary<string, object?> { ["name"] = name },
            ExpectedIndex = ExpectedIndexName
        };
    }
}
