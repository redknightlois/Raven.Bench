using Raven.Client.Documents;
using Raven.Client.Documents.Session;

namespace RavenBench.Core.Workload;

/// <summary>
/// Metadata for Users collection (from StackOverflow dataset), containing sampled names for parameterized equality queries
/// and reputation histogram for range queries.
/// </summary>
public sealed class StackOverflowUsersWorkloadMetadata
{
    public string[] SampleNames { get; set; } = Array.Empty<string>();
    public long SampleCount { get; set; }
    public long TotalUserCount { get; set; }
    public DateTime ComputedAt { get; set; }

    // Reputation histogram for range queries: bucketed reputation ranges
    // Each bucket contains [min, max] reputation values
    public ReputationBucket[] ReputationBuckets { get; set; } = Array.Empty<ReputationBucket>();
    public int MinReputation { get; set; }
    public int MaxReputation { get; set; }

    // Static index names, populated at runtime (include the engine suffix)
    public string? DisplayNameIndexName { get; set; }
    public string? ReputationIndexName { get; set; }
    public string? SpatialIndexName { get; set; }
}

/// <summary>
/// Represents a reputation range bucket for sampling range query parameters.
/// Buckets are designed to provide varying selectivity (different result set sizes).
/// </summary>
public sealed class ReputationBucket
{
    public int MinReputation { get; set; }
    public int MaxReputation { get; set; }
    public long EstimatedDocCount { get; set; } // Approximate number of users in this range
}

/// <summary>
/// Helper for discovering and caching StackOverflow Users workload metadata (sampled names).
/// </summary>
public static class StackOverflowUsersWorkloadHelper
{
    private const string MetadataDocId = "workload/users-metadata";
    private const int DefaultSampleSize = 10000;

    /// <summary>
    /// Discovers actual user names by sampling the database and caches them for workload use.
    /// Returns sampled names that exist in the database.
    /// </summary>
    public static async Task<StackOverflowUsersWorkloadMetadata> DiscoverOrLoadMetadataAsync(
        string serverUrl,
        string databaseName,
        int seed,
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
        var cached = await session.LoadAsync<StackOverflowUsersWorkloadMetadata>(MetadataDocId);

        if (cached != null && cached.SampleNames.Length > 0)
        {
            Console.WriteLine($"[Workload] Using cached Users metadata: {cached.SampleNames.Length} sampled names");
            return cached;
        }

        Console.WriteLine("[Workload] Discovering Users names and reputation histogram by sampling database...");

        var users = await StackOverflowWorkloadHelper.SampleExistingDocsAsync(store, "users", maxUserId, seed, sampleSize);

        var sampleNames = new HashSet<string>();
        var reputationSamples = new List<int>();
        foreach (var (_, doc) in users)
        {
            try { string? name = (doc as dynamic)?.DisplayName; if (string.IsNullOrWhiteSpace(name) == false) sampleNames.Add(name!); } catch { }
            try { var rep = (doc as dynamic)?.Reputation; if (rep != null) reputationSamples.Add(Convert.ToInt32(rep)); } catch { }
        }

        if (sampleNames.Count == 0)
        {
            throw new InvalidOperationException(
                $"Failed to discover Users names. Found {sampleNames.Count} names. " +
                "Ensure the StackOverflow dataset is imported before running benchmarks.");
        }

        var totalUserCount = await GetTotalUserCountAsync(store);

        var (reputationBuckets, minReputation, maxReputation) = BuildReputationHistogram(reputationSamples);

        Console.WriteLine($"[Workload] Sampled {sampleNames.Count} unique user names from {totalUserCount} total users");
        Console.WriteLine($"[Workload] Discovered reputation range: {minReputation} to {maxReputation} across {reputationBuckets.Length} buckets");

        var metadata = new StackOverflowUsersWorkloadMetadata
        {
            SampleNames = sampleNames.ToArray(),
            SampleCount = sampleNames.Count,
            TotalUserCount = totalUserCount,
            ReputationBuckets = reputationBuckets,
            MinReputation = minReputation,
            MaxReputation = maxReputation,
            ComputedAt = DateTime.UtcNow
        };

        await session.StoreAsync(metadata, MetadataDocId);
        await session.SaveChangesAsync();
        Console.WriteLine("[Workload] Stored Users workload metadata in database");

        return metadata;
    }

    /// <summary>
    /// Gets the total count of documents in the Users collection.
    /// </summary>
    private static async Task<long> GetTotalUserCountAsync(IDocumentStore store)
    {
        using var session = store.OpenAsyncSession();

        var query = session.Advanced.AsyncRawQuery<dynamic>("from Users").Statistics(out var stats).Take(0);
        await query.ToListAsync();

        return stats.TotalResults;
    }

    /// <summary>
    /// Builds reputation histogram buckets from sampled reputation values. Buckets follow
    /// percentile boundaries (0-25-50-75-90-100) to give range queries varying selectivity.
    /// </summary>
    private static (ReputationBucket[] buckets, int minRep, int maxRep) BuildReputationHistogram(
        List<int> reputationSamples)
    {
        var sampleSize = reputationSamples.Count;

        if (reputationSamples.Count == 0)
        {
            return (new[]
            {
                new ReputationBucket { MinReputation = 1, MaxReputation = 100, EstimatedDocCount = 1000 },
                new ReputationBucket { MinReputation = 100, MaxReputation = 1000, EstimatedDocCount = 5000 },
                new ReputationBucket { MinReputation = 1000, MaxReputation = 10000, EstimatedDocCount = 10000 }
            }, 1, 10000);
        }

        reputationSamples.Sort();
        var minRep = reputationSamples[0];
        var maxRep = reputationSamples[^1];

        // Create buckets based on percentiles to ensure varying selectivity
        // Buckets: 0-25th, 25-50th, 50-75th, 75-90th, 90-100th percentiles
        // This ensures we have both wide ranges (many results) and narrow ranges (few results)
        var buckets = new List<ReputationBucket>();

        var p25 = reputationSamples[(int)(reputationSamples.Count * 0.25)];
        var p50 = reputationSamples[(int)(reputationSamples.Count * 0.50)];
        var p75 = reputationSamples[(int)(reputationSamples.Count * 0.75)];
        var p90 = reputationSamples[(int)(reputationSamples.Count * 0.90)];

        buckets.Add(new ReputationBucket
        {
            MinReputation = minRep,
            MaxReputation = p25,
            EstimatedDocCount = sampleSize / 4  // ~25% of samples
        });

        buckets.Add(new ReputationBucket
        {
            MinReputation = p25,
            MaxReputation = p50,
            EstimatedDocCount = sampleSize / 4  // ~25% of samples
        });

        buckets.Add(new ReputationBucket
        {
            MinReputation = p50,
            MaxReputation = p75,
            EstimatedDocCount = sampleSize / 4  // ~25% of samples
        });

        buckets.Add(new ReputationBucket
        {
            MinReputation = p75,
            MaxReputation = p90,
            EstimatedDocCount = sampleSize * 15 / 100  // ~15% of samples
        });

        buckets.Add(new ReputationBucket
        {
            MinReputation = p90,
            MaxReputation = maxRep,
            EstimatedDocCount = sampleSize / 10  // ~10% of samples
        });

        return (buckets.ToArray(), minRep, maxRep);
    }
}

/// <summary>
/// Workload that exercises parameterized equality queries against the Users collection (from StackOverflow dataset).
/// Queries use the pattern: FROM Users WHERE DisplayName = $name
/// </summary>
public sealed class StackOverflowUsersByNameQueryWorkload : IWorkload
{
    private readonly string[] _sampleNames;
    private readonly string _expectedIndexName;

    /// <summary>
    /// Creates a Users query workload using sampled names.
    /// </summary>
    /// <param name="metadata">Workload metadata containing sampled user names and static index name</param>
    public StackOverflowUsersByNameQueryWorkload(StackOverflowUsersWorkloadMetadata metadata)
    {
        if (metadata.SampleNames.Length == 0)
        {
            throw new ArgumentException("Metadata must contain sampled user names");
        }

        _sampleNames = metadata.SampleNames;
        _expectedIndexName = metadata.DisplayNameIndexName
            ?? throw new ArgumentException("Metadata must contain DisplayNameIndexName for static index");
    }

    public OperationBase NextOperation(Random rng)
    {
        var name = _sampleNames[rng.Next(_sampleNames.Length)];

        return new QueryOperation
        {
            QueryText = $"from index '{_expectedIndexName}' where DisplayName = $name",
            Parameters = new Dictionary<string, object?> { ["name"] = name },
            ExpectedIndex = _expectedIndexName
        };
    }
}

/// <summary>
/// Workload that exercises parameterized range queries against the Users collection (from StackOverflow dataset).
/// Queries use the pattern: FROM Users WHERE Reputation BETWEEN $min AND $max
/// Samples from pre-computed histogram buckets to ensure varying selectivity.
/// </summary>
public sealed class StackOverflowUsersRangeQueryWorkload : IWorkload
{
    private readonly ReputationBucket[] _buckets;
    private readonly string _expectedIndexName;

    /// <summary>
    /// Creates a Users range query workload using reputation histogram buckets.
    /// </summary>
    /// <param name="metadata">Workload metadata containing reputation buckets and static index name</param>
    public StackOverflowUsersRangeQueryWorkload(StackOverflowUsersWorkloadMetadata metadata)
    {
        if (metadata.ReputationBuckets.Length == 0)
        {
            throw new ArgumentException("Metadata must contain reputation histogram buckets");
        }

        _buckets = metadata.ReputationBuckets;
        _expectedIndexName = metadata.ReputationIndexName
            ?? throw new ArgumentException("Metadata must contain ReputationIndexName for static index");
    }

    public OperationBase NextOperation(Random rng)
    {
        var bucket = _buckets[rng.Next(_buckets.Length)];

        // Optionally narrow the range within the bucket for more varied selectivity.
        // 50% chance to query full bucket (wider range, more results) or sub-range (narrower, fewer results)
        var useFullBucket = rng.NextDouble() < 0.5;
        int minRep, maxRep;

        if (useFullBucket || bucket.MaxReputation - bucket.MinReputation < 10)
        {
            minRep = bucket.MinReputation;
            maxRep = bucket.MaxReputation;
        }
        else
        {
            // Use a random sub-range within the bucket (25-50% of bucket size) for higher selectivity
            var rangeSize = bucket.MaxReputation - bucket.MinReputation;
            var subRangeSize = rng.Next(rangeSize / 4, rangeSize / 2);
            minRep = rng.Next(bucket.MinReputation, bucket.MaxReputation - subRangeSize + 1);
            maxRep = minRep + subRangeSize;
        }

        return new QueryOperation
        {
            QueryText = $"from index '{_expectedIndexName}' where Reputation between $min and $max",
            Parameters = new Dictionary<string, object?>
            {
                ["min"] = minRep,
                ["max"] = maxRep
            },
            ExpectedIndex = _expectedIndexName
        };
    }
}
