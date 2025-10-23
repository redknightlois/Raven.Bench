using Raven.Client.Documents;
using Raven.Client.Documents.Session;

namespace RavenBench.Core.Workload;

/// <summary>
/// Metadata for Users collection, containing sampled names for parameterized equality queries
/// and reputation histogram for range queries.
/// </summary>
public sealed class UsersWorkloadMetadata
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
        var cached = await session.LoadAsync<UsersWorkloadMetadata>(MetadataDocId);

        if (cached != null && cached.SampleNames.Length > 0)
        {
            Console.WriteLine($"[Workload] Using cached Users metadata: {cached.SampleNames.Length} sampled names");
            return cached;
        }

        Console.WriteLine("[Workload] Discovering Users names and reputation histogram by sampling database...");

        // Sample actual names from the database
        var sampleNames = await SampleUserNamesAsync(store, seed, sampleSize);

        if (sampleNames.Count == 0)
        {
            throw new InvalidOperationException(
                $"Failed to discover Users names. Found {sampleNames.Count} names. " +
                "Ensure the Users dataset is imported before running benchmarks.");
        }

        // Get actual total count of Users documents
        var totalUserCount = await GetTotalUserCountAsync(store);

        // Discover reputation histogram for range queries
        var (reputationBuckets, minReputation, maxReputation) = await DiscoverReputationHistogramAsync(store, seed);

        Console.WriteLine($"[Workload] Sampled {sampleNames.Count} unique user names from {totalUserCount} total users");
        Console.WriteLine($"[Workload] Discovered reputation range: {minReputation} to {maxReputation} across {reputationBuckets.Length} buckets");

        // Store metadata for future use
        var metadata = new UsersWorkloadMetadata
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
    /// Samples user names from the Users collection using RavenDB's random() ordering.
    /// Returns a list of actual names that exist in the database.
    /// Uses a deterministic seed for reproducibility.
    /// </summary>
    private static async Task<HashSet<string>> SampleUserNamesAsync(
        IDocumentStore store,
        int seed,
        int sampleSize)
    {
        var names = new HashSet<string>();

        using var session = store.OpenAsyncSession();

        // Use RavenDB's built-in random() ordering with a deterministic seed for reproducible sampling
        var samplingSeed = $"ravenbench-users-{seed}";
        var query = session.Advanced.AsyncRawQuery<dynamic>($"from Users order by random('{samplingSeed}') select Name limit {sampleSize}");

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

    /// <summary>
    /// Discovers reputation value distribution and creates histogram buckets for range queries.
    /// Creates buckets with varying selectivity to test different query patterns.
    /// Uses deterministic sampling to ensure reproducibility.
    /// </summary>
    private static async Task<(ReputationBucket[] buckets, int minRep, int maxRep)> DiscoverReputationHistogramAsync(
        IDocumentStore store,
        int seed)
    {
        using var session = store.OpenAsyncSession();

        // Sample reputation values using deterministic random ordering
        const int sampleSize = 10000;
        var samplingSeed = $"ravenbench-reputation-{seed}";
        var query = session.Advanced.AsyncRawQuery<dynamic>(
            $"from Users order by random('{samplingSeed}') select Reputation limit {sampleSize}");

        var reputationSamples = new List<int>();
        await using var stream = await session.Advanced.StreamAsync(query);

        while (await stream.MoveNextAsync())
        {
            var result = stream.Current.Document;
            int? reputation = null;

            // Extract Reputation value (handle different response formats)
            if (result is int repInt)
            {
                reputation = repInt;
            }
            else if (result != null)
            {
                try
                {
                    var repValue = (result as dynamic)?.Reputation;
                    if (repValue is int r)
                        reputation = r;
                    else if (repValue != null)
                        reputation = Convert.ToInt32(repValue);
                }
                catch
                {
                    // Skip entries where we can't extract reputation
                }
            }

            if (reputation.HasValue)
            {
                reputationSamples.Add(reputation.Value);
            }
        }

        if (reputationSamples.Count == 0)
        {
            // Fallback: create default buckets if we can't sample
            return (new[]
            {
                new ReputationBucket { MinReputation = 1, MaxReputation = 100, EstimatedDocCount = 1000 },
                new ReputationBucket { MinReputation = 100, MaxReputation = 1000, EstimatedDocCount = 5000 },
                new ReputationBucket { MinReputation = 1000, MaxReputation = 10000, EstimatedDocCount = 10000 }
            }, 1, 10000);
        }

        // Sort samples to compute percentiles
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

        // Create buckets with estimated doc counts based on percentile ranges
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

/// <summary>
/// Workload that exercises parameterized range queries against the Users collection.
/// Queries use the pattern: FROM Users WHERE Reputation BETWEEN $min AND $max
/// Samples from pre-computed histogram buckets to ensure varying selectivity.
/// </summary>
public sealed class UsersRangeQueryWorkload : IWorkload
{
    private readonly ReputationBucket[] _buckets;
    private const string ExpectedIndexName = "Auto/Users/ByReputation";

    /// <summary>
    /// Creates a Users range query workload using reputation histogram buckets.
    /// </summary>
    /// <param name="metadata">Workload metadata containing reputation buckets</param>
    public UsersRangeQueryWorkload(UsersWorkloadMetadata metadata)
    {
        if (metadata.ReputationBuckets.Length == 0)
        {
            throw new ArgumentException("Metadata must contain reputation histogram buckets");
        }

        _buckets = metadata.ReputationBuckets;
    }

    public OperationBase NextOperation(Random rng)
    {
        // Randomly select a bucket to query (buckets provide baseline selectivity)
        var bucket = _buckets[rng.Next(_buckets.Length)];

        // Optionally narrow the range within the bucket for more varied selectivity.
        // 50% chance to query full bucket (wider range, more results) or sub-range (narrower, fewer results)
        var useFullBucket = rng.NextDouble() < 0.5;
        int minRep, maxRep;

        if (useFullBucket || bucket.MaxReputation - bucket.MinReputation < 10)
        {
            // Use full bucket range for broader queries
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
            QueryText = "from Users where Reputation between $min and $max",
            Parameters = new Dictionary<string, object?>
            {
                ["min"] = minRep,
                ["max"] = maxRep
            },
            ExpectedIndex = ExpectedIndexName
        };
    }
}
