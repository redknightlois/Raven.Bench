using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;

namespace RavenBench.Core.Metrics;

/// <summary>
/// Thread-safe accumulator for per-query result metadata. Captured so query and vector benchmarks
/// can prove queries did real work — non-empty result sets, non-stale indexes, the expected index —
/// rather than reporting fast latencies for queries that silently returned nothing.
/// </summary>
public sealed class QueryStats
{
    private readonly ConcurrentDictionary<string, long> _indexUsage = new();
    private long _queryOps;
    private long _totalResults;
    private long _staleQueries;
    private int _minResultCount = int.MaxValue;
    private int _maxResultCount = int.MinValue;

    public void Record(string? indexName, int? resultCount, bool? isStale)
    {
        if (indexName == null && resultCount.HasValue == false && isStale.HasValue == false)
            return;

        Interlocked.Increment(ref _queryOps);

        if (indexName != null)
            _indexUsage.AddOrUpdate(indexName, 1, (_, count) => count + 1);

        if (resultCount.HasValue)
        {
            var value = resultCount.Value;
            Interlocked.Add(ref _totalResults, value);
            InterlockedMin(ref _minResultCount, value);
            InterlockedMax(ref _maxResultCount, value);
        }

        if (isStale == true)
            Interlocked.Increment(ref _staleQueries);
    }

    public QueryStatsSnapshot Snapshot()
    {
        var queryOps = Volatile.Read(ref _queryOps);
        if (queryOps == 0)
            return QueryStatsSnapshot.Empty;

        var min = Volatile.Read(ref _minResultCount);
        var max = Volatile.Read(ref _maxResultCount);
        return new QueryStatsSnapshot(
            queryOps,
            new Dictionary<string, long>(_indexUsage),
            Volatile.Read(ref _totalResults),
            min == int.MaxValue ? null : min,
            max == int.MinValue ? null : max,
            Volatile.Read(ref _staleQueries));
    }

    private static void InterlockedMin(ref int location, int value)
    {
        int current;
        do
        {
            current = Volatile.Read(ref location);
            if (value >= current)
                return;
        } while (Interlocked.CompareExchange(ref location, value, current) != current);
    }

    private static void InterlockedMax(ref int location, int value)
    {
        int current;
        do
        {
            current = Volatile.Read(ref location);
            if (value <= current)
                return;
        } while (Interlocked.CompareExchange(ref location, value, current) != current);
    }
}

/// <summary>
/// Immutable view of accumulated query metadata for one step.
/// </summary>
public sealed class QueryStatsSnapshot
{
    public static readonly QueryStatsSnapshot Empty =
        new(0, new Dictionary<string, long>(), 0, null, null, 0);

    public long QueryOperations { get; }
    public IReadOnlyDictionary<string, long> IndexUsage { get; }
    public long TotalResults { get; }
    public int? MinResultCount { get; }
    public int? MaxResultCount { get; }
    public long StaleQueries { get; }

    public bool HasQueries => QueryOperations > 0;
    public double? AvgResultCount => HasQueries ? (double)TotalResults / QueryOperations : null;

    public QueryStatsSnapshot(
        long queryOperations,
        IReadOnlyDictionary<string, long> indexUsage,
        long totalResults,
        int? minResultCount,
        int? maxResultCount,
        long staleQueries)
    {
        QueryOperations = queryOperations;
        IndexUsage = indexUsage;
        TotalResults = totalResults;
        MinResultCount = minResultCount;
        MaxResultCount = maxResultCount;
        StaleQueries = staleQueries;
    }
}
