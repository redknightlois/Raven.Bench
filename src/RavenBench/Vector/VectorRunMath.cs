using RavenBench.Core.Reporting;
using RavenBench.Core;
using RavenBench.Core.Workload;
using RavenBench.Dataset.Vectors;

namespace RavenBench.VectorBench;

/// <summary>The truth of a query set holds fewer than k neighbours once the insert slice is removed from it.</summary>
public sealed class InsufficientTruthDepthException(int query, int remaining, int k)
    : InvalidOperationException($"Truth for query {query} keeps {remaining} neighbours after the insert slice is removed; the run needs {k}. Raise the scenario key 'TruthDepth'.");

/// <summary>
/// How the base stream of a set splits into the loaded vectors and the insert slice, and which
/// loaded vectors carry the filter label. Positions count the set's base stream, after the query
/// hold-out. Every draw uses the scenario seed.
/// </summary>
public sealed class VectorSplit
{
    public const string LabelIn = "in";
    public const string LabelOut = "out";

    private readonly HashSet<long> _slice;
    private readonly HashSet<long> _labelled;

    public long BaseCount { get; }
    public long LoadedCount => BaseCount - _slice.Count;
    public long LabelledCount => _labelled.Count;

    public VectorSplit(long baseCount, int seed, int insertCount, double selectivity)
    {
        if (insertCount < 1 || insertCount >= baseCount)
            throw new ArgumentOutOfRangeException(nameof(insertCount), insertCount, $"The insert slice needs 1 to {baseCount - 1} vectors.");
        BaseCount = baseCount;
        _slice = new HashSet<long>(QueryDraw.Positions(baseCount, new QuerySelection(SeedMixer.Derive(seed, "insert-slice"), insertCount)));
        var labelCount = (int)Math.Round((baseCount - insertCount) * selectivity);
        if (labelCount < 1)
            throw new ArgumentOutOfRangeException(nameof(selectivity), selectivity, "The filter selectivity labels no vector of this set.");
        _labelled = new HashSet<long>(QueryDraw.Positions(baseCount - insertCount, new QuerySelection(SeedMixer.Derive(seed, "filter-labels"), labelCount)));
    }

    public bool IsInsertSlice(long basePosition) => _slice.Contains(basePosition);

    /// <summary>The label of the loaded vector at the given loaded ordinal.</summary>
    public string LabelOf(long loadedOrdinal) => _labelled.Contains(loadedOrdinal) ? LabelIn : LabelOut;
}

public static class VectorRunMath
{
    /// <summary>
    /// The lowest effort value whose measured recall reaches the threshold, taken in effort order
    /// whatever order the scenario lists the settings in; null when no setting reaches it.
    /// </summary>
    public static VectorEffortPoint? SelectLowest(IEnumerable<VectorEffortPoint> curve, double threshold) =>
        curve.OrderBy(p => p.Value).FirstOrDefault(p => p.Recall >= threshold);

    /// <summary>The truth with every insert-slice id removed, cut to k per query.</summary>
    public static string[][] WithoutSlice(IReadOnlyList<string[]> truth, IReadOnlySet<string> sliceIds, int k) =>
        truth.Select((neighbours, q) =>
        {
            var kept = neighbours.Where(id => sliceIds.Contains(id) == false).Take(k).ToArray();
            return kept.Length == k ? kept : throw new InsufficientTruthDepthException(q, kept.Length, k);
        }).ToArray();

    /// <summary>
    /// For each prefix length n, the exact top k of the loaded base plus the first n slice vectors.
    /// The loaded base contributes only its own top k, which is exact because the top k of a union
    /// lies within the union of the parts' top k. One pass over the slice serves every prefix.
    /// </summary>
    public static Dictionary<long, string[]> TruthWithInserts(float[] query, IReadOnlyList<BaseVector> quietTopK, IReadOnlyList<BaseVector> slice, IEnumerable<long> prefixes, VectorMetric metric, int k)
    {
        var q = BruteForceTruth.Prepare(query, metric);
        var best = quietTopK.Select((v, i) => (v.Id, Score: BruteForceTruth.Score(q, BruteForceTruth.Prepare(v.Vector, metric), metric), Order: (long)i)).ToList();
        var result = new Dictionary<long, string[]>();
        long taken = 0;
        foreach (var n in prefixes.Distinct().Order())
        {
            if (n > slice.Count)
                throw new ArgumentOutOfRangeException(nameof(prefixes), n, $"The slice holds {slice.Count} vectors.");
            for (; taken < n; taken++)
            {
                var v = slice[(int)taken];
                best.Add((v.Id, BruteForceTruth.Score(q, BruteForceTruth.Prepare(v.Vector, metric), metric), quietTopK.Count + taken));
            }
            best = best.OrderByDescending(c => c.Score).ThenBy(c => c.Order).Take(k).ToList();
            result[n] = best.Select(c => c.Id).ToArray();
        }
        return result;
    }

    /// <summary>recall@k of one result against one truth list; a short result counts k truth ids.</summary>
    public static double Recall(IReadOnlyList<string> returned, IReadOnlyList<string> truth, int k)
    {
        var top = returned.Take(k).ToHashSet(StringComparer.Ordinal);
        return (double)truth.Take(k).Count(top.Contains) / k;
    }
}
