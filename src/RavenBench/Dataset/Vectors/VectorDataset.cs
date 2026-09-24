using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using RavenBench.Core.Workload;

namespace RavenBench.Dataset.Vectors;

/// <summary>
/// The queries of one run: <see cref="Count"/> query vectors drawn from the set with <see cref="Seed"/>.
/// </summary>
public sealed record QuerySelection
{
    public int Seed { get; }
    public int Count { get; }

    public QuerySelection(int seed, int count)
    {
        if (count <= 0)
            throw new ArgumentOutOfRangeException(nameof(count), count, "The query count must be positive.");
        Seed = seed;
        Count = count;
    }
}

/// <summary>
/// One base vector. <see cref="Id"/> is the set's own key; a loader derives the document key from it
/// and recall maps a returned key back to it.
/// </summary>
public sealed record BaseVector(string Id, float[] Vector);

/// <summary>
/// The query vectors of a selection and their true neighbours: base ids, nearest first, at least k per query.
/// </summary>
public sealed record VectorQuerySet(float[][] Queries, string[][] Neighbors);

/// <summary>
/// The one dataset path. Every vector set yields its base vectors, its query vectors with their
/// product-neutral true neighbours, its metric and its dimensions, from files verified by checksum.
/// </summary>
public interface IVectorDataset
{
    string Name { get; }
    VectorMetric Metric { get; }
    int Dimensions { get; }
    IReadOnlyList<DatasetFile> Files { get; }

    /// <summary>
    /// The selected queries and their true neighbours at depth k.
    /// </summary>
    Task<VectorQuerySet> GetQueriesAsync(VerifiedFiles files, QuerySelection selection, int k, CancellationToken ct = default);

    /// <summary>
    /// The base vectors to load. A set without a published query split omits the held-out queries of the selection.
    /// </summary>
    IAsyncEnumerable<BaseVector> ReadBaseAsync(VerifiedFiles files, QuerySelection selection, CancellationToken ct = default);

    /// <summary>
    /// The number of base vectors a load of this selection writes.
    /// </summary>
    Task<long> BaseCountAsync(VerifiedFiles files, QuerySelection selection, CancellationToken ct = default);
}

/// <summary>
/// Draws query positions with the scenario seed.
/// </summary>
public static class QueryDraw
{
    /// <summary>
    /// <paramref name="count"/> distinct positions in [0, total), in draw order, by Floyd's sampling.
    /// </summary>
    public static long[] Positions(long total, QuerySelection selection)
    {
        if (selection.Count > total)
            throw new ArgumentOutOfRangeException(nameof(selection), $"Cannot draw {selection.Count} queries from {total} rows.");

        var rng = new Random(selection.Seed);
        var chosen = new HashSet<long>(selection.Count);
        var order = new List<long>(selection.Count);
        for (long j = total - selection.Count; j < total; j++)
        {
            long t = rng.NextInt64(j + 1);
            var pick = chosen.Add(t) ? t : j;
            if (pick == j)
                chosen.Add(j);
            order.Add(pick);
        }
        return order.ToArray();
    }
}

/// <summary>
/// A set that ships no query split. The selection's queries are held out of the base vectors, and the
/// truth is exact float32 brute force over the remaining base vectors, cached next to the data.
/// </summary>
public abstract class HeldOutVectorDataset : IVectorDataset
{
    public abstract string Name { get; }
    public abstract VectorMetric Metric { get; }
    public abstract int Dimensions { get; }
    public abstract IReadOnlyList<DatasetFile> Files { get; }

    /// <summary>
    /// The number of rows queries are drawn from; rows past it are never read.
    /// </summary>
    protected abstract Task<long> CountRowsAsync(VerifiedFiles files, CancellationToken ct);

    /// <summary>
    /// Every row in file order.
    /// </summary>
    protected abstract IAsyncEnumerable<BaseVector> ReadRowsAsync(VerifiedFiles files, CancellationToken ct);

    public async Task<long> BaseCountAsync(VerifiedFiles files, QuerySelection selection, CancellationToken ct = default) =>
        await CountRowsAsync(files, ct).ConfigureAwait(false) - selection.Count;

    public async Task<HashSet<long>> HeldOutPositionsAsync(VerifiedFiles files, QuerySelection selection, CancellationToken ct = default) =>
        new(QueryDraw.Positions(await CountRowsAsync(files, ct).ConfigureAwait(false), selection));

    public async Task<VectorQuerySet> GetQueriesAsync(VerifiedFiles files, QuerySelection selection, int k, CancellationToken ct = default)
    {
        var positions = QueryDraw.Positions(await CountRowsAsync(files, ct).ConfigureAwait(false), selection);
        var slot = new Dictionary<long, int>(positions.Length);
        for (int i = 0; i < positions.Length; i++)
            slot[positions[i]] = i;

        var queries = new float[positions.Length][];
        long position = 0;
        await foreach (var row in ReadRowsAsync(files, ct).ConfigureAwait(false))
        {
            if (slot.TryGetValue(position++, out var i))
                queries[i] = row.Vector;
        }
        if (queries.Any(q => q == null))
            throw new InvalidDataException($"Set '{Name}' holds fewer rows than it declared.");

        var cachePath = TruthCache.PathFor(files, Name, Metric, selection, k);
        var truth = TruthCache.TryLoad(cachePath, positions.Length, k);
        if (truth == null)
        {
            Console.WriteLine($"[Dataset] {Name}: computing exact float32 truth for {positions.Length} queries at k={k}");
            truth = await BruteForceTruth.ComputeAsync(queries, ReadBaseAsync(files, selection, ct), Metric, k, ct).ConfigureAwait(false);
            TruthCache.Store(cachePath, truth);
        }
        return new VectorQuerySet(queries, truth);
    }

    public async IAsyncEnumerable<BaseVector> ReadBaseAsync(VerifiedFiles files, QuerySelection selection, [EnumeratorCancellation] CancellationToken ct = default)
    {
        var total = await CountRowsAsync(files, ct).ConfigureAwait(false);
        var heldOut = new HashSet<long>(QueryDraw.Positions(total, selection));
        long position = 0;
        await foreach (var row in ReadRowsAsync(files, ct).ConfigureAwait(false))
        {
            if (position >= total)
                yield break;
            if (heldOut.Contains(position++) == false)
                yield return row;
        }
    }
}

/// <summary>
/// The first <see cref="Cap"/> base vectors of a set. The queries are the set's own; their truth is
/// exact float32 brute force over the capped base, cached next to the data under the cap.
/// </summary>
public sealed class CappedVectorDataset(IVectorDataset inner, int cap) : IVectorDataset
{
    public int Cap { get; } = cap > 0 ? cap : throw new ArgumentOutOfRangeException(nameof(cap), cap, "The cap must be positive.");
    public string Name => inner.Name;
    public VectorMetric Metric => inner.Metric;
    public int Dimensions => inner.Dimensions;
    public IReadOnlyList<DatasetFile> Files => inner.Files;

    public async Task<long> BaseCountAsync(VerifiedFiles files, QuerySelection selection, CancellationToken ct = default) =>
        Math.Min(Cap, await inner.BaseCountAsync(files, selection, ct).ConfigureAwait(false));

    public IAsyncEnumerable<BaseVector> ReadBaseAsync(VerifiedFiles files, QuerySelection selection, CancellationToken ct = default) =>
        inner.ReadBaseAsync(files, selection, ct).Take(Cap);

    // The inner set's truth is read and discarded; for a held-out set that is one full brute force, cached.
    public async Task<VectorQuerySet> GetQueriesAsync(VerifiedFiles files, QuerySelection selection, int k, CancellationToken ct = default)
    {
        var queries = (await inner.GetQueriesAsync(files, selection, k, ct).ConfigureAwait(false)).Queries;
        var cachePath = TruthCache.PathFor(files, $"{Name}-cap{Cap}", Metric, selection, k);
        var truth = TruthCache.TryLoad(cachePath, queries.Length, k);
        if (truth == null)
        {
            Console.WriteLine($"[Dataset] {Name}: computing exact float32 truth over the first {Cap} base vectors for {queries.Length} queries at k={k}");
            truth = await BruteForceTruth.ComputeAsync(queries, ReadBaseAsync(files, selection, ct), Metric, k, ct).ConfigureAwait(false);
            TruthCache.Store(cachePath, truth);
        }
        return new VectorQuerySet(queries, truth);
    }
}

/// <summary>
/// Exact nearest neighbours by float32 brute force, computed outside any product.
/// </summary>
public static class BruteForceTruth
{
    private const int BlockSize = 4096;

    /// <summary>
    /// The k nearest base ids per query, nearest first. An equal score ranks the earlier base vector first.
    /// </summary>
    public static async Task<string[][]> ComputeAsync(float[][] queries, IAsyncEnumerable<BaseVector> baseVectors, VectorMetric metric, int k, CancellationToken ct = default)
    {
        if (k <= 0)
            throw new ArgumentOutOfRangeException(nameof(k), k, "k must be positive.");

        var preparedQueries = queries.Select(q => Prepare(q, metric)).ToArray();
        var heaps = queries.Select(_ => new PriorityQueue<long, (float Score, long Ordinal)>(k + 1, WorstFirst.Instance)).ToArray();
        var ids = new List<string>();
        var block = new List<float[]>(BlockSize);

        void Flush()
        {
            var first = ids.Count - block.Count;
            Parallel.For(0, preparedQueries.Length, qi =>
            {
                var heap = heaps[qi];
                for (int b = 0; b < block.Count; b++)
                {
                    long ordinal = first + b;
                    var candidate = (Score(preparedQueries[qi], block[b], metric), ordinal);
                    if (heap.Count < k)
                        heap.Enqueue(ordinal, candidate);
                    else if (heap.TryPeek(out _, out var worst) && WorstFirst.Instance.Compare(candidate, worst) > 0)
                        heap.EnqueueDequeue(ordinal, candidate);
                }
            });
            block.Clear();
        }

        await foreach (var vector in baseVectors.WithCancellation(ct).ConfigureAwait(false))
        {
            if (vector.Vector.Length != queries[0].Length)
                throw new InvalidDataException($"Base vector '{vector.Id}' has {vector.Vector.Length} dimensions; queries have {queries[0].Length}.");
            ids.Add(vector.Id);
            block.Add(Prepare(vector.Vector, metric));
            if (block.Count == BlockSize)
                Flush();
        }
        Flush();

        if (ids.Count < k)
            throw new InvalidDataException($"Truth at k={k} needs at least {k} base vectors; the set yielded {ids.Count}.");

        return heaps.Select(heap =>
        {
            var nearest = new string[heap.Count];
            for (int i = nearest.Length - 1; i >= 0; i--)
                nearest[i] = ids[(int)heap.Dequeue()];
            return nearest;
        }).ToArray();
    }

    // Cosine normalises once, so the score is a plain dot product.
    internal static float[] Prepare(float[] vector, VectorMetric metric)
    {
        if (metric != VectorMetric.Cosine)
            return vector;
        var norm = MathF.Sqrt(Dot(vector, vector));
        return norm == 0 ? vector : vector.Select(v => v / norm).ToArray();
    }

    // Higher is nearer under every metric.
    internal static float Score(float[] q, float[] b, VectorMetric metric) => metric switch
    {
        VectorMetric.Cosine or VectorMetric.Dot => Dot(q, b),
        VectorMetric.L2 => -SquaredDistance(q, b),
        _ => throw new NotSupportedException($"Metric {metric} has no brute-force score.")
    };

    private static float Dot(ReadOnlySpan<float> a, ReadOnlySpan<float> b)
    {
        var va = MemoryMarshal.Cast<float, Vector<float>>(a);
        var vb = MemoryMarshal.Cast<float, Vector<float>>(b);
        var acc = Vector<float>.Zero;
        for (int i = 0; i < va.Length; i++)
            acc += va[i] * vb[i];
        var sum = Vector.Sum(acc);
        for (int i = va.Length * Vector<float>.Count; i < a.Length; i++)
            sum += a[i] * b[i];
        return sum;
    }

    private static float SquaredDistance(ReadOnlySpan<float> a, ReadOnlySpan<float> b)
    {
        var va = MemoryMarshal.Cast<float, Vector<float>>(a);
        var vb = MemoryMarshal.Cast<float, Vector<float>>(b);
        var acc = Vector<float>.Zero;
        for (int i = 0; i < va.Length; i++)
        {
            var d = va[i] - vb[i];
            acc += d * d;
        }
        var sum = Vector.Sum(acc);
        for (int i = va.Length * Vector<float>.Count; i < a.Length; i++)
        {
            var d = a[i] - b[i];
            sum += d * d;
        }
        return sum;
    }

    // Orders candidates worst first: a lower score, or an equal score from a later base vector.
    private sealed class WorstFirst : IComparer<(float Score, long Ordinal)>
    {
        public static readonly WorstFirst Instance = new();

        public int Compare((float Score, long Ordinal) x, (float Score, long Ordinal) y)
        {
            var byScore = x.Score.CompareTo(y.Score);
            return byScore != 0 ? byScore : y.Ordinal.CompareTo(x.Ordinal);
        }
    }
}

/// <summary>
/// Brute-force truth cached next to the data, keyed by the set's checksums, the metric, the query selection and k.
/// </summary>
public static class TruthCache
{
    private const string Magic = "RavenBench.Truth.v1";

    public static string PathFor(VerifiedFiles files, string setName, VectorMetric metric, QuerySelection selection, int k)
    {
        var key = $"{files.Fingerprint}|{metric}|seed={selection.Seed}|count={selection.Count}|k={k}";
        var hash = Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(key)))[..16];
        return Path.Combine(files.Directory, $"truth-{setName}-seed{selection.Seed}-q{selection.Count}-k{k}-{hash}.bin");
    }

    public static string[][]? TryLoad(string path, int queryCount, int k)
    {
        if (File.Exists(path) == false)
            return null;
        using var reader = new BinaryReader(File.OpenRead(path), Encoding.UTF8);
        if (reader.ReadString() != Magic || reader.ReadInt32() != queryCount)
            throw new InvalidDataException($"Truth cache '{path}' does not match its key; delete it to recompute.");
        var truth = new string[queryCount][];
        for (int q = 0; q < queryCount; q++)
        {
            truth[q] = new string[reader.ReadInt32()];
            if (truth[q].Length < k)
                throw new InvalidDataException($"Truth cache '{path}' holds fewer than {k} neighbours; delete it to recompute.");
            for (int i = 0; i < truth[q].Length; i++)
                truth[q][i] = reader.ReadString();
        }
        return truth;
    }

    public static void Store(string path, string[][] truth)
    {
        var temp = $"{path}.{Guid.NewGuid():N}.writing";
        using (var writer = new BinaryWriter(File.Create(temp), Encoding.UTF8))
        {
            writer.Write(Magic);
            writer.Write(truth.Length);
            foreach (var neighbours in truth)
            {
                writer.Write(neighbours.Length);
                foreach (var id in neighbours)
                    writer.Write(id);
            }
        }
        File.Move(temp, path, overwrite: true);
    }
}
