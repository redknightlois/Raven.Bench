using System.Net;
using Raven.Client.Documents.Indexes;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using RavenBench.Core;
using RavenBench.Core.Transport;
using RavenBench.Core.Workload;

namespace RavenBench.Dataset.Vectors;

/// <summary>
/// One product under the vector parity check: how it loads the sample and how it runs an exact search.
/// Returned ids are the sample's own base ids. <see cref="PreexistingAsync"/> describes data the product held before
/// the check, or returns null when it held none; the check loads and cleans up only a product that held none.
/// </summary>
public sealed record VectorParityProduct(
    string Name,
    Func<CancellationToken, Task<string?>> PreexistingAsync,
    Func<IReadOnlyList<BaseVector>, CancellationToken, Task> LoadAsync,
    Func<float[], int, CancellationToken, Task<IReadOnlyList<string>>> ExactSearchAsync,
    Func<Task> CleanupAsync);

/// <summary>A query whose exact result differs from the brute-force truth beyond ties.</summary>
public sealed record VectorParityMismatch(int Query, IReadOnlyList<string> Expected, IReadOnlyList<string> Actual);

/// <summary>The outcome for one product. <see cref="Failure"/> is set when the product could not run the check.</summary>
public sealed record VectorParityResult(string Product, int Compared, IReadOnlyList<VectorParityMismatch> Mismatches, string? Failure)
{
    public bool Agreed => Failure is null && Mismatches.Count == 0;
}

public sealed record VectorParityReport(int BaseCount, int QueryCount, int Dimensions, int K, VectorMetric Metric, IReadOnlyList<VectorParityResult> Results)
{
    public int ExitCode => Results.All(r => r.Agreed) ? 0 : 1;
}

/// <summary>
/// Over a seeded sample, the exact search of every product must return the float32 brute-force
/// truth for every query. Two results agree when their i-th distances match for every rank, so a
/// tie broken differently is not a mismatch.
/// </summary>
public sealed class VectorParityCheck
{
    /// <summary>RavenDB searches by cosine only, so the check runs under the metric every product serves.</summary>
    public const VectorMetric Metric = VectorMetric.Cosine;

    public const int DefaultBaseCount = 500;
    public const int DefaultQueryCount = 50;
    public const int DefaultDimensions = 32;
    public const int DefaultK = 10;

    // Relative tolerance for comparing distances computed by different engines in float32.
    private const float DistanceTolerance = 1e-4f;

    private readonly int _seed;
    private readonly int _baseCount;
    private readonly int _queryCount;
    private readonly int _dimensions;
    private readonly int _k;

    public VectorParityCheck(int seed, int baseCount, int queryCount, int dimensions, int k)
    {
        if (baseCount < k || k < 1 || queryCount < 1 || dimensions < 1)
            throw new ArgumentOutOfRangeException(nameof(baseCount), $"The sample needs k >= 1, at least k base vectors, a query and a dimension; got base {baseCount}, queries {queryCount}, dimensions {dimensions}, k {k}.");
        _seed = seed;
        _baseCount = baseCount;
        _queryCount = queryCount;
        _dimensions = dimensions;
        _k = k;
    }

    public async Task<VectorParityReport> RunAsync(IReadOnlyList<VectorParityProduct> products, CancellationToken ct)
    {
        var random = new Random(_seed);
        var baseVectors = Enumerable.Range(0, _baseCount).Select(i => new BaseVector(i.ToString(), RandomVector(random))).ToList();
        var queries = Enumerable.Range(0, _queryCount).Select(_ => RandomVector(random)).ToArray();
        var truth = await BruteForceTruth.ComputeAsync(queries, baseVectors.ToAsyncEnumerable(), Metric, _k, ct);
        var byId = baseVectors.ToDictionary(v => v.Id, v => v.Vector);

        var results = new List<VectorParityResult>(products.Count);
        foreach (var product in products)
            results.Add(await CheckAsync(product, baseVectors, queries, truth, byId, ct));

        return new VectorParityReport(_baseCount, _queryCount, _dimensions, _k, Metric, results);
    }

    private async Task<VectorParityResult> CheckAsync(VectorParityProduct product, IReadOnlyList<BaseVector> baseVectors, float[][] queries,
        string[][] truth, IReadOnlyDictionary<string, float[]> byId, CancellationToken ct)
    {
        var mismatches = new List<VectorParityMismatch>();
        if (await product.PreexistingAsync(ct) is { } preexisting)
            return new VectorParityResult(product.Name, 0, mismatches, $"{preexisting}; the check needs a target without it and leaves it in place.");

        string? failure = null;
        try
        {
            await product.LoadAsync(baseVectors, ct);
            for (int q = 0; q < queries.Length; q++)
            {
                var actual = await product.ExactSearchAsync(queries[q], _k, ct);
                if (Agrees(queries[q], truth[q], actual, byId) == false)
                    mismatches.Add(new VectorParityMismatch(q, truth[q], actual));
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            failure = $"{ex.GetType().Name}: {ex.Message}";
        }

        try
        {
            await product.CleanupAsync();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            failure = (failure is null ? "" : failure + "; ") + $"cleanup failed, {ex.GetType().Name}: {ex.Message}";
        }

        return new VectorParityResult(product.Name, failure is null ? queries.Length : 0, mismatches, failure);
    }

    /// <summary>True when the result holds k distinct known ids whose distances match the truth rank by rank.</summary>
    public static bool Agrees(float[] query, IReadOnlyList<string> expected, IReadOnlyList<string> actual, IReadOnlyDictionary<string, float[]> byId)
    {
        if (actual.Count != expected.Count || actual.Distinct().Count() != actual.Count || actual.All(byId.ContainsKey) == false)
            return false;

        var want = expected.Select(id => CosineDistance(query, byId[id])).ToArray();
        var got = actual.Select(id => CosineDistance(query, byId[id])).Order().ToArray();
        return want.Zip(got).All(p => MathF.Abs(p.First - p.Second) <= DistanceTolerance * MathF.Max(1f, MathF.Abs(p.First)));
    }

    private static float CosineDistance(float[] a, float[] b)
    {
        double dot = 0, na = 0, nb = 0;
        for (int i = 0; i < a.Length; i++)
        {
            dot += a[i] * b[i];
            na += a[i] * a[i];
            nb += b[i] * b[i];
        }
        return (float)(1 - dot / Math.Sqrt(na * nb));
    }

    private float[] RandomVector(Random random)
    {
        var v = new float[_dimensions];
        for (int i = 0; i < v.Length; i++)
            v[i] = (float)(random.NextDouble() * 2 - 1);
        return v;
    }

    /// <summary>
    /// pgvector under the check: the sample goes into an empty table through binary COPY, and the
    /// exact search runs with index scans disabled and a plan proven not to use the HNSW index.
    /// </summary>
    public static VectorParityProduct PgVector(PgVectorTransport transport) => new(
        PgVectorTransport.Target,
        async _ =>
        {
            await transport.EnsureDatabaseExistsAsync(transport.DatabaseName);
            var existing = await transport.GetDocumentCountAsync("");
            return existing == 0 ? null : $"Table '{PgVectorTransport.TableName}' already holds {existing} rows";
        },
        async (sample, ct) =>
        {
            var result = await transport.ExecuteAsync(new BulkInsertOperation<VectorRow>
            {
                Documents = sample.Select(v => new DocumentToWrite<VectorRow> { Id = v.Id, Document = new VectorRow(v.Vector, null) }).ToList()
            }, ct);
            if (result.IsSuccess == false)
                throw new InvalidOperationException($"The pgvector sample load failed: {result.ErrorDetails}");
            await transport.BuildIndexAsync(ct: ct);
        },
        (query, k, ct) => transport.ExactSearchAsync(query, k, filter: null, ct),
        () => transport.DropTableAsync());

    /// <summary>
    /// RavenDB under the check: the sample goes into a throwaway database with a float32 vector index,
    /// and the exact search is RavenDB's <c>exact(vector.search(...))</c> over raw HTTP.
    /// </summary>
    public static VectorParityProduct RavenDb(string url, string database, int dimensions)
    {
        var indexName = PublishedSetImport.IndexName(VectorQuantization.None, IndexingEngine.Corax, null, null);
        RawHttpTransport? transport = null;
        return new VectorParityProduct(
            "ravendb",
            async ct =>
            {
                using var store = HttpHelper.Create(url, database, HttpVersion.Version11);
                return await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(database), ct) == null ? null : $"Database '{database}' already exists";
            },
            async (sample, ct) =>
            {
                using var store = HttpHelper.Create(url, database, HttpVersion.Version11);
                await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(database)), ct);
                await using (var bulk = store.BulkInsert(token: ct))
                {
                    foreach (var vector in sample)
                        await bulk.StoreAsync(new { Embedding = vector.Vector }, PublishedSetImport.DocumentIdPrefix + vector.Id,
                            new Raven.Client.Json.MetadataAsDictionary { ["@collection"] = PublishedSetImport.CollectionName });
                }
                await PublishedSetImport.CreateVectorIndexAsync(store, dimensions, VectorQuantization.None, IndexingEngine.Corax, null, null);
                transport = new RawHttpTransport(url, database, CompressionMode.Identity, HttpVersion.Version11);
            },
            async (query, k, ct) =>
            {
                var result = await transport!.ExecuteAsync(new VectorSearchOperation
                {
                    QueryVector = query,
                    FieldName = "Vector",
                    TopK = k,
                    UseExactSearch = true,
                    ExpectedIndex = indexName
                }, ct);
                if (result.IsSuccess == false)
                    throw new InvalidOperationException($"The RavenDB exact search failed: {result.ErrorDetails}");
                return result.NeighborIds!.Select(id => id[PublishedSetImport.DocumentIdPrefix.Length..]).ToList();
            },
            async () =>
            {
                transport?.Dispose();
                using var store = HttpHelper.Create(url, database, HttpVersion.Version11);
                await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(database, hardDelete: true));
            });
    }
}
