using System.Diagnostics;
using Raven.Client.Documents;
using RavenBench.Core;
using RavenBench.Core.Metrics;
using RavenBench.Core.Workload;
using Sparrow.Json;

namespace RavenBench.Dataset;

/// <summary>
/// Measures recall@K for vector search by comparing approximate (HNSW) results
/// against brute-force exact nearest neighbor ground truth.
/// Ground truth is computed once and cached in the target database.
/// </summary>
public sealed class RecallMeasurement
{
    private const string GroundTruthDocId = "benchmark/ground-truth";

    /// <summary>
    /// Runs the full recall measurement: compute or load ground truth, then measure recall
    /// at each requested K cutoff.
    /// </summary>
    /// <summary>
    /// Runs recall measurement at a single efSearch value.
    /// </summary>
    public async Task<RecallResult> MeasureAsync(
        string serverUrl,
        string databaseName,
        VectorWorkloadMetadata metadata,
        int[] recallKs,
        VectorQuantization quantization,
        IndexingEngine searchEngine,
        Version? httpVersion = null,
        int? efSearch = null)
    {
        if (metadata.IndexName == null)
            throw new InvalidOperationException("VectorWorkloadMetadata.IndexName must be set for recall measurement.");

        var maxK = recallKs.Max();

        using var store = new DocumentStore { Urls = [serverUrl], Database = databaseName };
        if (httpVersion != null)
            HttpHelper.ConfigureHttpVersion(store, httpVersion, HttpVersionPolicy.RequestVersionExact);
        store.Initialize();

        // Step 1: Compute or load ground truth
        var queryFingerprint = ComputeQueryFingerprint(metadata);
        var (groundTruth, cached, groundTruthTime) = await GetGroundTruthAsync(store, metadata, maxK, quantization, searchEngine, queryFingerprint);

        // Step 2: Run approximate search and compute recall
        var measureSw = Stopwatch.StartNew();
        var recallAtK = await ComputeRecallAsync(store, metadata, groundTruth, recallKs, maxK, quantization, searchEngine, efSearch);
        measureSw.Stop();

        return new RecallResult
        {
            RecallAtK = recallAtK,
            QueryCount = metadata.QueryVectorCount,
            GroundTruthDepth = maxK,
            GroundTruthCached = cached,
            GroundTruthComputeTime = groundTruthTime,
            MeasurementTime = measureSw.Elapsed
        };
    }

    /// <summary>
    /// Runs recall measurement sweeping multiple efSearch values. Returns one RecallResult per efSearch.
    /// Ground truth is computed once and reused.
    /// </summary>
    public async Task<Dictionary<int, RecallResult>> MeasureSweepAsync(
        string serverUrl,
        string databaseName,
        VectorWorkloadMetadata metadata,
        int[] recallKs,
        int[] efSearchValues,
        VectorQuantization quantization,
        IndexingEngine searchEngine,
        Version? httpVersion = null)
    {
        if (metadata.IndexName == null)
            throw new InvalidOperationException("VectorWorkloadMetadata.IndexName must be set for recall measurement.");

        var maxK = recallKs.Max();

        using var store = new DocumentStore { Urls = [serverUrl], Database = databaseName };
        if (httpVersion != null)
            HttpHelper.ConfigureHttpVersion(store, httpVersion, HttpVersionPolicy.RequestVersionExact);
        store.Initialize();

        // Compute ground truth once
        var queryFingerprint = ComputeQueryFingerprint(metadata);
        var (groundTruth, cached, groundTruthTime) = await GetGroundTruthAsync(store, metadata, maxK, quantization, searchEngine, queryFingerprint);

        var results = new Dictionary<int, RecallResult>();
        foreach (var ef in efSearchValues.Order())
        {
            Console.WriteLine($"[Recall] --- efSearch={ef} ---");
            var measureSw = Stopwatch.StartNew();
            var recallAtK = await ComputeRecallAsync(store, metadata, groundTruth, recallKs, maxK, quantization, searchEngine, ef);
            measureSw.Stop();

            results[ef] = new RecallResult
            {
                RecallAtK = recallAtK,
                QueryCount = metadata.QueryVectorCount,
                GroundTruthDepth = maxK,
                GroundTruthCached = cached,
                GroundTruthComputeTime = groundTruthTime,
                MeasurementTime = measureSw.Elapsed
            };

            // Only count ground truth time for the first iteration
            cached = true;
            groundTruthTime = TimeSpan.Zero;
        }

        return results;
    }

    private async Task<(Dictionary<int, string[]> groundTruth, bool cached, TimeSpan computeTime)> GetGroundTruthAsync(
        IDocumentStore store,
        VectorWorkloadMetadata metadata,
        int maxK,
        VectorQuantization quantization,
        IndexingEngine searchEngine,
        string queryFingerprint)
    {
        // Try to load from cache
        var cached = await LoadGroundTruthAsync(store, maxK, metadata.QueryVectorCount, queryFingerprint);
        if (cached != null)
        {
            Console.WriteLine($"[Recall] Loaded cached ground truth ({cached.Count} queries, depth {maxK})");
            return (cached, true, TimeSpan.Zero);
        }

        // Compute via exact search
        Console.WriteLine($"[Recall] Computing ground truth for {metadata.QueryVectorCount} queries at depth {maxK}...");
        var sw = Stopwatch.StartNew();
        var groundTruth = await ComputeGroundTruthAsync(store, metadata, maxK, quantization, searchEngine);
        sw.Stop();
        Console.WriteLine($"[Recall] Ground truth computed in {sw.Elapsed}");

        // Cache in database
        await StoreGroundTruthAsync(store, groundTruth, maxK, queryFingerprint);
        Console.WriteLine($"[Recall] Ground truth cached in database");

        return (groundTruth, false, sw.Elapsed);
    }

    private static async Task<Dictionary<int, string[]>?> LoadGroundTruthAsync(
        IDocumentStore store, int requiredDepth, int requiredQueryCount, string queryFingerprint)
    {
        using var session = store.OpenAsyncSession();
        var doc = await session.LoadAsync<GroundTruthDocument>(GroundTruthDocId);
        if (doc == null)
            return null;

        // Validate cached ground truth matches current query vectors
        if (doc.QueryFingerprint != queryFingerprint)
        {
            Console.WriteLine($"[Recall] Cached ground truth was computed with different query vectors — recomputing");
            return null;
        }

        // Validate cached ground truth is sufficient
        if (doc.MaxK < requiredDepth || doc.QueryCount < requiredQueryCount)
        {
            Console.WriteLine($"[Recall] Cached ground truth insufficient (depth {doc.MaxK} < {requiredDepth} or queries {doc.QueryCount} < {requiredQueryCount}) — recomputing");
            return null;
        }

        // Rebuild dictionary, truncating to requiredDepth if cached has more
        var result = new Dictionary<int, string[]>(doc.Entries.Count);
        foreach (var entry in doc.Entries)
        {
            var ids = entry.NearestIds;
            if (ids.Length > requiredDepth)
                ids = ids[..requiredDepth];
            result[entry.QueryIndex] = ids;
        }

        return result;
    }

    /// <summary>
    /// Computes a fingerprint of the query vectors so we can detect when they change.
    /// Uses first/last vector values + count to avoid hashing megabytes of floats.
    /// </summary>
    private static string ComputeQueryFingerprint(VectorWorkloadMetadata metadata)
    {
        var vecs = metadata.QueryVectors;
        if (vecs.Length == 0)
            return "empty";

        // Hash: count + first vector's first 4 values + last vector's first 4 values
        var first = vecs[0];
        var last = vecs[^1];
        var parts = new List<string>
        {
            vecs.Length.ToString(),
            metadata.VectorDimensions.ToString()
        };
        for (int i = 0; i < Math.Min(4, first.Length); i++)
            parts.Add(first[i].ToString("R"));
        for (int i = 0; i < Math.Min(4, last.Length); i++)
            parts.Add(last[i].ToString("R"));

        return string.Join("|", parts);
    }

    private static async Task StoreGroundTruthAsync(
        IDocumentStore store, Dictionary<int, string[]> groundTruth, int maxK, string queryFingerprint)
    {
        var doc = new GroundTruthDocument
        {
            MaxK = maxK,
            QueryCount = groundTruth.Count,
            QueryFingerprint = queryFingerprint,
            ComputedAt = DateTimeOffset.UtcNow,
            Entries = groundTruth
                .OrderBy(kvp => kvp.Key)
                .Select(kvp => new GroundTruthEntry { QueryIndex = kvp.Key, NearestIds = kvp.Value })
                .ToList()
        };

        using var session = store.OpenAsyncSession();
        await session.StoreAsync(doc, GroundTruthDocId);
        await session.SaveChangesAsync();
    }

    /// <summary>
    /// Computes ground truth by running exact vector search for each query vector.
    /// </summary>
    private static async Task<Dictionary<int, string[]>> ComputeGroundTruthAsync(
        IDocumentStore store,
        VectorWorkloadMetadata metadata,
        int maxK,
        VectorQuantization quantization,
        IndexingEngine searchEngine)
    {
        var groundTruth = new Dictionary<int, string[]>(metadata.QueryVectorCount);

        using var session = store.OpenAsyncSession();
        session.Advanced.MaxNumberOfRequestsPerSession = int.MaxValue;

        for (int i = 0; i < metadata.QueryVectorCount; i++)
        {
            var queryVector = metadata.QueryVectors[i];
            var rql = BuildExactSearchQuery(metadata, quantization, searchEngine);

            var results = await session.Advanced.AsyncRawQuery<BlittableJsonReaderObject>(rql)
                .AddParameter("vector", queryVector)
                .Take(maxK)
                .ToListAsync();

            var ids = new string[results.Count];
            for (int j = 0; j < results.Count; j++)
            {
                ids[j] = session.Advanced.GetDocumentId(results[j]);
            }

            groundTruth[i] = ids;

            if ((i + 1) % 100 == 0 || i == metadata.QueryVectorCount - 1)
            {
                Console.Write($"\r[Recall] Ground truth: {i + 1}/{metadata.QueryVectorCount} queries");
            }
        }

        Console.WriteLine();
        return groundTruth;
    }

    /// <summary>
    /// Runs approximate search and computes recall@K at each requested cutoff.
    /// </summary>
    private static async Task<Dictionary<int, double>> ComputeRecallAsync(
        IDocumentStore store,
        VectorWorkloadMetadata metadata,
        Dictionary<int, string[]> groundTruth,
        int[] recallKs,
        int maxK,
        VectorQuantization quantization,
        IndexingEngine searchEngine,
        int? efSearch = null)
    {
        // Per-K hit count
        var hits = new Dictionary<int, int>();
        var queryCount = new Dictionary<int, int>();
        foreach (var k in recallKs)
        {
            hits[k] = 0;
            queryCount[k] = 0;
        }

        Console.WriteLine($"[Recall] Measuring recall at K={string.Join(",", recallKs)} over {metadata.QueryVectorCount} queries...");

        using var session = store.OpenAsyncSession();
        session.Advanced.MaxNumberOfRequestsPerSession = int.MaxValue;

        for (int i = 0; i < metadata.QueryVectorCount; i++)
        {
            if (groundTruth.TryGetValue(i, out var truthIds) == false)
                continue;

            var queryVector = metadata.QueryVectors[i];
            var rql = BuildApproximateSearchQuery(metadata, quantization, searchEngine, efSearch);

            var query = session.Advanced.AsyncRawQuery<BlittableJsonReaderObject>(rql)
                .AddParameter("vector", queryVector)
                .Take(maxK);

            if (efSearch.HasValue)
                query = query.AddParameter("efSearch", efSearch.Value);

            var results = await query.ToListAsync();

            var annIds = new string[results.Count];
            for (int j = 0; j < results.Count; j++)
            {
                annIds[j] = session.Advanced.GetDocumentId(results[j]);
            }

            // recall@K: is the true nearest neighbor found anywhere in the ANN top-K?
            // This is monotonically non-decreasing with K — if found in top-1, it's in top-10.
            var trueNearest = truthIds[0]; // ground truth is ordered by similarity, [0] is the true #1
            foreach (var k in recallKs)
            {
                var annAtK = annIds.Length >= k ? annIds.AsSpan(0, k) : annIds.AsSpan();

                bool found = false;
                foreach (var annId in annAtK)
                {
                    if (annId == trueNearest)
                    {
                        found = true;
                        break;
                    }
                }

                if (found)
                    hits[k]++;
                queryCount[k]++;
            }

            if ((i + 1) % 100 == 0 || i == metadata.QueryVectorCount - 1)
            {
                Console.Write($"\r[Recall] Measured: {i + 1}/{metadata.QueryVectorCount} queries");
            }
        }

        Console.WriteLine();

        // Average recall across all queries
        var recallAtK = new Dictionary<int, double>();
        foreach (var k in recallKs)
        {
            recallAtK[k] = queryCount[k] > 0 ? (double)hits[k] / queryCount[k] : 0.0;
            Console.WriteLine($"[Recall] recall@{k} = {recallAtK[k]:P2}");
        }

        return recallAtK;
    }

    /// <summary>
    /// Builds an exact (brute-force) vector search RQL query.
    /// </summary>
    private static string BuildExactSearchQuery(
        VectorWorkloadMetadata metadata,
        VectorQuantization quantization,
        IndexingEngine searchEngine)
    {
        var fieldName = metadata.IndexedFieldName ?? metadata.FieldName;
        var embeddingSelector = GetEmbeddingSelector(fieldName, quantization);
        var indexName = GetIndexName(metadata, quantization, searchEngine);
        return $"from index '{indexName}' where exact(vector.search({embeddingSelector}, $vector))";
    }

    /// <summary>
    /// Builds an approximate (HNSW) vector search RQL query.
    /// </summary>
    private static string BuildApproximateSearchQuery(
        VectorWorkloadMetadata metadata,
        VectorQuantization quantization,
        IndexingEngine searchEngine,
        int? efSearch = null)
    {
        var fieldName = metadata.IndexedFieldName ?? metadata.FieldName;
        var embeddingSelector = GetEmbeddingSelector(fieldName, quantization);
        var indexName = GetIndexName(metadata, quantization, searchEngine);
        // vector.search(field, value, minimumSimilarity, numberOfCandidates)
        if (efSearch.HasValue)
            return $"from index '{indexName}' where vector.search({embeddingSelector}, $vector, 0.0, $efSearch)";
        return $"from index '{indexName}' where vector.search({embeddingSelector}, $vector)";
    }

    private static string GetEmbeddingSelector(string fieldName, VectorQuantization quantization)
    {
        return quantization switch
        {
            VectorQuantization.Int8 => $"embedding.f32_i8('{fieldName}')",
            VectorQuantization.Binary => $"embedding.f32_i1('{fieldName}')",
            VectorQuantization.Int4 => $"embedding.f32_i4('{fieldName}')",
            VectorQuantization.Int3 => $"embedding.f32_i3('{fieldName}')",
            VectorQuantization.Int2 => $"embedding.f32_i2('{fieldName}')",
            _ => $"'{fieldName}'"
        };
    }

    private static string GetIndexName(
        VectorWorkloadMetadata metadata,
        VectorQuantization quantization,
        IndexingEngine searchEngine)
    {
        // If the metadata has an explicit index name, use it
        if (string.IsNullOrEmpty(metadata.IndexName) == false)
            return metadata.IndexName;

        // Fall back to the convention-based naming
        var engineSuffix = searchEngine == IndexingEngine.Lucene ? "-lucene" : "-corax";
        var collection = metadata.CollectionName ?? "Words";
        return VectorIndexNaming.GetIndexName(collection, quantization, engineSuffix);
    }

    // --- Ground truth document model ---

    private sealed class GroundTruthDocument
    {
        public int MaxK { get; set; }
        public int QueryCount { get; set; }
        public string QueryFingerprint { get; set; } = "";
        public DateTimeOffset ComputedAt { get; set; }
        public List<GroundTruthEntry> Entries { get; set; } = new();
    }

    private sealed class GroundTruthEntry
    {
        public int QueryIndex { get; set; }
        public string[] NearestIds { get; set; } = [];
    }
}
