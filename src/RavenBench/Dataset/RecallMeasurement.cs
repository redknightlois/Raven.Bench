using System.Diagnostics;
using System.Net;
using Raven.Client.Documents;
using RavenBench.Core;
using RavenBench.Core.Metrics;
using RavenBench.Core.Transport;
using RavenBench.Core.Workload;

namespace RavenBench.Dataset;

/// <summary>
/// Measures recall@K for vector search: each query runs through the typed vector search operation, and
/// the returned ids are compared with the set's product-neutral truth. No product search produces truth.
/// </summary>
public sealed class RecallMeasurement
{
    /// <summary>
    /// Runs recall measurement at a single RavenDB numberOfCandidates value against a RavenDB database over raw HTTP.
    /// </summary>
    public async Task<RecallResult> MeasureAsync(
        string serverUrl,
        string databaseName,
        VectorWorkloadMetadata metadata,
        int[] recallKs,
        VectorQuantization quantization,
        IndexingEngine searchEngine,
        Version? httpVersion = null,
        int? efSearch = null,
        Uri? nodeExporterUrl = null)
    {
        using var nodeExporter = await NodeExporterClient.ConnectAsync(nodeExporterUrl);
        using var transport = await OpenRavenAsync(serverUrl, databaseName, metadata, quantization, searchEngine, httpVersion);
        return await MeasureAsync(transport, metadata, recallKs, quantization, efSearch is { } ef ? SearchEffort.RavenDb(ef) : null, nodeExporter);
    }

    /// <summary>
    /// Runs recall measurement sweeping RavenDB numberOfCandidates values against a RavenDB database. Returns one result per value.
    /// </summary>
    public async Task<Dictionary<int, RecallResult>> MeasureSweepAsync(
        string serverUrl,
        string databaseName,
        VectorWorkloadMetadata metadata,
        int[] recallKs,
        int[] efSearchValues,
        VectorQuantization quantization,
        IndexingEngine searchEngine,
        Version? httpVersion = null,
        Uri? nodeExporterUrl = null)
    {
        using var nodeExporter = await NodeExporterClient.ConnectAsync(nodeExporterUrl);
        using var transport = await OpenRavenAsync(serverUrl, databaseName, metadata, quantization, searchEngine, httpVersion);
        var results = new Dictionary<int, RecallResult>();
        foreach (var effort in efSearchValues.Order())
        {
            Console.WriteLine($"[Recall] --- effort={effort} ---");
            results[effort] = await MeasureAsync(transport, metadata, recallKs, quantization, SearchEffort.RavenDb(effort), nodeExporter);
        }
        return results;
    }

    /// <summary>
    /// Runs every query of the metadata through the transport at one effort, the transport's own knob, and scores
    /// it against the metadata's truth. A null effort runs the product at its default. A node_exporter client brackets
    /// the queries with two scrapes and fills the server columns.
    /// </summary>
    public async Task<RecallResult> MeasureAsync(IYcsbTransport transport, VectorWorkloadMetadata metadata, int[] recallKs, VectorQuantization quantization, SearchEffort? effort, NodeExporterClient? nodeExporter = null, CancellationToken ct = default)
    {
        var truth = metadata.GroundTruth
            ?? throw new InvalidOperationException("Recall needs the set's truth; the vector metadata carries none.");
        var maxK = recallKs.Max();
        var prefix = metadata.DocumentIdPrefix ?? "";

        Console.WriteLine($"[Recall] Measuring recall at K={string.Join(",", recallKs)} over {metadata.QueryVectorCount} queries...");
        var nodeStart = nodeExporter?.ScrapeAsync(ct);
        var sw = Stopwatch.StartNew();
        var returned = new List<string>[metadata.QueryVectorCount];
        for (int i = 0; i < metadata.QueryVectorCount; i++)
        {
            var result = await transport.ExecuteAsync(new VectorSearchOperation
            {
                QueryVector = metadata.QueryVectors[i],
                FieldName = metadata.IndexedFieldName ?? metadata.FieldName,
                TopK = maxK,
                Quantization = quantization,
                ExpectedIndex = metadata.IndexName,
                Effort = effort
            }, ct).ConfigureAwait(false);

            if (result.IsSuccess == false)
                throw new InvalidOperationException($"Recall query {i} failed: {result.ErrorDetails}");
            var ids = result.NeighborIds ?? throw new InvalidOperationException($"{transport.ProductName} returned no neighbour ids.");
            returned[i] = ids.Select(id => StripPrefix(id, prefix)).ToList();
        }
        sw.Stop();
        var server = nodeStart == null ? null : await nodeExporter!.EndWindowAsync(nodeStart, ct);

        var recallAtK = ComputeRecall(returned, truth, recallKs);
        foreach (var (k, recall) in recallAtK.OrderBy(kvp => kvp.Key))
            Console.WriteLine($"[Recall] recall@{k} = {recall:P2}");

        return new RecallResult
        {
            RecallAtK = recallAtK,
            QueryCount = metadata.QueryVectorCount,
            GroundTruthDepth = truth.Values.Min(t => t.Length),
            GroundTruthCached = true,
            GroundTruthComputeTime = TimeSpan.Zero,
            MeasurementTime = sw.Elapsed,
            ServerCpu = server?.CpuPercent,
            ServerMemoryMB = server?.MemoryMB,
            ServerCpuSource = server?.CpuPercent.HasValue == true ? NodeExporterClient.SourceName : null,
            ServerMemorySource = server?.MemoryMB.HasValue == true ? NodeExporterClient.SourceName : null,
            ServerMetricsHostWide = server?.CpuPercent.HasValue == true || server?.MemoryMB.HasValue == true ? true : null,
            ServerMetricsUnavailable = server?.Unavailable
        };
    }

    /// <summary>
    /// recall@K = |truth top K ∩ returned top K| / K, averaged over queries. A query that returned fewer than K
    /// ids still counts K truth ids, so a short result lowers recall.
    /// </summary>
    public static Dictionary<int, double> ComputeRecall(IReadOnlyList<IReadOnlyList<string>> returned, IReadOnlyDictionary<int, string[]> truth, int[] recallKs)
    {
        var recallAtK = new Dictionary<int, double>();
        foreach (var k in recallKs)
        {
            long hits = 0, expected = 0;
            for (int q = 0; q < returned.Count; q++)
            {
                if (truth.TryGetValue(q, out var nearest) == false || nearest.Length < k)
                    throw new InvalidOperationException($"Truth for query {q} holds fewer than {k} neighbours.");
                var top = returned[q].Take(k).ToHashSet(StringComparer.Ordinal);
                hits += nearest.Take(k).Count(top.Contains);
                expected += k;
            }
            recallAtK[k] = expected == 0 ? 0 : (double)hits / expected;
        }
        return recallAtK;
    }

    private static string StripPrefix(string id, string prefix) =>
        id.StartsWith(prefix, StringComparison.OrdinalIgnoreCase)
            ? id[prefix.Length..]
            : throw new InvalidDataException($"Returned id '{id}' lacks the document prefix '{prefix}'.");

    private static async Task<RawHttpTransport> OpenRavenAsync(string serverUrl, string databaseName, VectorWorkloadMetadata metadata, VectorQuantization quantization, IndexingEngine searchEngine, Version? httpVersion)
    {
        if (metadata.IndexName == null)
            throw new InvalidOperationException("VectorWorkloadMetadata.IndexName must be set for recall measurement.");

        using (var store = HttpHelper.Create(serverUrl, databaseName, httpVersion))
            await EnsureIndexExistsAsync(store, metadata, metadata.IndexName);

        return new RawHttpTransport(serverUrl, databaseName, CompressionMode.Identity, httpVersion ?? HttpVersion.Version11);
    }

    private static async Task EnsureIndexExistsAsync(IDocumentStore store, VectorWorkloadMetadata metadata, string indexName)
    {
        var indexes = await store.Maintenance.SendAsync(new Raven.Client.Documents.Operations.Indexes.GetIndexNamesOperation(0, int.MaxValue));
        if (indexes.Contains(indexName))
            return;

        Console.WriteLine($"[Recall] Index '{indexName}' not found — creating via metadata callback...");
        if (metadata.EnsureIndexExists == null)
            throw new InvalidOperationException(
                $"Index '{indexName}' does not exist and no EnsureIndexExists callback is configured. " +
                "Run the benchmark first to create the index, or use the standalone recall command.");

        await metadata.EnsureIndexExists(store, indexName);
    }
}
