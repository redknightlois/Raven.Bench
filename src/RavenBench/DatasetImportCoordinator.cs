using RavenBench.Core;
using RavenBench.Core.Transport;
using RavenBench.Core.Workload;
using RavenBench.Dataset;
using RavenBench.Dataset.Vectors;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;

namespace RavenBench;

internal static class DatasetImportCoordinator
{
    internal static async Task<string> ImportDatasetAsync(RunOptions opts)
    {
        Console.WriteLine($"[Raven.Bench] Dataset import requested: {opts.Dataset}");

        using var datasetManager = new Dataset.DatasetManager(opts.DatasetCacheDir);

        string targetDatabase;
        int datasetSize;

        if (string.IsNullOrEmpty(opts.DatasetProfile) == false)
        {
            var profile = Enum.Parse<Dataset.DatasetProfile>(opts.DatasetProfile, ignoreCase: true);
            targetDatabase = Dataset.KnownDatasets.GetDatabaseName(profile);
            datasetSize = Dataset.KnownDatasets.GetDatasetSize(profile);
            Console.WriteLine($"[Raven.Bench] Using dataset profile '{opts.DatasetProfile}': {targetDatabase} (~{(datasetSize == 0 ? 50 : datasetSize + 2)}GB)");
        }
        else
        {
            targetDatabase = Dataset.KnownDatasets.GetDatabaseNameForSize(opts.DatasetSize);
            datasetSize = opts.DatasetSize;
            Console.WriteLine($"[Raven.Bench] Using custom dataset size: {targetDatabase} (~{(datasetSize == 0 ? 50 : datasetSize + 2)}GB)");
        }

        if (opts.DatasetSkipIfExists)
        {
            var exists = await datasetManager.IsStackOverflowDatasetImportedAsync(opts.Url, targetDatabase, opts.HttpVersion, expectedMinDocuments: 10000);
            if (exists)
            {
                Console.WriteLine($"[Raven.Bench] Dataset appears to already exist in database '{targetDatabase}'. Skipping import.");
                Console.WriteLine($"[Raven.Bench] Use --dataset-skip-if-exists=false to force re-import.");
                return targetDatabase;
            }
        }

        Dataset.DatasetInfo? dataset;
        if (datasetSize > 0)
        {
            Console.WriteLine($"[Raven.Bench] Importing partial dataset with {datasetSize} post dump files to '{targetDatabase}'");
            dataset = Dataset.KnownDatasets.StackOverflowPartial(datasetSize);
        }
        else
        {
            Console.WriteLine($"[Raven.Bench] Importing full dataset to '{targetDatabase}'");
            dataset = Dataset.KnownDatasets.GetByName(opts.Dataset!);
        }

        if (dataset == null)
        {
            throw new ArgumentException($"Unknown dataset: {opts.Dataset}. Supported: stackoverflow, clinicalwords100d, clinicalwords300d, clinicalwords600d");
        }

        await datasetManager.ImportDatasetAsync(dataset, opts.Url, targetDatabase, opts.HttpVersion);

        return targetDatabase;
    }

    /// <summary>
    /// Query vectors drawn per vector run; the scenario seed chooses which.
    /// </summary>
    internal const int VectorQueryCount = 1000;

    internal static QuerySelection VectorQuerySelection(RunOptions opts) => new(opts.Seed, VectorQueryCount);

    // Truth depth covers the search depth and every recall cutoff.
    internal static int VectorTruthDepth(RunOptions opts) => Math.Max(opts.VectorTopK, opts.VectorRecallKs is { Length: > 0 } ks ? ks.Max() : 0);

    internal static string VectorDataDirectory(RunOptions opts) =>
        opts.DatasetCacheDir ?? Path.Combine(Directory.GetCurrentDirectory(), "datasets");

    internal static int ClinicalWordsDimensions(string dataset) =>
        dataset.Contains("300d", StringComparison.OrdinalIgnoreCase) ? 300
        : dataset.Contains("600d", StringComparison.OrdinalIgnoreCase) ? 600
        : 100;

    /// <summary>
    /// Refuses a set RavenDB cannot search, then verifies the set's pinned files. Runs before any load.
    /// </summary>
    internal static async Task<VerifiedFiles> PrepareVectorSetAsync(IVectorDataset set, RunOptions opts)
    {
        UnsupportedVectorMetricException.ThrowIfUnsupported(RawHttpTransport.RavenDbProductName, RavenDbVectorMetrics.Supported, set.Metric);
        return await PinnedFiles.EnsureAsync(set, VectorDataDirectory(opts), opts.DatasetSource, opts.DatasetSha256);
    }

    internal static async Task<(string database, bool imported)> ImportClinicalWordsDatasetAsync(RunOptions opts, Version httpVersion)
    {
        var provider = new Dataset.ClinicalWordsDatasetProvider(ClinicalWordsDimensions(opts.Dataset!));
        var targetDatabase = provider.GetDatabaseName();
        Console.WriteLine($"[Raven.Bench] {provider.Name} dataset -> '{targetDatabase}' (engine: {opts.SearchEngine})");

        var files = await PrepareVectorSetAsync(provider, opts);
        var exactSearch = opts.Profile == WorkloadProfile.VectorSearchExact || opts.VectorExactSearch;
        var imported = await provider.ImportWordsAsync(opts.Url, targetDatabase, files, VectorQuerySelection(opts), opts.VectorQuantization, exactSearch, httpVersion: httpVersion, searchEngine: opts.SearchEngine);
        return (targetDatabase, imported);
    }

    internal static async Task<(string database, bool imported)> ImportSphereDatasetAsync(RunOptions opts, Version httpVersion)
    {
        var profile = opts.DatasetProfile ?? "100k";
        var provider = new Dataset.SphereDatasetProvider(profile);
        var targetDatabase = provider.GetDatabaseName(profile);
        Console.WriteLine($"[Raven.Bench] {provider.Name} dataset -> '{targetDatabase}' (engine: {opts.SearchEngine})");

        var files = await PrepareVectorSetAsync(provider, opts);
        var exactSearch = opts.Profile == WorkloadProfile.VectorSearchExact || opts.VectorExactSearch;
        var result = await provider.ImportAsync(opts.Url, targetDatabase, files, VectorQuerySelection(opts),
            opts.VectorQuantization, exactSearch, httpVersion: httpVersion, searchEngine: opts.SearchEngine,
            numberOfEdges: opts.VectorEdges, numberOfCandidatesForIndexing: opts.VectorCandidates);
        return (targetDatabase, result.DocumentsImported > 0);
    }

    internal static async Task<(string database, bool imported)> ImportPublishedSetAsync(RunOptions opts, IVectorDataset set, Version httpVersion)
    {
        var files = await PrepareVectorSetAsync(set, opts);
        Console.WriteLine($"[Raven.Bench] {set.Name} dataset -> '{PublishedSetImport.DatabaseName(set)}' (engine: {opts.SearchEngine})");
        var imported = await PublishedSetImport.ImportAsync(opts.Url, set, files, VectorQuerySelection(opts), opts.VectorQuantization, opts.SearchEngine,
            opts.VectorEdges, opts.VectorCandidates, httpVersion);
        return (PublishedSetImport.DatabaseName(set), imported);
    }

    internal static async Task WaitForNonStaleIndexesAsync(string serverUrl, string databaseName, Version httpVersion)
    {
        Console.WriteLine("[Raven.Bench] Waiting for indexes to become non-stale...");

        using var store = HttpHelper.Create(serverUrl, databaseName, httpVersion);

        var maxWait = TimeSpan.FromMinutes(10);
        var sw = System.Diagnostics.Stopwatch.StartNew();

        while (sw.Elapsed < maxWait)
        {
            var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
            var staleIndexes = stats.Indexes.Where(i => i.IsStale).ToList();

            if (staleIndexes.Count == 0)
            {
                Console.WriteLine($"[Raven.Bench] All indexes are non-stale (waited {sw.Elapsed.TotalSeconds:F1}s)");
                return;
            }

            Console.WriteLine($"[Raven.Bench] {staleIndexes.Count} stale index(es), waiting... ({sw.Elapsed.TotalSeconds:F0}s elapsed)");
            await Task.Delay(2000);
        }

        Console.WriteLine($"[Raven.Bench] WARNING: Indexes still stale after {maxWait.TotalMinutes} minutes");
    }

    internal static async Task<VectorWorkloadMetadata?> LoadVectorMetadataAsync(RunOptions opts)
    {
        var datasetName = opts.Dataset;

        if (string.IsNullOrEmpty(datasetName))
        {
            throw new InvalidOperationException($"Vector search profiles require --dataset option. Supported: clinicalwords100d, clinicalwords300d, clinicalwords600d, sphere, {string.Join(", ", VectorSets.Published.Select(s => s.Name))}");
        }

        if (VectorSets.FindPublished(datasetName) is { } published)
        {
            var files = await PrepareVectorSetAsync(published, opts);
            return await PublishedSetImport.MetadataAsync(published, files, VectorQuerySelection(opts), VectorTruthDepth(opts),
                opts.VectorQuantization, opts.SearchEngine, opts.VectorEdges, opts.VectorCandidates);
        }

        var engineSuffix = VectorIndexMapping.GetEngineSuffix(opts.SearchEngine);

        if (datasetName.StartsWith("clinicalwords", StringComparison.OrdinalIgnoreCase))
        {
            var provider = new Dataset.ClinicalWordsDatasetProvider(ClinicalWordsDimensions(datasetName));
            var files = await PrepareVectorSetAsync(provider, opts);
            var metadata = await provider.GenerateQueryVectorsAsync(files, VectorQuerySelection(opts), VectorTruthDepth(opts));
            metadata.IndexName = VectorIndexNaming.GetIndexName("Words", opts.VectorQuantization, engineSuffix, opts.VectorEdges, opts.VectorCandidates);
            metadata.CollectionName = "WordDocuments";
            metadata.IndexedFieldName = "Vector";
            return metadata;
        }

        if (datasetName.StartsWith("sphere", StringComparison.OrdinalIgnoreCase))
        {
            var profile = opts.DatasetProfile ?? "100k";
            var provider = new Dataset.SphereDatasetProvider(profile);
            var files = await PrepareVectorSetAsync(provider, opts);
            var metadata = await provider.GenerateQueryVectorsAsync(files, VectorQuerySelection(opts), VectorTruthDepth(opts));
            metadata.IndexName = VectorIndexNaming.GetIndexName(Dataset.SphereDatasetProvider.CollectionName, opts.VectorQuantization, engineSuffix, opts.VectorEdges, opts.VectorCandidates);
            metadata.CollectionName = Dataset.SphereDatasetProvider.CollectionName;
            metadata.IndexedFieldName = "Vector";
            metadata.EnsureIndexExists = async (storeObj, indexName) =>
            {
                var s = (IDocumentStore)storeObj;
                await Dataset.SphereDatasetProvider.CreateVectorIndexAsync(
                    s, opts.VectorQuantization, opts.VectorExactSearch, opts.SearchEngine,
                    opts.VectorEdges, opts.VectorCandidates);
            };
            return metadata;
        }

        throw new NotSupportedException($"Dataset '{datasetName}' is not supported for vector search queries.");
    }

    internal static async Task EnsureVectorIndexExistsAsync(
        ITransport transport,
        RunOptions opts,
        VectorWorkloadMetadata metadata,
        string effectiveDatabase)
    {
        var indexName = metadata.IndexName!;
        Console.WriteLine($"[Raven.Bench] Verifying vector index '{indexName}' exists...");

        var httpVersion = opts.HttpVersion != "auto"
            ? HttpHelper.ParseHttpVersion(HttpHelper.NormalizeHttpVersion(opts.HttpVersion))
            : null;
        using var store = HttpHelper.Create(opts.Url, effectiveDatabase, httpVersion);

        var indexes = await store.Maintenance.SendAsync(
            new Raven.Client.Documents.Operations.Indexes.GetIndexNamesOperation(0, int.MaxValue));

        if (indexes.Contains(indexName) == false)
        {
            Console.WriteLine($"[Raven.Bench] Vector index '{indexName}' not found — creating...");
            if (metadata.EnsureIndexExists != null)
                await metadata.EnsureIndexExists(store, indexName);
            else
                throw new InvalidOperationException(
                    $"Vector index '{indexName}' does not exist and cannot be auto-created. " +
                    "Ensure the dataset was imported with the correct HNSW parameters.");
        }

        Console.WriteLine($"[Raven.Bench] Waiting for vector index '{indexName}' to be non-stale...");
        await VectorIndexHelper.WaitForNonStaleAsync(store, indexName);
        Console.WriteLine($"[Raven.Bench] Vector index '{indexName}' is ready");
    }
}
