using RavenBench.Core;
using RavenBench.Core.Transport;
using RavenBench.Core.Workload;
using RavenBench.Dataset;
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

    internal static async Task<(string database, bool imported)> ImportClinicalWordsDatasetAsync(RunOptions opts, Version httpVersion)
    {
        var datasetName = opts.Dataset!.ToLowerInvariant();
        int dimensions = 100;
        if (datasetName.Contains("300d")) dimensions = 300;
        else if (datasetName.Contains("600d")) dimensions = 600;

        var provider = new Dataset.ClinicalWordsDatasetProvider(dimensions);
        var targetDatabase = provider.GetDatabaseName();

        Console.WriteLine($"[Raven.Bench] ClinicalWords{dimensions}D dataset -> '{targetDatabase}'");

        if (opts.DatasetSkipIfExists)
        {
            Console.WriteLine($"[Raven.Bench] Checking if data already imported...");
            var exists = await provider.IsDatasetImportedAsync(opts.Url, targetDatabase, expectedMinDocuments: Dataset.ClinicalWordsDatasetProvider.MinExpectedDocuments, httpVersion: httpVersion);
            if (exists)
            {
                Console.WriteLine($"[Raven.Bench] ClinicalWords{dimensions}D already imported. Ready to use.");
                return (targetDatabase, imported: false);
            }
            Console.WriteLine($"[Raven.Bench] Data not found or incomplete, will import.");
        }

        Console.WriteLine($"[Raven.Bench] Importing clinical word embeddings to RavenDB (engine: {opts.SearchEngine})...");
        var exactSearch = opts.Profile == WorkloadProfile.VectorSearchExact || opts.VectorExactSearch;
        await provider.ImportWordsAsync(opts.Url, targetDatabase, opts.VectorQuantization, exactSearch, httpVersion: httpVersion, searchEngine: opts.SearchEngine);

        Console.WriteLine($"[Raven.Bench] ClinicalWords{dimensions}D import complete.");

        return (targetDatabase, imported: true);
    }

    internal static async Task<(string database, bool imported)> ImportSphereDatasetAsync(RunOptions opts, Version httpVersion)
    {
        var profile = opts.DatasetProfile ?? "100k";
        var provider = new Dataset.SphereDatasetProvider(profile);
        var targetDatabase = provider.GetDatabaseName(profile);

        Console.WriteLine($"[Raven.Bench] SPHERE {profile} dataset -> '{targetDatabase}'");

        if (opts.DatasetSkipIfExists)
        {
            Console.WriteLine($"[Raven.Bench] Checking if data already imported...");
            var expectedMin = (int)Math.Min(Dataset.SphereDatasetProvider.GetProfile(profile).TargetDocCount, int.MaxValue);
            var exists = await provider.IsDatasetImportedAsync(opts.Url, targetDatabase, expectedMinDocuments: expectedMin, httpVersion: httpVersion);
            if (exists)
            {
                Console.WriteLine($"[Raven.Bench] SPHERE {profile} already imported. Ready to use.");
                return (targetDatabase, imported: false);
            }
            Console.WriteLine($"[Raven.Bench] Data not found or incomplete, will import.");
        }

        var dataSourcePath = opts.DatasetSource
            ?? opts.DatasetCacheDir
            ?? Path.Combine(Directory.GetCurrentDirectory(), "datasets", "sphere");

        Console.WriteLine($"[Raven.Bench] Importing SPHERE dataset from '{dataSourcePath}' (engine: {opts.SearchEngine})...");
        var exactSearch = opts.Profile == WorkloadProfile.VectorSearchExact || opts.VectorExactSearch;
        await provider.ImportAsync(opts.Url, targetDatabase, dataSourcePath,
            opts.VectorQuantization, exactSearch, httpVersion: httpVersion, searchEngine: opts.SearchEngine,
            numberOfEdges: opts.VectorEdges, numberOfCandidatesForIndexing: opts.VectorCandidates);

        Console.WriteLine($"[Raven.Bench] SPHERE {profile} import complete.");

        return (targetDatabase, imported: true);
    }

    internal static async Task WaitForNonStaleIndexesAsync(string serverUrl, string databaseName, Version httpVersion)
    {
        Console.WriteLine("[Raven.Bench] Waiting for indexes to become non-stale...");

        using var store = new DocumentStore
        {
            Urls = new[] { serverUrl },
            Database = databaseName
        };
        HttpHelper.ConfigureHttpVersion(store, httpVersion, HttpVersionPolicy.RequestVersionExact);
        store.Initialize();

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
            throw new InvalidOperationException("Vector search profiles require --dataset option. Supported: clinicalwords100d, clinicalwords300d, clinicalwords600d, sphere");
        }

        var engineSuffix = VectorIndexMapping.GetEngineSuffix(opts.SearchEngine);

        if (datasetName.StartsWith("clinicalwords", StringComparison.OrdinalIgnoreCase))
        {
            int dimensions = 100;
            if (datasetName.Contains("300d")) dimensions = 300;
            else if (datasetName.Contains("600d")) dimensions = 600;

            var provider = new Dataset.ClinicalWordsDatasetProvider(dimensions);
            var metadata = await provider.GenerateQueryVectorsAsync(count: 1000);
            metadata.IndexName = VectorIndexNaming.GetIndexName("Words", opts.VectorQuantization, engineSuffix, opts.VectorEdges, opts.VectorCandidates);
            metadata.CollectionName = "WordDocuments";
            return metadata;
        }

        if (datasetName.StartsWith("sphere", StringComparison.OrdinalIgnoreCase))
        {
            var profile = opts.DatasetProfile ?? "100k";
            var provider = new Dataset.SphereDatasetProvider(profile);
            var dbName = provider.GetDatabaseName(profile);
            var metadata = await provider.GenerateQueryVectorsAsync(opts.Url, dbName, count: 1000);
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

        using var store = new DocumentStore
        {
            Urls = [opts.Url],
            Database = effectiveDatabase
        };
        var httpVersion = opts.HttpVersion != "auto"
            ? HttpHelper.ParseHttpVersion(HttpHelper.NormalizeHttpVersion(opts.HttpVersion))
            : null;
        if (httpVersion != null)
            HttpHelper.ConfigureHttpVersion(store, httpVersion, HttpVersionPolicy.RequestVersionExact);
        store.Initialize();

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
        using var session = store.OpenAsyncSession();
        session.Advanced.MaxNumberOfRequestsPerSession = int.MaxValue;
        await session.Query<object>(indexName)
            .Customize(x => x.WaitForNonStaleResults(TimeSpan.MaxValue))
            .Take(0)
            .ToListAsync();
        Console.WriteLine($"[Raven.Bench] Vector index '{indexName}' is ready");
    }
}
