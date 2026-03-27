using System.Diagnostics;
using System.Formats.Tar;
using System.IO.Compression;
using System.Net;
using System.Text.Json;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Session;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using RavenBench.Core;
using RavenBench.Core.Workload;

namespace RavenBench.Dataset;

/// <summary>
/// SPHERE dataset provider — streaming import of Meta's SPHERE dataset at TB scale.
/// Pre-computed DPR embeddings (768D) from Weaviate-hosted .jsonl.tar.gz files.
/// Supports tiered profiles from 100K to 899M passages.
/// </summary>
public sealed class SphereDatasetProvider : IDatasetProvider
{
    /// <summary>
    /// Document record for SPHERE passages. Vector is stored as a binary attachment, not inline.
    /// </summary>
    private sealed class SphereDocument
    {
        public required string Raw { get; init; }
        public required string Sha { get; init; }
        public required string Title { get; init; }
        public required string Url { get; init; }
    }

    /// <summary>
    /// Ground truth cache document stored in the target database.
    /// </summary>
    private sealed class GroundTruthCacheDocument
    {
        public required Dictionary<int, string[]> GroundTruth { get; init; }
        public required int QueryCount { get; init; }
        public required int TopK { get; init; }
        public required long BaseVectorCount { get; init; }
    }

    public const int DprDimensions = 768;
    private const string GroundTruthDocumentId = "SphereGroundTruth/Cache";
    private const string CollectionName = "SphereDocuments";
    private const int BulkInsertBatchSize = 100_000;

    private static readonly HttpClient SharedHttpClient = new() { Timeout = TimeSpan.FromHours(24) };

    private const string GcsBase = "https://storage.googleapis.com/sphere-demo";

    /// <summary>
    /// Profile definitions: line count and download URL for each tier.
    /// Files hosted on GCS by Weaviate. Access may require contacting hello@weaviate.io.
    /// </summary>
    private static readonly Dictionary<string, (long Lines, string Url)> Profiles = new(StringComparer.OrdinalIgnoreCase)
    {
        { "100k",  (100_000,         $"{GcsBase}/full.sphere.100k.jsonl.tar.gz") },
        { "1m",    (1_000_000,       $"{GcsBase}/full.sphere.1M.jsonl.tar.gz") },
        { "10m",   (10_000_000,      $"{GcsBase}/full.sphere.10M.jsonl.tar.gz") },
        { "100m",  (100_000_000,     $"{GcsBase}/full.sphere.100M.jsonl.tar.gz") },
        { "full",  (899_000_000,     $"{GcsBase}/full.sphere.899M.jsonl.tar.gz") },
    };

    public string DatasetName => "sphere";

    public DatasetInfo GetDatasetInfo(string? profile = null, int? customSize = null)
    {
        var profileName = NormalizeProfile(profile);
        var (lines, url) = Profiles[profileName];

        return new DatasetInfo
        {
            Name = $"Sphere-{FormatProfileName(profileName)}",
            Description = $"SPHERE dataset ({lines:N0} passages, {DprDimensions}D DPR embeddings)",
            MaxQuestionId = 0,
            MaxUserId = 0,
            Files = new List<DatasetFile>
            {
                new DatasetFile
                {
                    FileName = $"full.sphere.{profileName}.jsonl.tar.gz",
                    Url = url,
                    Type = "vectors",
                    EstimatedSizeBytes = lines * 4_300, // ~4.3 KB per compressed line
                    Description = $"SPHERE {FormatProfileName(profileName)} passages with {DprDimensions}D DPR embeddings"
                }
            }
        };
    }

    public string GetDatabaseName(string? profile = null, int? customSize = null)
    {
        var profileName = NormalizeProfile(profile);
        return $"Sphere-{FormatProfileName(profileName)}";
    }

    /// <summary>
    /// Returns the index name that ImportAsync creates, for use by the benchmark runner.
    /// </summary>
    public static string GetIndexName(
        VectorQuantization quantization = VectorQuantization.None,
        IndexingEngine searchEngine = IndexingEngine.Corax)
    {
        var engineSuffix = searchEngine == IndexingEngine.Lucene ? "-lucene" : "-corax";
        return quantization switch
        {
            VectorQuantization.Int8 => $"Sphere/ByVectorInt8{engineSuffix}",
            VectorQuantization.Binary => $"Sphere/ByVectorBinary{engineSuffix}",
            _ => $"Sphere/ByVector{engineSuffix}"
        };
    }

    public async Task<bool> IsDatasetImportedAsync(
        string serverUrl,
        string databaseName,
        int expectedMinDocuments = 1000,
        Version? httpVersion = null)
    {
        try
        {
            using var store = new DocumentStore
            {
                Urls = new[] { serverUrl },
                Database = databaseName
            };
            if (httpVersion != null)
                HttpHelper.ConfigureHttpVersion(store, httpVersion, HttpVersionPolicy.RequestVersionExact);
            store.Initialize();

            var dbRecord = await store.Maintenance.Server.SendAsync(
                new GetDatabaseRecordOperation(databaseName));
            if (dbRecord == null)
            {
                Console.WriteLine($"[Sphere] Database '{databaseName}' does not exist");
                return false;
            }

            var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
            if (stats.CountOfDocuments < expectedMinDocuments)
            {
                Console.WriteLine($"[Sphere] Database '{databaseName}' has {stats.CountOfDocuments} documents (expected >= {expectedMinDocuments})");
                return false;
            }

            using var session = store.OpenAsyncSession();
            var collectionExists = await session.Advanced.AsyncRawQuery<object>($"from {CollectionName}")
                .Take(1)
                .AnyAsync();

            if (collectionExists == false)
            {
                Console.WriteLine($"[Sphere] Database '{databaseName}' exists but '{CollectionName}' collection is missing");
                return false;
            }

            Console.WriteLine($"[Sphere] Database '{databaseName}' already has {stats.CountOfDocuments:N0} documents — skipping import");
            return true;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Sphere] Import check failed: {ex.Message}");
            return false;
        }
    }

    /// <summary>
    /// Imports SPHERE data by streaming from the Weaviate-hosted .jsonl.tar.gz into RavenDB bulk insert.
    /// Downloads directly from GCS, or uses a local file if sourcePathOverride is provided.
    /// No intermediate decompressed files on disk. Documents are keyed by SHA for idempotent upsert.
    /// </summary>
    public async Task<(TimeSpan importTime, TimeSpan indexingTime)> ImportAsync(
        string serverUrl,
        string databaseName,
        string profile,
        string? sourcePathOverride = null,
        string? cacheDir = null,
        VectorQuantization quantization = VectorQuantization.None,
        IndexingEngine searchEngine = IndexingEngine.Corax,
        Version? httpVersion = null)
    {
        var profileName = NormalizeProfile(profile);
        var (expectedLines, sourceUrl) = Profiles[profileName];
        var sourcePath = ResolveSourcePath(profileName, sourceUrl, sourcePathOverride, cacheDir);

        using var store = new DocumentStore
        {
            Urls = new[] { serverUrl },
            Database = databaseName
        };
        if (httpVersion != null)
            HttpHelper.ConfigureHttpVersion(store, httpVersion, HttpVersionPolicy.RequestVersionExact);
        store.Initialize();

        // Create database if needed
        var dbRecord = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
        if (dbRecord == null)
        {
            Console.WriteLine($"[Sphere] Creating database: {databaseName}");
            await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(databaseName)));
        }

        // Check existing document count for resume
        var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
        long existingDocCount = stats.CountOfDocuments;

        // Exclude the ground truth cache document from the count
        if (existingDocCount > 0)
        {
            using var checkSession = store.OpenAsyncSession();
            var groundTruthExists = await checkSession.LoadAsync<object>(GroundTruthDocumentId);
            if (groundTruthExists != null)
                existingDocCount--;
        }

        if (existingDocCount >= expectedLines)
        {
            Console.WriteLine($"[Sphere] Database already has {existingDocCount:N0} documents (expected {expectedLines:N0}) — skipping import");
            return (TimeSpan.Zero, TimeSpan.Zero);
        }

        // Import phase — measure wall clock
        Console.WriteLine($"[Sphere] Importing {profileName} profile ({expectedLines:N0} passages) from {sourcePath}");
        if (existingDocCount > 0)
            Console.WriteLine($"[Sphere] Resuming — skipping first {existingDocCount:N0} already-imported lines");

        var importSw = Stopwatch.StartNew();
        long imported = 0;
        long skipped = 0;
        int batchCount = 0;
        var lastReport = DateTime.UtcNow;

        await using var sourceStream = OpenSourceStream(sourcePath);
        await using var gzipStream = new GZipStream(sourceStream, CompressionMode.Decompress);
        await using var tarReader = new TarReader(gzipStream);

        while (await tarReader.GetNextEntryAsync() is { } entry)
        {
            if (entry.EntryType != TarEntryType.RegularFile)
                continue;

            if (entry.Name.EndsWith(".jsonl", StringComparison.OrdinalIgnoreCase) == false)
                continue;

            var dataStream = entry.DataStream;
            if (dataStream == null)
                continue;

            using var reader = new StreamReader(dataStream, leaveOpen: true);

            // Batch bulk inserts to avoid single-session overhead at large scale
            var bulkInsert = store.BulkInsert();
            try
            {
                while (await reader.ReadLineAsync() is { } line)
                {
                    if (string.IsNullOrWhiteSpace(line))
                        continue;

                    // Resume: skip already-imported lines
                    if (skipped < existingDocCount)
                    {
                        skipped++;
                        continue;
                    }

                    // Stop at profile line limit
                    if (imported + existingDocCount >= expectedLines)
                        break;

                    using var jsonDoc = JsonDocument.Parse(line);
                    var root = jsonDoc.RootElement;

                    var passageId = root.GetProperty("id").GetString() ?? "";
                    var raw = root.GetProperty("raw").GetString() ?? "";
                    var sha = root.GetProperty("sha").GetString() ?? "";
                    var title = root.GetProperty("title").GetString() ?? "";
                    var url = root.GetProperty("url").GetString() ?? "";

                    var vectorElement = root.GetProperty("vector");
                    var vectorFloats = new float[vectorElement.GetArrayLength()];
                    int vi = 0;
                    foreach (var v in vectorElement.EnumerateArray())
                        vectorFloats[vi++] = v.GetSingle();

                    var doc = new SphereDocument
                    {
                        Raw = raw,
                        Sha = sha,
                        Title = title,
                        Url = url
                    };

                    // Use passage UUID as document ID — SHAs are per-document, not per-passage
                    var docId = $"sphere/{passageId}";
                    await bulkInsert.StoreAsync(doc, docId);

                    // Store vector as binary attachment — keeps documents lean
                    var vectorBytes = new byte[vectorFloats.Length * sizeof(float)];
                    Buffer.BlockCopy(vectorFloats, 0, vectorBytes, 0, vectorBytes.Length);
                    await bulkInsert.AttachmentsFor(docId).StoreAsync("vector", new MemoryStream(vectorBytes));
                    imported++;
                    batchCount++;

                    // Rotate bulk insert session periodically to bound server-side memory
                    if (batchCount >= BulkInsertBatchSize)
                    {
                        bulkInsert.Dispose();
                        bulkInsert = store.BulkInsert();
                        batchCount = 0;
                    }

                    // Progress reporting
                    if (DateTime.UtcNow - lastReport > TimeSpan.FromSeconds(5))
                    {
                        var total = imported + existingDocCount;
                        var pct = expectedLines > 0 ? (double)total / expectedLines * 100 : 0;
                        var elapsed = importSw.Elapsed;
                        var docsPerSec = imported / elapsed.TotalSeconds;
                        Console.WriteLine($"[Sphere] Imported {total:N0}/{expectedLines:N0} ({pct:F1}%) — {docsPerSec:N0} docs/sec");
                        lastReport = DateTime.UtcNow;
                    }
                }
            }
            finally
            {
                bulkInsert.Dispose();
            }

            if (imported + existingDocCount >= expectedLines)
                break;
        }

        importSw.Stop();
        var importTime = importSw.Elapsed;
        Console.WriteLine($"[Sphere] Import complete: {imported:N0} new documents in {importTime}");

        // Create vector index
        var indexName = GetIndexName(quantization, searchEngine);
        var engineName = searchEngine == IndexingEngine.Lucene ? "Lucene" : "Corax";

        var (sourceType, destType) = quantization switch
        {
            VectorQuantization.Int8 => (VectorEmbeddingType.Single, VectorEmbeddingType.Int8),
            VectorQuantization.Binary => (VectorEmbeddingType.Single, VectorEmbeddingType.Binary),
            _ => (VectorEmbeddingType.Single, VectorEmbeddingType.Single)
        };

        Console.WriteLine($"[Sphere] Creating vector index '{indexName}' (quantization: {quantization}, engine: {engineName})...");

        var index = new IndexDefinition
        {
            Name = indexName,
            Maps = new HashSet<string>
            {
                $@"from d in docs.{CollectionName}
let attachment = LoadAttachment(d, ""vector"")
select new {{ d.Title, Embedding = CreateVector(attachment.GetContentAsStream()) }}"
            },
            Fields = new Dictionary<string, IndexFieldOptions>
            {
                {
                    "Embedding",
                    new IndexFieldOptions
                    {
                        Vector = new VectorOptions
                        {
                            Dimensions = DprDimensions,
                            SourceEmbeddingType = sourceType,
                            DestinationEmbeddingType = destType
                        }
                    }
                }
            },
            Configuration = new IndexConfiguration { { "Indexing.Static.SearchEngineType", engineName } }
        };

        await store.Maintenance.SendAsync(new PutIndexesOperation(index));
        Console.WriteLine($"[Sphere] Created index '{indexName}'");

        // Measure indexing time (wait for non-stale)
        Console.WriteLine($"[Sphere] Waiting for index to become non-stale...");
        var indexingSw = Stopwatch.StartNew();
        using var session = store.OpenAsyncSession();
        session.Advanced.MaxNumberOfRequestsPerSession = int.MaxValue;
        await session.Query<SphereDocument>(indexName)
            .Customize(x => x.WaitForNonStaleResults(TimeSpan.MaxValue))
            .Take(0)
            .ToListAsync();
        indexingSw.Stop();
        var indexingTime = indexingSw.Elapsed;

        Console.WriteLine($"[Sphere] Indexing complete in {indexingTime}");
        Console.WriteLine($"[Sphere] Import time: {importTime}, Indexing time: {indexingTime}");

        return (importTime, indexingTime);
    }

    /// <summary>
    /// Generates VectorWorkloadMetadata by sampling query vectors from imported embeddings.
    /// </summary>
    public async Task<VectorWorkloadMetadata> GenerateQueryVectorsAsync(
        string serverUrl,
        string databaseName,
        int queryCount = 1000,
        int topK = 10,
        Version? httpVersion = null)
    {
        using var store = new DocumentStore
        {
            Urls = new[] { serverUrl },
            Database = databaseName
        };
        if (httpVersion != null)
            HttpHelper.ConfigureHttpVersion(store, httpVersion, HttpVersionPolicy.RequestVersionExact);
        store.Initialize();

        var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
        long baseVectorCount = stats.CountOfDocuments;

        // Sample random documents and load their vector attachments
        Console.WriteLine($"[Sphere] Sampling {queryCount} query vectors from {baseVectorCount:N0} documents...");
        var queryVectors = new List<float[]>();

        using var session = store.OpenAsyncSession();
        session.Advanced.MaxNumberOfRequestsPerSession = int.MaxValue;

        var samples = await session.Query<SphereDocument>()
            .Customize(x => x.RandomOrdering())
            .Take(queryCount)
            .ToListAsync();

        foreach (var sample in samples)
        {
            var docId = session.Advanced.GetDocumentId(sample);
            var vector = await LoadVectorAttachmentAsync(session, docId);
            if (vector != null)
                queryVectors.Add(vector);
        }

        Console.WriteLine($"[Sphere] Sampled {queryVectors.Count} query vectors ({DprDimensions}D)");

        // Compute ground truth
        var groundTruth = await ComputeOrLoadGroundTruthAsync(
            store, queryVectors, topK, baseVectorCount);

        return new VectorWorkloadMetadata
        {
            QueryVectors = queryVectors.ToArray(),
            FieldName = "Embedding",
            VectorDimensions = DprDimensions,
            BaseVectorCount = baseVectorCount,
            GroundTruth = groundTruth
        };
    }

    /// <summary>
    /// Computes brute-force ground truth for recall@K, or loads from cache.
    /// For large datasets (10M+), computation is skipped (too expensive).
    /// </summary>
    private async Task<Dictionary<int, int[]>?> ComputeOrLoadGroundTruthAsync(
        IDocumentStore store,
        List<float[]> queryVectors,
        int topK,
        long baseVectorCount)
    {
        // Try to load cached ground truth
        using var loadSession = store.OpenAsyncSession(new SessionOptions { NoTracking = true });
        var cached = await loadSession.LoadAsync<GroundTruthCacheDocument>(GroundTruthDocumentId);
        if (cached != null && cached.QueryCount == queryVectors.Count && cached.TopK == topK)
        {
            Console.WriteLine($"[Sphere] Loading cached ground truth ({cached.QueryCount} queries, top-{cached.TopK})");
            var groundTruth = new Dictionary<int, int[]>();
            foreach (var (queryIdx, docIds) in cached.GroundTruth)
            {
                // Use deterministic hash for recall comparison
                groundTruth[queryIdx] = docIds.Select(id => DeterministicHash(id)).ToArray();
            }
            return groundTruth;
        }

        // For very large datasets, skip ground truth (too expensive without GPU)
        if (baseVectorCount > 10_000_000)
        {
            Console.WriteLine($"[Sphere] Skipping brute-force ground truth for {baseVectorCount:N0} vectors (too large)");
            return null;
        }

        Console.WriteLine($"[Sphere] Computing brute-force ground truth ({queryVectors.Count} queries × {baseVectorCount:N0} vectors, top-{topK})...");

        // Load all vectors for brute-force computation using NoTracking to avoid memory bloat
        var allVectors = new List<(string Id, float[] Vector)>();
        const int pageSize = 1024;

        int page = 0;
        while (true)
        {
            using var pageSession = store.OpenAsyncSession();
            pageSession.Advanced.MaxNumberOfRequestsPerSession = int.MaxValue;
            var batch = await pageSession.Advanced.AsyncRawQuery<SphereDocument>(
                $"from {CollectionName} limit $take offset $skip")
                .AddParameter("skip", page * pageSize)
                .AddParameter("take", pageSize)
                .ToListAsync();

            if (batch.Count == 0)
                break;

            foreach (var doc in batch)
            {
                var id = pageSession.Advanced.GetDocumentId(doc);
                var vector = await LoadVectorAttachmentAsync(pageSession, id);
                if (vector != null)
                    allVectors.Add((id, vector));
            }
            pageSession.Advanced.Clear();
            page++;

            if (page % 100 == 0)
                Console.WriteLine($"[Sphere] Loaded {allVectors.Count:N0} vectors for ground truth...");
        }

        // Compute exact nearest neighbors for each query
        var groundTruthResult = new Dictionary<int, int[]>();
        var groundTruthDocIds = new Dictionary<int, string[]>();

        for (int q = 0; q < queryVectors.Count; q++)
        {
            var queryVec = queryVectors[q];

            var scored = allVectors
                .Select((item, idx) => (Id: item.Id, Similarity: CosineSimilarity(queryVec, item.Vector)))
                .OrderByDescending(x => x.Similarity)
                .Take(topK)
                .ToArray();

            groundTruthResult[q] = scored.Select(x => DeterministicHash(x.Id)).ToArray();
            groundTruthDocIds[q] = scored.Select(x => x.Id).ToArray();

            if ((q + 1) % 100 == 0)
                Console.WriteLine($"[Sphere] Ground truth: {q + 1}/{queryVectors.Count} queries computed");
        }

        // Cache ground truth in the database
        using var storeSession = store.OpenAsyncSession();
        var cacheDoc = new GroundTruthCacheDocument
        {
            GroundTruth = groundTruthDocIds,
            QueryCount = queryVectors.Count,
            TopK = topK,
            BaseVectorCount = allVectors.Count
        };
        await storeSession.StoreAsync(cacheDoc, GroundTruthDocumentId);
        await storeSession.SaveChangesAsync();

        Console.WriteLine($"[Sphere] Ground truth cached ({queryVectors.Count} queries, top-{topK})");

        return groundTruthResult;
    }

    /// <summary>
    /// Loads a vector from a document's binary "vector" attachment.
    /// Returns null if the attachment doesn't exist.
    /// </summary>
    private static async Task<float[]?> LoadVectorAttachmentAsync(IAsyncDocumentSession session, string docId)
    {
        using var result = await session.Advanced.Attachments.GetAsync(docId, "vector");
        if (result == null)
            return null;

        using var ms = new MemoryStream();
        await result.Stream.CopyToAsync(ms);
        var bytes = ms.ToArray();
        var floats = new float[bytes.Length / sizeof(float)];
        Buffer.BlockCopy(bytes, 0, floats, 0, bytes.Length);
        return floats;
    }

    /// <summary>
    /// Deterministic hash for document IDs — consistent across process restarts.
    /// .NET's String.GetHashCode() is randomized by default.
    /// </summary>
    private static int DeterministicHash(string s)
    {
        unchecked
        {
            int hash = 17;
            foreach (char c in s)
                hash = hash * 31 + c;
            return hash;
        }
    }

    private static float CosineSimilarity(float[] a, float[] b)
    {
        float dot = 0, normA = 0, normB = 0;
        for (int i = 0; i < a.Length; i++)
        {
            dot += a[i] * b[i];
            normA += a[i] * a[i];
            normB += b[i] * b[i];
        }
        var denom = MathF.Sqrt(normA) * MathF.Sqrt(normB);
        return denom == 0 ? 0 : dot / denom;
    }

    /// <summary>
    /// Resolves the source path for SPHERE data: local override, cached download, or streaming URL.
    /// </summary>
    private static string ResolveSourcePath(string profileName, string sourceUrl, string? sourcePathOverride, string? cacheDir)
    {
        // 1. Explicit local file override
        if (string.IsNullOrEmpty(sourcePathOverride) == false)
        {
            if (File.Exists(sourcePathOverride))
                return sourcePathOverride;
            throw new FileNotFoundException($"SPHERE source file not found: {sourcePathOverride}");
        }

        // 2. Check environment variable
        var envPath = Environment.GetEnvironmentVariable("SPHERE_DATA_PATH");
        if (string.IsNullOrEmpty(envPath) == false && File.Exists(envPath))
            return envPath;

        // 3. Check cache directory for previously downloaded file
        var effectiveCacheDir = cacheDir ?? Path.Combine(Path.GetTempPath(), "RavenBench", "datasets");
        Directory.CreateDirectory(effectiveCacheDir);
        var cachedFile = Path.Combine(effectiveCacheDir, $"full.sphere.{profileName}.jsonl.tar.gz");

        if (File.Exists(cachedFile))
        {
            Console.WriteLine($"[Sphere] Using cached file: {cachedFile}");
            return cachedFile;
        }

        // 4. Download from GCS
        Console.WriteLine($"[Sphere] Downloading {profileName} from {sourceUrl}");
        Console.WriteLine($"[Sphere] (If access is denied, request access via hello@weaviate.io)");
        DownloadFile(sourceUrl, cachedFile);
        return cachedFile;
    }

    private static void DownloadFile(string url, string destinationPath)
    {
        var tempPath = destinationPath + ".downloading";
        try
        {
            using var response = SharedHttpClient.GetAsync(url, HttpCompletionOption.ResponseHeadersRead).GetAwaiter().GetResult();
            response.EnsureSuccessStatusCode();

            var totalBytes = response.Content.Headers.ContentLength;
            using var contentStream = response.Content.ReadAsStream();
            using var fileStream = new FileStream(tempPath, FileMode.Create, FileAccess.Write, FileShare.None, 81920);

            var buffer = new byte[81920];
            long downloaded = 0;
            int bytesRead;
            var lastReport = DateTime.UtcNow;

            while ((bytesRead = contentStream.Read(buffer, 0, buffer.Length)) > 0)
            {
                fileStream.Write(buffer, 0, bytesRead);
                downloaded += bytesRead;

                if (DateTime.UtcNow - lastReport > TimeSpan.FromSeconds(5))
                {
                    var pct = totalBytes > 0 ? (double)downloaded / totalBytes.Value * 100 : 0;
                    Console.WriteLine($"[Sphere] Downloaded {FormatBytes(downloaded)}" +
                        (totalBytes > 0 ? $" / {FormatBytes(totalBytes.Value)} ({pct:F1}%)" : ""));
                    lastReport = DateTime.UtcNow;
                }
            }

            Console.WriteLine($"[Sphere] Download complete: {FormatBytes(downloaded)}");
        }
        catch
        {
            // Clean up partial download
            if (File.Exists(tempPath))
                File.Delete(tempPath);
            throw;
        }

        // Validate the download is real data, not a Weaviate access-gate placeholder
        var fileInfo = new FileInfo(tempPath);
        if (fileInfo.Length < 10_000)
        {
            // Small file is likely a placeholder — check content
            try
            {
                using var checkGz = new GZipStream(File.OpenRead(tempPath), CompressionMode.Decompress);
                using var checkTar = new TarReader(checkGz);
                while (checkTar.GetNextEntry() is { } entry)
                {
                    if (entry.DataStream == null) continue;
                    using var sr = new StreamReader(entry.DataStream);
                    var firstLine = sr.ReadLine() ?? "";
                    if (firstLine.Contains("hello@weaviate.io", StringComparison.OrdinalIgnoreCase) ||
                        firstLine.Contains("request", StringComparison.OrdinalIgnoreCase))
                    {
                        File.Delete(tempPath);
                        throw new InvalidOperationException(
                            "SPHERE dataset access is gated. The GCS URL returned a placeholder.\n" +
                            "Request the real download link from Weaviate: hello@weaviate.io\n" +
                            "Then either:\n" +
                            "  - Set SPHERE_DATA_PATH=/path/to/full.sphere.100k.jsonl.tar.gz\n" +
                            "  - Or place the file in the dataset cache directory");
                    }
                    break;
                }
            }
            catch (InvalidOperationException) { throw; }
            catch { /* Not a valid tar.gz — let the import fail naturally */ }
        }

        File.Move(tempPath, destinationPath);
    }

    private static string FormatBytes(long bytes)
    {
        string[] sizes = { "B", "KB", "MB", "GB", "TB" };
        double len = bytes;
        int order = 0;
        while (len >= 1024 && order < sizes.Length - 1) { order++; len /= 1024; }
        return $"{len:0.##} {sizes[order]}";
    }

    private static Stream OpenSourceStream(string path)
    {
        if (File.Exists(path) == false)
            throw new FileNotFoundException($"SPHERE source file not found: {path}");

        return File.OpenRead(path);
    }

    private static string NormalizeProfile(string? profile)
    {
        if (string.IsNullOrWhiteSpace(profile))
            return "100k";

        var normalized = profile.Trim().ToLowerInvariant();
        if (Profiles.ContainsKey(normalized) == false)
            throw new ArgumentException(
                $"Invalid SPHERE profile: '{profile}'. Valid profiles: {string.Join(", ", Profiles.Keys)}");

        return normalized;
    }

    private static string FormatProfileName(string profile)
    {
        return profile.ToUpperInvariant() switch
        {
            "100K" => "100K",
            "1M" => "1M",
            "10M" => "10M",
            "100M" => "100M",
            "FULL" => "Full",
            _ => profile.ToUpperInvariant()
        };
    }
}
