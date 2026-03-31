using System.Diagnostics;
using System.Formats.Tar;
using System.IO.Compression;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using RavenBench.Core;
using RavenBench.Core.Workload;

namespace RavenBench.Dataset;

/// <summary>
/// SPHERE dataset provider — streaming import of Meta's SPHERE passages with pre-computed DPR embeddings.
/// Supports profiles from 100K to 899M passages. Streams .jsonl.tar.gz files directly into RavenDB
/// bulk insert with no intermediate decompressed files on disk.
/// </summary>
public sealed class SphereDatasetProvider : IDatasetProvider
{
    public const int VectorDimensions = 768; // facebook-dpr-ctx_encoder-single-nq-base
    public const string CollectionName = "Passages";
    private const string CheckpointDocId = "sphere/import-checkpoint";
    private const int ProgressInterval = 10_000;
    private const int CheckpointInterval = 100_000;

    private readonly string _profile;

    private static readonly Dictionary<string, SphereProfile> Profiles = new(StringComparer.OrdinalIgnoreCase)
    {
        { "100k", new(100_000, "Sphere-100K") },
        { "1m", new(1_000_000, "Sphere-1M") },
        { "10m", new(10_000_000, "Sphere-10M") },
        { "100m", new(100_000_000, "Sphere-100M") },
        { "full", new(899_000_000, "Sphere-Full") },
    };

    public record SphereProfile(long TargetDocCount, string DatabaseName);

    public sealed class SphereJsonLine
    {
        [JsonPropertyName("id")]
        public string Id { get; set; } = "";
        [JsonPropertyName("raw")]
        public string Raw { get; set; } = "";
        [JsonPropertyName("sha")]
        public string Sha { get; set; } = "";
        [JsonPropertyName("title")]
        public string Title { get; set; } = "";
        [JsonPropertyName("url")]
        public string Url { get; set; } = "";
        [JsonPropertyName("vector")]
        public float[] Vector { get; set; } = [];
    }

    private record Passage(string Text, string Sha, string Title, string Url);

    private sealed class ImportCheckpoint
    {
        public long LinesImported { get; set; }
        public string? LastSha { get; set; }
        public DateTimeOffset Timestamp { get; set; }
    }

    public record ImportResult(long DocumentsImported, TimeSpan ImportDuration, TimeSpan IndexingDuration);

    public SphereDatasetProvider(string profile = "100k")
    {
        if (Profiles.ContainsKey(profile) == false)
        {
            var valid = string.Join(", ", Profiles.Keys);
            throw new ArgumentException($"Unknown SPHERE profile: '{profile}'. Valid profiles: {valid}");
        }
        _profile = profile;
    }

    public string DatasetName => "sphere";
    public string Profile => _profile;

    public static IReadOnlyCollection<string> AvailableProfiles => Profiles.Keys;

    public DatasetInfo GetDatasetInfo(string? profile = null, int? customSize = null)
    {
        var p = ResolveProfile(profile);
        return new DatasetInfo
        {
            Name = $"SPHERE-{(profile ?? _profile).ToUpperInvariant()}",
            Description = $"SPHERE {p.TargetDocCount:N0} passages with {VectorDimensions}D DPR embeddings",
            MaxQuestionId = 0,
            MaxUserId = 0,
            Files = new()
        };
    }

    public string GetDatabaseName(string? profile = null, int? customSize = null)
    {
        return ResolveProfile(profile).DatabaseName;
    }

    public async Task<bool> IsDatasetImportedAsync(string serverUrl, string databaseName,
        int expectedMinDocuments = 1000, Version? httpVersion = null)
    {
        try
        {
            using var store = new DocumentStore { Urls = [serverUrl], Database = databaseName };
            if (httpVersion != null)
                HttpHelper.ConfigureHttpVersion(store, httpVersion, HttpVersionPolicy.RequestVersionExact);
            store.Initialize();

            var dbRecord = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
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
            var passagesExist = await session.Advanced.AsyncRawQuery<object>($"from {CollectionName}")
                .Take(1)
                .AnyAsync();

            if (passagesExist == false)
            {
                Console.WriteLine($"[Sphere] Database '{databaseName}' exists but '{CollectionName}' collection is missing");
                return false;
            }

            Console.WriteLine($"[Sphere] Database '{databaseName}' already has {stats.CountOfDocuments:N0} documents - skipping import");
            return true;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Sphere] Skip check failed: {ex.Message}");
            return false;
        }
    }

    /// <summary>
    /// Streams a .jsonl.tar.gz source into RavenDB via bulk insert. Supports resume and measures
    /// import time and indexing time separately.
    /// </summary>
    public async Task<ImportResult> ImportAsync(
        string serverUrl,
        string databaseName,
        string dataSourcePath,
        VectorQuantization quantization = VectorQuantization.None,
        bool exactSearch = false,
        Version? httpVersion = null,
        IndexingEngine searchEngine = IndexingEngine.Corax,
        int? numberOfEdges = null,
        int? numberOfCandidatesForIndexing = null,
        CancellationToken ct = default)
    {
        using var store = new DocumentStore { Urls = [serverUrl], Database = databaseName };
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

        var profile = ResolveProfile(null);

        // Check existing document count
        var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
        bool documentsExist = stats.CountOfDocuments >= profile.TargetDocCount;

        var importSw = Stopwatch.StartNew();
        long totalImported;

        if (documentsExist == false)
        {
            // Load resume checkpoint
            var checkpoint = await LoadCheckpointAsync(store);
            long skipLines = checkpoint?.LinesImported ?? 0;

            if (skipLines > 0)
                Console.WriteLine($"[Sphere] Resuming from line {skipLines:N0} (checkpoint: {checkpoint!.LastSha})");

            // Resolve data source files
            var files = ResolveSourceFiles(dataSourcePath, _profile);
            Console.WriteLine($"[Sphere] Importing from {files.Count} file(s) into {databaseName} (target: {profile.TargetDocCount:N0} docs)");

            long imported = 0;
            long skipped = 0;
            var rateSw = Stopwatch.StartNew();
            string? lastSha = null;

            using (var bulkInsert = store.BulkInsert())
            {
                foreach (var file in files)
                {
                    Console.WriteLine($"[Sphere] Processing: {Path.GetFileName(file)}");

                    await foreach (var line in StreamJsonLinesAsync(file, ct))
                    {
                        if (skipped < skipLines)
                        {
                            skipped++;
                            continue;
                        }

                        if (imported >= profile.TargetDocCount)
                            break;

                        var doc = new Passage(line.Raw, line.Sha, line.Title, line.Url);
                        var docId = string.IsNullOrEmpty(line.Id) ? $"{CollectionName}/{line.Sha}" : $"{CollectionName}/{line.Id}";
                        await bulkInsert.StoreAsync(doc, docId);

                        // Store vector as binary attachment (768D × 4 bytes = 3072 bytes)
                        var vectorBytes = new byte[line.Vector.Length * sizeof(float)];
                        Buffer.BlockCopy(line.Vector, 0, vectorBytes, 0, vectorBytes.Length);
                        using var vectorStream = new MemoryStream(vectorBytes);
                        bulkInsert.AttachmentsFor(docId).Store("vector", vectorStream);
                        imported++;
                        lastSha = line.Sha;

                        if (imported % ProgressInterval == 0)
                        {
                            var docsPerSec = imported / rateSw.Elapsed.TotalSeconds;
                            var pct = (double)imported / profile.TargetDocCount * 100;
                            Console.Write($"\r[Sphere] Imported {imported:N0}/{profile.TargetDocCount:N0} ({pct:F1}%, {docsPerSec:N0} docs/sec)");
                        }

                        if (imported % CheckpointInterval == 0)
                        {
                            await StoreCheckpointAsync(store, skipLines + imported, lastSha);
                        }
                    }

                    if (imported >= profile.TargetDocCount)
                        break;
                }
            }

            Console.WriteLine($"\n[Sphere] Import complete: {imported:N0} documents in {importSw.Elapsed}");
            totalImported = imported;

            if (imported >= profile.TargetDocCount)
                await ClearCheckpointAsync(store);
        }
        else
        {
            Console.WriteLine($"[Sphere] Database already contains {stats.CountOfDocuments:N0} documents - skipping import");
            totalImported = stats.CountOfDocuments;
        }

        importSw.Stop();
        var importDuration = importSw.Elapsed;

        // Create vector index and measure indexing time
        var indexingSw = Stopwatch.StartNew();
        await CreateVectorIndexAsync(store, quantization, exactSearch, searchEngine, numberOfEdges, numberOfCandidatesForIndexing);
        indexingSw.Stop();
        var indexingDuration = indexingSw.Elapsed;

        Console.WriteLine($"[Sphere] Import: {importDuration}, Indexing: {indexingDuration}");

        return new ImportResult(totalImported, importDuration, indexingDuration);
    }

    /// <summary>
    /// Generates query vectors by sampling random documents from the imported SPHERE data.
    /// </summary>
    public async Task<VectorWorkloadMetadata> GenerateQueryVectorsAsync(
        string serverUrl, string databaseName, int count = 1000, Version? httpVersion = null, int seed = 42)
    {
        using var store = new DocumentStore { Urls = [serverUrl], Database = databaseName };
        if (httpVersion != null)
            HttpHelper.ConfigureHttpVersion(store, httpVersion, HttpVersionPolicy.RequestVersionExact);
        store.Initialize();

        var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
        var totalDocs = stats.CountOfDocuments;

        if (totalDocs == 0)
            throw new InvalidOperationException($"No documents found in {CollectionName} collection. Import the SPHERE dataset first.");

        // Deterministic sampling: load a block of documents ordered by id, then pick
        // with a seeded RNG. Same seed → same query vectors → ground truth cache reusable.
        Console.WriteLine($"[Sphere] Sampling {count} query vectors from {databaseName} (seed={seed})...");

        using var session = store.OpenAsyncSession();
        session.Advanced.MaxNumberOfRequestsPerSession = int.MaxValue;

        // Load a pool of document IDs, then sample and load their vector attachments
        var poolSize = (int)Math.Min(totalDocs, count * 3);
        var pool = await session.Advanced.AsyncRawQuery<Passage>(
                $"from {CollectionName} order by id()")
            .Take(poolSize)
            .ToListAsync();

        // Deterministic shuffle and pick
        var rng = new Random(seed);
        var indices = Enumerable.Range(0, pool.Count).ToArray();
        for (int i = indices.Length - 1; i > 0; i--)
        {
            int j = rng.Next(i + 1);
            (indices[i], indices[j]) = (indices[j], indices[i]);
        }

        var selectedIndices = indices.Take(Math.Min(count, pool.Count)).ToArray();
        var selected = new float[selectedIndices.Length][];
        for (int i = 0; i < selectedIndices.Length; i++)
        {
            var docId = session.Advanced.GetDocumentId(pool[selectedIndices[i]]);
            using var attachmentResult = await session.Advanced.Attachments.GetAsync(docId, "vector");
            if (attachmentResult == null)
                throw new InvalidOperationException($"Document {docId} has no 'vector' attachment. Was the dataset imported with attachment-based vectors?");
            var bytes = new byte[VectorDimensions * sizeof(float)];
            await attachmentResult.Stream.ReadExactlyAsync(bytes);
            var floats = new float[VectorDimensions];
            Buffer.BlockCopy(bytes, 0, floats, 0, bytes.Length);
            selected[i] = floats;
        }

        Console.WriteLine($"[Sphere] Sampled {selected.Length} query vectors ({VectorDimensions}D) from {totalDocs:N0} documents");

        return new VectorWorkloadMetadata
        {
            QueryVectors = selected,
            FieldName = "Embedding",
            VectorDimensions = VectorDimensions,
            BaseVectorCount = totalDocs
        };
    }

    /// <summary>
    /// Resolves the data source path to a list of .jsonl.tar.gz or .jsonl.gz files.
    /// If a profile is specified, looks for the profile-specific file first and downloads if missing.
    /// </summary>
    public static List<string> ResolveSourceFiles(string dataSourcePath, string? profile = null)
    {
        // If a specific file was provided, use it directly
        if (File.Exists(dataSourcePath))
            return [dataSourcePath];

        // When we know the profile, look for the profile-specific file first
        if (profile != null && ProfileFileNames.TryGetValue(profile, out var expectedFileName))
        {
            // Search in the provided path and datasets/sphere/ directories
            var searchDirs = new List<string>();
            if (Directory.Exists(dataSourcePath))
                searchDirs.Add(dataSourcePath);

            foreach (var startDir in new[] { Directory.GetCurrentDirectory(), AppContext.BaseDirectory })
            {
                var dir = new DirectoryInfo(startDir);
                while (dir != null)
                {
                    var sphereDir = Path.Combine(dir.FullName, "datasets", "sphere");
                    if (Directory.Exists(sphereDir))
                        searchDirs.Add(sphereDir);
                    dir = dir.Parent;
                }
            }

            // Check if the profile-specific file exists in any search directory
            foreach (var searchDir in searchDirs)
            {
                var profileFile = Path.Combine(searchDir, expectedFileName);
                if (File.Exists(profileFile))
                    return [profileFile];
            }

            // Profile file not found locally — download it
            var targetDir = searchDirs.Count > 0 ? searchDirs[0]
                : Path.Combine(Directory.GetCurrentDirectory(), "datasets", "sphere");
            var downloaded = DownloadSphereFileAsync(profile, targetDir).GetAwaiter().GetResult();
            if (downloaded != null)
                return [downloaded];
        }

        // Fallback: find any sphere files in the provided path or datasets/sphere/
        if (Directory.Exists(dataSourcePath))
        {
            var files = FindSphereFiles(dataSourcePath);
            if (files.Count > 0)
                return files;
        }

        foreach (var startDir in new[] { Directory.GetCurrentDirectory(), AppContext.BaseDirectory })
        {
            var dir = new DirectoryInfo(startDir);
            while (dir != null)
            {
                var sphereDir = Path.Combine(dir.FullName, "datasets", "sphere");
                if (Directory.Exists(sphereDir))
                {
                    var files = FindSphereFiles(sphereDir);
                    if (files.Count > 0)
                        return files;
                }
                dir = dir.Parent;
            }
        }

        throw new FileNotFoundException(
            $"SPHERE data source not found at '{dataSourcePath}'. " +
            "Provide a .jsonl.tar.gz file, a directory containing them, or place files in datasets/sphere/.");
    }

    // Profile -> GCS file mapping. Available at: https://storage.googleapis.com/sphere-demo/
    private static readonly Dictionary<string, string> ProfileFileNames = new(StringComparer.OrdinalIgnoreCase)
    {
        { "100k", "full.sphere.100k.jsonl.tar.gz" },
        { "1m", "full.sphere.1M.jsonl.tar.gz" },
        { "10m", "full.sphere.10M.jsonl.tar.gz" },
        { "100m", "full.sphere.100M.jsonl.tar.gz" },
        { "full", "full.sphere.899M.jsonl.tar.gz" },
    };

    private static async Task<string?> DownloadSphereFileAsync(string profile, string targetDir)
    {
        if (ProfileFileNames.TryGetValue(profile, out var fileName) == false)
            return null;

        Directory.CreateDirectory(targetDir);
        var targetPath = Path.Combine(targetDir, fileName);

        if (File.Exists(targetPath))
            return targetPath;

        var url = $"https://storage.googleapis.com/sphere-demo/{fileName}";
        Console.WriteLine($"[Sphere] Downloading {fileName} from {url}...");

        using var httpClient = new HttpClient { Timeout = TimeSpan.FromHours(12) };
        using var response = await httpClient.GetAsync(url, HttpCompletionOption.ResponseHeadersRead);
        response.EnsureSuccessStatusCode();

        var totalBytes = response.Content.Headers.ContentLength;
        var totalMB = totalBytes.HasValue ? $"{totalBytes.Value / (1024.0 * 1024.0):N0} MB" : "unknown size";
        Console.WriteLine($"[Sphere] Download size: {totalMB}");

        await using var contentStream = await response.Content.ReadAsStreamAsync();
        var tempPath = targetPath + ".downloading";
        await using (var fileStream = new FileStream(tempPath, FileMode.Create, FileAccess.Write, FileShare.None, 81920, true))
        {
            var buffer = new byte[81920];
            long totalRead = 0;
            int bytesRead;
            var lastReport = DateTime.UtcNow;

            while ((bytesRead = await contentStream.ReadAsync(buffer)) > 0)
            {
                await fileStream.WriteAsync(buffer.AsMemory(0, bytesRead));
                totalRead += bytesRead;

                if ((DateTime.UtcNow - lastReport).TotalSeconds >= 5)
                {
                    var pct = totalBytes.HasValue ? $" ({100.0 * totalRead / totalBytes.Value:F1}%)" : "";
                    Console.Write($"\r[Sphere] Downloaded {totalRead / (1024.0 * 1024.0):N0} MB{pct}");
                    lastReport = DateTime.UtcNow;
                }
            }
        }

        File.Move(tempPath, targetPath);
        Console.WriteLine($"\n[Sphere] Download complete: {targetPath}");
        return targetPath;
    }

    private static List<string> FindSphereFiles(string directory)
    {
        return Directory.GetFiles(directory, "*.jsonl.tar.gz", SearchOption.TopDirectoryOnly)
            .Concat(Directory.GetFiles(directory, "*.jsonl.gz", SearchOption.TopDirectoryOnly))
            .OrderBy(f => f, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    // --- Streaming pipeline ---

    /// <summary>
    /// Streams JSONL lines from a .jsonl.tar.gz or .jsonl.gz file.
    /// Zero intermediate files — decompresses in-memory.
    /// </summary>
    public static async IAsyncEnumerable<SphereJsonLine> StreamJsonLinesAsync(
        string filePath, [EnumeratorCancellation] CancellationToken ct = default)
    {
        await using var fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read,
            FileShare.Read, bufferSize: 81920, useAsync: true);
        await using var gzipStream = new GZipStream(fileStream, CompressionMode.Decompress);

        if (filePath.EndsWith(".tar.gz", StringComparison.OrdinalIgnoreCase))
        {
            await using var tarReader = new TarReader(gzipStream, leaveOpen: true);
            while (await tarReader.GetNextEntryAsync(copyData: false, ct) is { } entry)
            {
                if (entry.DataStream == null)
                    continue;
                if (entry.Name.EndsWith(".jsonl", StringComparison.OrdinalIgnoreCase) == false)
                    continue;

                using var reader = new StreamReader(entry.DataStream, leaveOpen: true);
                while (await reader.ReadLineAsync(ct) is { } line)
                {
                    if (string.IsNullOrWhiteSpace(line))
                        continue;

                    var parsed = JsonSerializer.Deserialize<SphereJsonLine>(line);
                    if (parsed != null && string.IsNullOrEmpty(parsed.Sha) == false && parsed.Vector.Length > 0)
                        yield return parsed;
                }
            }
        }
        else
        {
            // Plain .jsonl.gz
            using var reader = new StreamReader(gzipStream);
            while (await reader.ReadLineAsync(ct) is { } line)
            {
                if (string.IsNullOrWhiteSpace(line))
                    continue;

                var parsed = JsonSerializer.Deserialize<SphereJsonLine>(line);
                if (parsed != null && string.IsNullOrEmpty(parsed.Sha) == false && parsed.Vector.Length > 0)
                    yield return parsed;
            }
        }
    }

    // --- Vector index ---

    private static async Task CreateVectorIndexAsync(
        IDocumentStore store,
        VectorQuantization quantization,
        bool exactSearch,
        IndexingEngine searchEngine,
        int? numberOfEdges = null,
        int? numberOfCandidatesForIndexing = null)
    {
        var engineName = searchEngine == IndexingEngine.Lucene ? "Lucene" : "Corax";
        var engineSuffix = searchEngine == IndexingEngine.Lucene ? "-lucene" : "-corax";

        var indexName = quantization switch
        {
            VectorQuantization.Int8 => $"{CollectionName}/ByEmbeddingInt8{engineSuffix}",
            VectorQuantization.Binary => $"{CollectionName}/ByEmbeddingBinary{engineSuffix}",
            VectorQuantization.Int4 => $"{CollectionName}/ByEmbeddingInt4{engineSuffix}",
            VectorQuantization.Int3 => $"{CollectionName}/ByEmbeddingInt3{engineSuffix}",
            VectorQuantization.Int2 => $"{CollectionName}/ByEmbeddingInt2{engineSuffix}",
            _ => $"{CollectionName}/ByEmbedding{engineSuffix}"
        };

        Console.WriteLine($"[Sphere] Creating vector index '{indexName}' (quantization: {quantization}, exact: {exactSearch}, engine: {engineName})...");

        var (sourceType, destType) = quantization switch
        {
            VectorQuantization.Int8 => (VectorEmbeddingType.Single, VectorEmbeddingType.Int8),
            VectorQuantization.Binary => (VectorEmbeddingType.Single, VectorEmbeddingType.Binary),
            // Int2=4, Int3=5, Int4=6 in the turboquant VectorEmbeddingType enum — cast directly
            // since the NuGet client library doesn't define these values yet.
            VectorQuantization.Int4 => (VectorEmbeddingType.Single, (VectorEmbeddingType)6),
            VectorQuantization.Int3 => (VectorEmbeddingType.Single, (VectorEmbeddingType)5),
            VectorQuantization.Int2 => (VectorEmbeddingType.Single, (VectorEmbeddingType)4),
            _ => (VectorEmbeddingType.Single, VectorEmbeddingType.Single)
        };

        var index = new IndexDefinition
        {
            Name = indexName,
            Maps = new HashSet<string>
            {
                $"from p in docs.{CollectionName} let attachment = LoadAttachment(p, \"vector\") select new {{ Vector = CreateVector(attachment.GetContentAsStream()) }}"
            },
            Fields = new Dictionary<string, IndexFieldOptions>
            {
                {
                    "Vector",
                    new IndexFieldOptions
                    {
                        Vector = new VectorOptions
                        {
                            Dimensions = VectorDimensions,
                            SourceEmbeddingType = sourceType,
                            DestinationEmbeddingType = destType,
                            NumberOfEdges = numberOfEdges,
                            NumberOfCandidatesForIndexing = numberOfCandidatesForIndexing
                        }
                    }
                }
            },
            Configuration = new IndexConfiguration { { "Indexing.Static.SearchEngineType", engineName } }
        };

        await store.Maintenance.SendAsync(new PutIndexesOperation(index));
        Console.WriteLine($"[Sphere] Created index '{indexName}'");

        Console.WriteLine($"[Sphere] Waiting for index to become non-stale...");
        using var session = store.OpenAsyncSession();
        session.Advanced.MaxNumberOfRequestsPerSession = int.MaxValue;
        await session.Query<Passage>(indexName)
            .Customize(x => x.WaitForNonStaleResults(TimeSpan.MaxValue))
            .Take(0)
            .ToListAsync();

        Console.WriteLine($"[Sphere] Index '{indexName}' is ready");
    }

    // --- Checkpoint management ---

    private static async Task<ImportCheckpoint?> LoadCheckpointAsync(IDocumentStore store)
    {
        using var session = store.OpenAsyncSession();
        return await session.LoadAsync<ImportCheckpoint>(CheckpointDocId);
    }

    private static async Task StoreCheckpointAsync(IDocumentStore store, long linesImported, string? lastSha)
    {
        using var session = store.OpenAsyncSession();
        var checkpoint = await session.LoadAsync<ImportCheckpoint>(CheckpointDocId);
        if (checkpoint == null)
        {
            checkpoint = new ImportCheckpoint();
            await session.StoreAsync(checkpoint, CheckpointDocId);
        }
        checkpoint.LinesImported = linesImported;
        checkpoint.LastSha = lastSha;
        checkpoint.Timestamp = DateTimeOffset.UtcNow;
        await session.SaveChangesAsync();
    }

    private static async Task ClearCheckpointAsync(IDocumentStore store)
    {
        using var session = store.OpenAsyncSession();
        var checkpoint = await session.LoadAsync<ImportCheckpoint>(CheckpointDocId);
        if (checkpoint != null)
        {
            session.Delete(checkpoint);
            await session.SaveChangesAsync();
        }
    }

    // --- Helpers ---

    private SphereProfile ResolveProfile(string? profile)
    {
        var key = profile ?? _profile;
        if (Profiles.TryGetValue(key, out var p) == false)
            throw new ArgumentException($"Unknown SPHERE profile: '{key}'");
        return p;
    }

    public static SphereProfile GetProfile(string profile)
    {
        if (Profiles.TryGetValue(profile, out var p) == false)
            throw new ArgumentException($"Unknown SPHERE profile: '{profile}'");
        return p;
    }
}
