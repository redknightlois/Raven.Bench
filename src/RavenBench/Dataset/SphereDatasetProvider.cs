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
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using RavenBench.Core;
using RavenBench.Core.Workload;
using RavenBench.Dataset.Vectors;

namespace RavenBench.Dataset;

/// <summary>
/// SPHERE dataset provider — streaming import of Meta's SPHERE passages with pre-computed DPR embeddings.
/// Supports profiles from 100K to 899M passages. Streams .jsonl.tar.gz files directly into RavenDB
/// bulk insert with no intermediate decompressed files on disk. As a vector set it ships no query split: the
/// scenario seed holds query passages out of the load and the truth is brute force.
/// </summary>
public sealed class SphereDatasetProvider : HeldOutVectorDataset
{
    public const int VectorDimensions = 768; // facebook-dpr-ctx_encoder-single-nq-base
    public const string CollectionName = "Passages";
    private const string CheckpointDocId = "sphere/import-checkpoint";
    private const int ProgressInterval = 10_000;
    private const int CheckpointInterval = 100_000;

    private readonly string _profile;
    private readonly DatasetFile? _file;
    private readonly long? _rowCount;

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

    public const string DocumentIdPrefix = CollectionName + "/";

    // DPR embeddings; the harness has always indexed them for cosine similarity, so the set is defined under cosine.
    public override string Name => $"sphere-{_profile.ToLowerInvariant()}";
    public override VectorMetric Metric => VectorMetric.Cosine;
    public override int Dimensions => VectorDimensions;
    public override IReadOnlyList<DatasetFile> Files => [_file ?? PinnedFile(_profile)];

    private static DatasetFile PinnedFile(string profile) =>
        new DatasetFile
        {
            FileName = ProfileFileNames[profile],
            Url = $"https://storage.googleapis.com/sphere-demo/{ProfileFileNames[profile]}",
            Type = "vectors",
            EstimatedSizeBytes = 0,
            Sha256 = ProfileSha256.GetValueOrDefault(profile)
        };

    // Profiles without a pin fail by set name before any use.
    private static readonly Dictionary<string, string> ProfileSha256 = new(StringComparer.OrdinalIgnoreCase)
    {
        { "100k", "42460da4d032393aeba587b0988a1e806aef0b240076267a2738ec57967811b1" },
        { "1m", "4be688444fdf63824d337287b5daa108a1b7f8dce2ab52cd4210847d87a7fbfe" },
    };

    protected override Task<long> CountRowsAsync(VerifiedFiles files, CancellationToken ct) =>
        Task.FromResult(TargetDocCount);

    private long TargetDocCount => _rowCount ?? ResolveProfile(null).TargetDocCount;

    protected override async IAsyncEnumerable<BaseVector> ReadRowsAsync(VerifiedFiles files, [EnumeratorCancellation] CancellationToken ct)
    {
        await foreach (var line in StreamJsonLinesAsync(files.PathOf(Files[0].FileName), ct))
            yield return new BaseVector(BaseId(line), line.Vector);
    }

    private static string BaseId(SphereJsonLine line) => string.IsNullOrEmpty(line.Id) ? line.Sha : line.Id;

    public SphereDatasetProvider(string profile = "100k")
    {
        if (Profiles.ContainsKey(profile) == false)
        {
            var valid = string.Join(", ", Profiles.Keys);
            throw new ArgumentException($"Unknown SPHERE profile: '{profile}'. Valid profiles: {valid}");
        }
        _profile = profile;
    }

    /// <summary>
    /// A profile served from a caller-pinned file of <paramref name="rowCount"/> passages.
    /// </summary>
    internal SphereDatasetProvider(string profile, DatasetFile file, long rowCount) : this(profile)
    {
        _file = file;
        _rowCount = rowCount;
    }

    public string Profile => _profile;

    public static IReadOnlyCollection<string> AvailableProfiles => Profiles.Keys;

    public string GetDatabaseName(string? profile = null, int? customSize = null)
    {
        return ResolveProfile(profile).DatabaseName;
    }

    /// <summary>
    /// Streams a .jsonl.tar.gz source into RavenDB via bulk insert. Supports resume and measures
    /// import time and indexing time separately.
    /// </summary>
    public async Task<ImportResult> ImportAsync(
        string serverUrl,
        string databaseName,
        VerifiedFiles files,
        QuerySelection selection,
        VectorQuantization quantization = VectorQuantization.None,
        bool exactSearch = false,
        Version? httpVersion = null,
        IndexingEngine searchEngine = IndexingEngine.Corax,
        int? numberOfEdges = null,
        int? numberOfCandidatesForIndexing = null,
        CancellationToken ct = default)
    {
        using var store = HttpHelper.Create(serverUrl, databaseName, httpVersion);

        var dbRecord = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
        if (dbRecord == null)
        {
            Console.WriteLine($"[Sphere] Creating database: {databaseName}");
            await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(databaseName)));
        }


        // The expected count includes the held-out query record itself.
        var documentsExist = await HeldOutManifest.EnsureMatchesAsync(store, files, selection, TargetDocCount - selection.Count + 1);

        var importSw = Stopwatch.StartNew();
        long totalImported = 0;

        if (documentsExist == false)
        {
            var heldOut = await HeldOutPositionsAsync(files, selection, ct);
            var checkpoint = await LoadCheckpointAsync(store);
            long skipLines = checkpoint?.LinesImported ?? 0;

            if (skipLines > 0)
                Console.WriteLine($"[Sphere] Resuming from line {skipLines:N0} (checkpoint: {checkpoint!.LastSha})");
            else
                await HeldOutManifest.StoreAsync(store, files, selection);

            var file = files.PathOf(Files[0].FileName);
            Console.WriteLine($"[Sphere] Importing {Path.GetFileName(file)} into {databaseName} (target: {TargetDocCount - heldOut.Count:N0} docs, {heldOut.Count} held out as queries)");

            long position = 0;
            var rateSw = Stopwatch.StartNew();
            string? lastSha = null;

            using (var bulkInsert = store.BulkInsert())
            {
                await foreach (var line in StreamJsonLinesAsync(file, ct))
                {
                    if (position >= TargetDocCount)
                        break;
                    var linePosition = position++;
                    if (linePosition < skipLines || heldOut.Contains(linePosition))
                        continue;

                    var doc = new Passage(line.Raw, line.Sha, line.Title, line.Url);
                    var docId = DocumentIdPrefix + BaseId(line);
                    await bulkInsert.StoreAsync(doc, docId);

                    // Store vector as binary attachment (768D × 4 bytes = 3072 bytes)
                    var vectorBytes = new byte[line.Vector.Length * sizeof(float)];
                    Buffer.BlockCopy(line.Vector, 0, vectorBytes, 0, vectorBytes.Length);
                    using var vectorStream = new MemoryStream(vectorBytes);
                    bulkInsert.AttachmentsFor(docId).Store("vector", vectorStream);
                    totalImported++;
                    lastSha = line.Sha;

                    if (totalImported % ProgressInterval == 0)
                    {
                        var docsPerSec = totalImported / rateSw.Elapsed.TotalSeconds;
                        var pct = (double)position / TargetDocCount * 100;
                        Console.Write($"\r[Sphere] Imported {totalImported:N0} ({pct:F1}%, {docsPerSec:N0} docs/sec)");
                    }

                    if (totalImported % CheckpointInterval == 0)
                        await StoreCheckpointAsync(store, position, lastSha);
                }
            }

            Console.WriteLine($"\n[Sphere] Import complete: {totalImported:N0} documents in {importSw.Elapsed}");

            if (position < TargetDocCount)
                throw new InvalidDataException($"Set '{Name}' holds {position:N0} passages; the profile needs {TargetDocCount:N0}.");
            await ClearCheckpointAsync(store);
        }
        else
        {
            Console.WriteLine($"[Sphere] Database already holds the selection's documents - skipping import");
        }

        importSw.Stop();
        var importDuration = importSw.Elapsed;

        var indexingSw = Stopwatch.StartNew();
        await CreateVectorIndexAsync(store, quantization, exactSearch, searchEngine, numberOfEdges, numberOfCandidatesForIndexing);
        indexingSw.Stop();
        var indexingDuration = indexingSw.Elapsed;

        Console.WriteLine($"[Sphere] Import: {importDuration}, Indexing: {indexingDuration}");

        return new ImportResult(totalImported, importDuration, indexingDuration);
    }

    /// <summary>
    /// The seeded query selection, held out of the load, with its brute-force truth at depth k.
    /// </summary>
    public async Task<VectorWorkloadMetadata> GenerateQueryVectorsAsync(VerifiedFiles files, QuerySelection selection, int k)
    {
        var metadata = await VectorSets.BuildMetadataAsync(this, files, selection, k, fieldName: "Embedding", DocumentIdPrefix);
        metadata.CollectionName = CollectionName;
        return metadata;
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
        await using var gzipStream = new GZipStream(fileStream, System.IO.Compression.CompressionMode.Decompress);

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

    public static async Task CreateVectorIndexAsync(
        IDocumentStore store,
        VectorQuantization quantization,
        bool exactSearch,
        IndexingEngine searchEngine,
        int? numberOfEdges = null,
        int? numberOfCandidatesForIndexing = null)
    {
        var engineName = searchEngine == IndexingEngine.Lucene ? "Lucene" : "Corax";
        var engineSuffix = VectorIndexMapping.GetEngineSuffix(searchEngine);

        var indexName = VectorIndexNaming.GetIndexName(CollectionName, quantization, engineSuffix, numberOfEdges, numberOfCandidatesForIndexing);

        Console.WriteLine($"[Sphere] Creating vector index '{indexName}' (quantization: {quantization}, exact: {exactSearch}, engine: {engineName})...");

        var (sourceType, destType) = VectorIndexMapping.GetEmbeddingTypes(quantization);

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

        await VectorIndexHelper.CreateAndWaitForIndexAsync(store, index, "[Sphere]");
        Console.WriteLine($"[Sphere] Index '{index.Name}' is ready");
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
