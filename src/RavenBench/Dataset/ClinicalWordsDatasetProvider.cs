using System.Text.RegularExpressions;
using Parquet;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using RavenBench.Core.Workload;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Operations.Indexes;
using RavenBench.Core;

namespace RavenBench.Dataset;

/// <summary>
/// Word embeddings provider for clinical vocabulary (Word2Vec 100D, 300D, 600D).
/// </summary>
public sealed class ClinicalWordsDatasetProvider
{
    /// <summary>
    /// Helper record for deserializing word documents during batch import.
    /// </summary>
    private record WordDocument(string Word, float[] Embedding, int Dimensions);

    /// <summary>
    /// Minimum document count to consider ClinicalWords dataset as fully imported.
    /// Clinical embeddings datasets contain 100K-200K words depending on version.
    /// </summary>
    public const int MinExpectedDocuments = 100_000;

    private readonly int _dimensions;
    private Dictionary<string, float[]>? _wordVectors;
    private readonly SemaphoreSlim _loadSemaphore = new(1, 1);

    private static readonly Dictionary<int, string> ParquetFiles = new()
    {
        { 100, "w2v_100d_oa_cr_embeddings.parquet" },
        { 300, "w2v_300d_oa_cr_embeddings.parquet" },
        { 600, "w2v_600d_oa_cr_embeddings.parquet" },
    };

    public ClinicalWordsDatasetProvider(int dimensions = 100)
    {
        if (ParquetFiles.ContainsKey(dimensions) == false)
            throw new ArgumentException($"Supported dimensions: 100, 300, 600. Got: {dimensions}");
        _dimensions = dimensions;
    }

    public int Dimensions => _dimensions;
    public static IReadOnlyCollection<int> AvailableDimensions => ParquetFiles.Keys;

    private string GetParquetPath()
    {
        // Search upwards from these starting directories
        var startDirs = new[]
        {
            AppContext.BaseDirectory,
            Directory.GetCurrentDirectory(),
        };

        foreach (var startDir in startDirs)
        {
            var dir = new DirectoryInfo(startDir);
            while (dir != null)
            {
                var datasetsPath = Path.Combine(dir.FullName, "datasets", ParquetFiles[_dimensions]);
                if (File.Exists(datasetsPath))
                    return datasetsPath;
                dir = dir.Parent;
            }
        }

        throw new FileNotFoundException(
            $"Word embeddings not found. Run:\n  python datasets/prepare_clinical_embeddings.py");
    }

    private async Task<Dictionary<string, float[]>> LoadWordVectorsAsync()
    {
        // We have already loaded and cached the vectors.
        if (_wordVectors != null) 
            return _wordVectors;

        // Use SemaphoreSlim for async locking instead of lock()
        await _loadSemaphore.WaitAsync();
        try
        {
            // Someone else did it before us while we were waiting.
            if (_wordVectors != null) 
                return _wordVectors;

            var path = GetParquetPath();
            Console.WriteLine($"[ClinicalWords] Loading {_dimensions}D vectors from {path}");

            _wordVectors = new Dictionary<string, float[]>(StringComparer.OrdinalIgnoreCase);

            using var stream = File.OpenRead(path);
            using var reader = await ParquetReader.CreateAsync(stream);
            var fields = reader.Schema.GetDataFields();

            for (int rg = 0; rg < reader.RowGroupCount; rg++)
            {
                using var groupReader = reader.OpenRowGroupReader(rg);
                var wordCol = await groupReader.ReadColumnAsync(fields[0]);
                var vecCol = await groupReader.ReadColumnAsync(fields[1]);

                var words = (string[])wordCol.Data;

                // Parquet.Net returns list columns as flat double?[] - slice into chunks of _dimensions
                if (vecCol.Data is double?[] flatVectors)
                {
                    int vecSize = flatVectors.Length / words.Length;
                    for (int i = 0; i < words.Length; i++)
                    {
                        var vec = new float[vecSize];
                        int offset = i * vecSize;
                        for (int j = 0; j < vecSize; j++)
                            vec[j] = (float)(flatVectors[offset + j] ?? 0.0);
                        _wordVectors[words[i]] = vec;
                    }
                }
                else if (vecCol.Data is double[] flatDoubles)
                {
                    int vecSize = flatDoubles.Length / words.Length;
                    for (int i = 0; i < words.Length; i++)
                    {
                        var vec = new float[vecSize];
                        int offset = i * vecSize;
                        for (int j = 0; j < vecSize; j++)
                            vec[j] = (float)flatDoubles[offset + j];
                        _wordVectors[words[i]] = vec;
                    }
                }
                else
                    throw new InvalidOperationException($"Unknown vector format: {vecCol.Data?.GetType()}");
            }

            Console.WriteLine($"[ClinicalWords] Loaded {_wordVectors.Count:N0} words");
            return _wordVectors;
        }
        finally
        {
            _loadSemaphore.Release();
        }
    }

    public async Task<float[]?> GetWordVectorAsync(string word)
    {
        var vectors = await LoadWordVectorsAsync();
        return vectors.TryGetValue(word, out var v) ? v : null;
    }

    public async Task<float[]> ComputeDocumentEmbeddingAsync(string text)
    {
        var w2v = await LoadWordVectorsAsync();
        var result = new float[_dimensions];
        int count = 0;

        foreach (var word in Tokenize(text))
        {
            if (w2v.TryGetValue(word, out var vec))
            {
                for (int i = 0; i < result.Length; i++)
                    result[i] += vec[i];
                count++;
            }
        }

        if (count > 0)
            for (int i = 0; i < result.Length; i++)
                result[i] /= count;

        return result;
    }

    private static string[] Tokenize(string text) =>
        Regex.Split(text.ToLowerInvariant(), @"[^a-z0-9_]+").Where(t => t.Length > 1).ToArray();

    public async Task<VectorWorkloadMetadata> GenerateQueryVectorsAsync(int count = 100)
    {
        var w2v = await LoadWordVectorsAsync();
        var words = w2v.Keys.ToArray();
        var rng = new Random(42);
        var vectors = Enumerable.Range(0, Math.Min(count, words.Length))
            .Select(_ => w2v[words[rng.Next(words.Length)]])
            .ToArray();

        return new VectorWorkloadMetadata
        {
            QueryVectors = vectors,
            FieldName = "Embedding",
            VectorDimensions = _dimensions,
            BaseVectorCount = w2v.Count
        };
    }

    public string GetDatabaseName(string? profile = null, int? customSize = null) => $"ClinicalWords{_dimensions}D";

    public async Task<bool> IsDatasetImportedAsync(string serverUrl, string databaseName, int expectedMinDocuments = 1000, Version? httpVersion = null)
    {
        // Check if parquet file exists locally
        try { GetParquetPath(); }
        catch { return false; }

        return await DatasetImportCheck.RunAsync(serverUrl, databaseName, httpVersion, "[ClinicalWords]", "WordDocuments", expectedMinDocuments);
    }

    /// <summary>
    /// Imports all words with their embeddings as documents into RavenDB.
    /// Each word becomes a document: {{ "Word": "patient", "Embedding": [0.1, 0.2, ...] }}
    /// </summary>
    public async Task ImportWordsAsync(
        string serverUrl,
        string databaseName,
        VectorQuantization quantization = VectorQuantization.None,
        bool exactSearch = false,
        int batchSize = 1000,
        Version? httpVersion = null,
        IndexingEngine searchEngine = IndexingEngine.Corax)
    {
        using var store = HttpHelper.Create(serverUrl, databaseName, httpVersion);

        // Create database if needed (use default Corax, search engine is set per-index)
        var dbRecord = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
        if (dbRecord == null)
        {
            Console.WriteLine($"[ClinicalWords] Creating database: {databaseName}");
            await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(databaseName)));
        }

        // Determine search engine name for per-index configuration
        var engineName = searchEngine == IndexingEngine.Lucene ? "Lucene" : "Corax";

        // Check if documents already exist
        var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
        bool documentsExist = stats.CountOfDocuments >= MinExpectedDocuments;

        if (documentsExist == false)
        {
            // Import documents
            var words = await LoadWordVectorsAsync();
            Console.WriteLine($"[ClinicalWords] Importing {words.Count:N0} words to {databaseName}...");

            int imported = 0;
            using (var bulkInsert = store.BulkInsert())
            {
                foreach (var (word, vector) in words)
                {
                    // Use the WordDocument record type to ensure proper @metadata
                    var doc = new WordDocument(word, vector, _dimensions);

                    // Sanitize document ID: RavenDB doesn't allow IDs ending with '|' or containing certain chars
                    var docId = $"Words/{SanitizeDocumentId(word)}";

                    await bulkInsert.StoreAsync(doc, docId);
                    imported++;
                    if (imported % batchSize == 0)
                    {
                        Console.Write($"\r[ClinicalWords] Imported {imported:N0}/{words.Count:N0} words...");
                    }
                }
            }

            Console.WriteLine($"\n[ClinicalWords] Import complete: {imported:N0} documents");
        }
        else
        {
            Console.WriteLine($"[ClinicalWords] Database already contains {stats.CountOfDocuments:N0} documents - skipping import");
        }

        var engineSuffix = VectorIndexMapping.GetEngineSuffix(searchEngine);
        var indexName = VectorIndexNaming.GetIndexName("Words", quantization, engineSuffix);

        Console.WriteLine($"[ClinicalWords] Creating vector index '{indexName}' (quantization: {quantization}, exact: {exactSearch}, engine: {engineName})...");

        var (sourceType, destType) = VectorIndexMapping.GetEmbeddingTypes(quantization);

        var index = new IndexDefinition
        {
            Name = indexName,
            Maps = new HashSet<string> { "from w in docs.WordDocuments select new { w.Word, Vector = CreateVector(w.Embedding) }" },
            Fields = new Dictionary<string, IndexFieldOptions>
            {
                {
                    "Vector",
                    new IndexFieldOptions
                    {
                        Vector = new VectorOptions
                        {
                            Dimensions = _dimensions,
                            SourceEmbeddingType = sourceType,
                            DestinationEmbeddingType = destType
                        }
                    }
                }
            },
            Configuration = new IndexConfiguration { { "Indexing.Static.SearchEngineType", engineName } }
        };

        await VectorIndexHelper.CreateAndWaitForIndexAsync(store, index, "[ClinicalWords]");

        // Sanity check: verify we can query the index
        Console.WriteLine($"[ClinicalWords] Running sanity check...");
        using var session = store.OpenAsyncSession();
        var sampleResults = await session.Query<WordDocument>(indexName)
            .Take(5)
            .ToListAsync();

        if (sampleResults.Count == 0)
        {
            throw new InvalidOperationException($"Sanity check failed: Index '{indexName}' returned no results");
        }

        Console.WriteLine($"[ClinicalWords] Sanity check passed: Retrieved {sampleResults.Count} sample documents");
        foreach (var result in sampleResults)
        {
            Console.WriteLine($"  - {result.Word} ({result.Embedding.Length}D)");
        }
    }


    /// <summary>
    /// Sanitizes a word for use as a RavenDB document ID suffix.
    /// RavenDB doesn't allow document IDs ending with '|' and has restrictions on certain characters.
    /// </summary>
    private static string SanitizeDocumentId(string word)
    {
        if (string.IsNullOrEmpty(word))
            return "empty";

        // RavenDB document ID restrictions: cannot end with '|', and other special chars may cause issues
        var sanitized = word
            .Replace("|", "_pipe_")
            .Replace("\\", "_backslash_")
            .Replace("/", "_slash_")
            .Replace("?", "_question_")
            .Replace("<", "_lt_")
            .Replace(">", "_gt_")
            .Replace("\"", "_quote_")
            .Replace(":", "_colon_")
            .Replace("*", "_star_");

        // Ensure it doesn't end with pipe (shouldn't happen after replacement, but be safe)
        sanitized = sanitized.TrimEnd('|');

        return string.IsNullOrEmpty(sanitized) ? "special" : sanitized;
    }
}
