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
using RavenBench.Dataset.Vectors;

namespace RavenBench.Dataset;

/// <summary>
/// Word embeddings provider for clinical vocabulary (Word2Vec 100D, 300D, 600D). As a vector set it ships
/// no query split: the scenario seed holds query words out of the load and the truth is brute force.
/// </summary>
public sealed class ClinicalWordsDatasetProvider : HeldOutVectorDataset
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
    private readonly DatasetFile _file;
    private string? _loadedPath;
    private Dictionary<string, float[]>? _wordVectors;
    private readonly SemaphoreSlim _loadSemaphore = new(1, 1);

    private static readonly Dictionary<int, string> ParquetFiles = new()
    {
        { 100, "w2v_100d_oa_cr_embeddings.parquet" },
        { 300, "w2v_300d_oa_cr_embeddings.parquet" },
        { 600, "w2v_600d_oa_cr_embeddings.parquet" },
    };

    public const string DocumentIdPrefix = "Words/";

    public ClinicalWordsDatasetProvider(int dimensions = 100)
    {
        if (ParquetFiles.ContainsKey(dimensions) == false)
            throw new ArgumentException($"Supported dimensions: 100, 300, 600. Got: {dimensions}");
        _dimensions = dimensions;
        // The parquet derives locally from an upstream archive that no longer serves, so the catalog carries
        // neither a URL nor a pin; the operator pins the file they produced.
        _file = new DatasetFile
        {
            FileName = ParquetFiles[dimensions],
            Url = "",
            Type = "vectors",
            EstimatedSizeBytes = 0,
            Description = $"Produce it with: python datasets/prepare_clinical_embeddings.py --model w2v_{dimensions}d_oa_cr"
        };
    }

    public override string Name => $"clinical-words-{_dimensions}";
    public override VectorMetric Metric => VectorMetric.Cosine;
    public override int Dimensions => _dimensions;
    public override IReadOnlyList<DatasetFile> Files => [_file];

    protected override async Task<long> CountRowsAsync(VerifiedFiles files, CancellationToken ct) =>
        (await LoadWordVectorsAsync(files.PathOf(_file.FileName))).Count;

    protected override async IAsyncEnumerable<BaseVector> ReadRowsAsync(VerifiedFiles files, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        var ids = new HashSet<string>(StringComparer.Ordinal);
        foreach (var (word, vector) in await LoadWordVectorsAsync(files.PathOf(_file.FileName)))
        {
            var id = SanitizeDocumentId(word);
            if (ids.Add(id) == false)
                throw new InvalidDataException($"Set '{Name}' maps two words to the document id '{DocumentIdPrefix}{id}'.");
            yield return new BaseVector(id, vector);
        }
    }
    public static IReadOnlyCollection<int> AvailableDimensions => ParquetFiles.Keys;

    // Enumeration order is the parquet row order: the dictionary sees no removals.
    private async Task<Dictionary<string, float[]>> LoadWordVectorsAsync(string path)
    {
        if (_wordVectors != null && _loadedPath == path)
            return _wordVectors;

        await _loadSemaphore.WaitAsync();
        try
        {
            if (_wordVectors != null && _loadedPath == path)
                return _wordVectors;

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
            _loadedPath = path;
            return _wordVectors;
        }
        finally
        {
            _loadSemaphore.Release();
        }
    }

    public async Task<float[]?> GetWordVectorAsync(VerifiedFiles files, string word)
    {
        var vectors = await LoadWordVectorsAsync(files.PathOf(_file.FileName));
        return vectors.TryGetValue(word, out var v) ? v : null;
    }

    public async Task<float[]> ComputeDocumentEmbeddingAsync(VerifiedFiles files, string text)
    {
        var w2v = await LoadWordVectorsAsync(files.PathOf(_file.FileName));
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

    /// <summary>
    /// The seeded query selection, held out of the load, with its brute-force truth at depth k.
    /// </summary>
    public async Task<VectorWorkloadMetadata> GenerateQueryVectorsAsync(VerifiedFiles files, QuerySelection selection, int k) =>
        await VectorSets.BuildMetadataAsync(this, files, selection, k, fieldName: "Embedding", DocumentIdPrefix, await BaseCountAsync(files, selection));

    public string GetDatabaseName(string? profile = null, int? customSize = null) => $"ClinicalWords{_dimensions}D";

    /// <summary>
    /// Imports all words with their embeddings as documents into RavenDB.
    /// Each word becomes a document: {{ "Word": "patient", "Embedding": [0.1, 0.2, ...] }}
    /// </summary>
    /// <returns>True when documents were loaded; false when the database already held them.</returns>
    public async Task<bool> ImportWordsAsync(
        string serverUrl,
        string databaseName,
        VerifiedFiles files,
        QuerySelection selection,
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

        // The expected count includes the held-out query record itself.
        var documentsExist = await HeldOutManifest.EnsureMatchesAsync(store, files, selection, await CountRowsAsync(files, default) - selection.Count + 1);

        if (documentsExist == false)
        {
            var words = await LoadWordVectorsAsync(files.PathOf(_file.FileName));
            var heldOut = await HeldOutPositionsAsync(files, selection);
            Console.WriteLine($"[ClinicalWords] Importing {words.Count - heldOut.Count:N0} words to {databaseName} ({heldOut.Count} held out as queries)...");

            await HeldOutManifest.StoreAsync(store, files, selection);
            int imported = 0;
            long position = 0;
            using (var bulkInsert = store.BulkInsert())
            {
                foreach (var (word, vector) in words)
                {
                    if (heldOut.Contains(position++))
                        continue;

                    var doc = new WordDocument(word, vector, _dimensions);
                    await bulkInsert.StoreAsync(doc, DocumentIdPrefix + SanitizeDocumentId(word));
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
            Console.WriteLine($"[ClinicalWords] Database already holds the selection's documents - skipping import");
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

        return documentsExist == false;
    }


    /// <summary>
    /// Sanitizes a word for use as a RavenDB document ID suffix.
    /// RavenDB doesn't allow document IDs ending with '|' and has restrictions on certain characters.
    /// </summary>
    internal static string SanitizeDocumentId(string word)
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
