using System.Text.RegularExpressions;
using Parquet;
using Parquet.Serialization;
using RavenBench.Core.Workload;

namespace RavenBench.Dataset;

/// <summary>
/// Word embeddings provider for clinical vocabulary (Word2Vec 100D, 300D, 600D).
/// </summary>
public sealed class ClinicalWordsDatasetProvider : IDatasetProvider
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
        if (!ParquetFiles.ContainsKey(dimensions))
            throw new ArgumentException($"Supported dimensions: 100, 300, 600. Got: {dimensions}");
        _dimensions = dimensions;
    }

    public string DatasetName => $"clinicalwords{_dimensions}d";
    public int Dimensions => _dimensions;
    public static IReadOnlyCollection<int> AvailableDimensions => ParquetFiles.Keys;

    private string GetParquetPath()
    {
        var candidates = new[]
        {
            Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", "..", "datasets"),
            Path.Combine(AppContext.BaseDirectory, "datasets"),
            Path.Combine(Directory.GetCurrentDirectory(), "datasets"),
        };

        foreach (var dir in candidates)
        {
            var path = Path.GetFullPath(Path.Combine(dir, ParquetFiles[_dimensions]));
            if (File.Exists(path)) return path;
        }

        throw new FileNotFoundException(
            $"Word embeddings not found. Run:\n  python datasets/prepare_clinical_embeddings.py");
    }

    private async Task<Dictionary<string, float[]>> LoadWordVectorsAsync()
    {
        if (_wordVectors != null) return _wordVectors;

        // Use SemaphoreSlim for async locking instead of lock()
        await _loadSemaphore.WaitAsync();
        try
        {
            if (_wordVectors != null) return _wordVectors;

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

    // IDatasetProvider implementation
    public DatasetInfo GetDatasetInfo(string? profile = null, int? customSize = null) => new()
    {
        Name = $"ClinicalWords{_dimensions}D",
        Description = $"Clinical Word2Vec {_dimensions}D embeddings",
        MaxQuestionId = 0, MaxUserId = 0, Files = new()
    };

    public string GetDatabaseName(string? profile = null, int? customSize = null) => $"ClinicalWords{_dimensions}D";

    public async Task<bool> IsDatasetImportedAsync(string serverUrl, string databaseName, int expectedMinDocuments = 1000)
    {
        // Check if parquet file exists locally
        try { GetParquetPath(); }
        catch { return false; }

        // Check if database has documents and index is ready
        using var client = new HttpClient();
        try
        {
            var url = $"{serverUrl}/databases/{databaseName}/stats";
            var response = await client.GetAsync(url);
            if (!response.IsSuccessStatusCode)
            {
                Console.WriteLine($"[ClinicalWords] Skip check: database stats returned {response.StatusCode}");
                return false;
            }

            var json = await response.Content.ReadAsStringAsync();

            // Check document count
            var countMatch = Regex.Match(json, @"""CountOfDocuments""\s*:\s*(\d+)");
            if (!countMatch.Success || !int.TryParse(countMatch.Groups[1].Value, out int docCount))
            {
                Console.WriteLine($"[ClinicalWords] Skip check: could not parse CountOfDocuments");
                return false;
            }
            if (docCount < expectedMinDocuments)
            {
                Console.WriteLine($"[ClinicalWords] Skip check: only {docCount} docs, need {expectedMinDocuments}");
                return false;
            }

            // Check if index exists and is non-stale
            if (!json.Contains(@"""IsStale"":false"))
            {
                Console.WriteLine($"[ClinicalWords] Skip check: no non-stale index found");
                return false;
            }

            Console.WriteLine($"[ClinicalWords] Database has {docCount:N0} documents, index ready - skipping import");
            return true;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[ClinicalWords] Skip check failed: {ex.Message}");
        }
        return false;
    }

    /// <summary>
    /// Imports all words with their embeddings as documents into RavenDB.
    /// Each word becomes a document: { "Word": "patient", "Embedding": [0.1, 0.2, ...] }
    /// </summary>
    public async Task ImportWordsAsync(string serverUrl, string databaseName, int batchSize = 1000)
    {
        var words = await LoadWordVectorsAsync();
        Console.WriteLine($"[ClinicalWords] Importing {words.Count:N0} words to {databaseName}...");

        using var client = new HttpClient { BaseAddress = new Uri(serverUrl) };

        // Create database if needed
        await EnsureDatabaseExistsAsync(client, databaseName);

        // Import in batches
        var batch = new List<object>();
        int imported = 0;

        foreach (var (word, vector) in words)
        {
            batch.Add(new
            {
                Word = word,
                Embedding = vector,
                Dimensions = _dimensions
            });

            if (batch.Count >= batchSize)
            {
                await SendBatchAsync(client, databaseName, batch);
                imported += batch.Count;
                Console.Write($"\r[ClinicalWords] Imported {imported:N0}/{words.Count:N0} words...");
                batch.Clear();
            }
        }

        // Final batch
        if (batch.Count > 0)
        {
            await SendBatchAsync(client, databaseName, batch);
            imported += batch.Count;
        }

        Console.WriteLine($"\n[ClinicalWords] Import complete: {imported:N0} documents");

        // Create vector index
        await CreateVectorIndexAsync(client, databaseName);
    }

    private async Task EnsureDatabaseExistsAsync(HttpClient client, string databaseName)
    {
        var response = await client.GetAsync($"/databases/{databaseName}/stats");
        if (response.IsSuccessStatusCode) return;

        // Create database
        var createRequest = new
        {
            DatabaseName = databaseName,
            Settings = new Dictionary<string, string>()
        };
        var json = System.Text.Json.JsonSerializer.Serialize(createRequest);
        var content = new StringContent(json, System.Text.Encoding.UTF8, "application/json");
        await client.PutAsync($"/admin/databases?name={databaseName}", content);
        Console.WriteLine($"[ClinicalWords] Created database: {databaseName}");
    }

    private async Task SendBatchAsync(HttpClient client, string databaseName, List<object> documents)
    {
        // Use bulk insert format
        var commands = new List<object>();
        foreach (var doc in documents)
        {
            // Safely deserialize to extract the Word field without using dynamic
            var json = System.Text.Json.JsonSerializer.Serialize(doc);
            var wordDoc = System.Text.Json.JsonSerializer.Deserialize<WordDocument>(json)!;

            commands.Add(new
            {
                Id = $"words/{wordDoc.Word}",
                Type = "PUT",
                Document = doc,
                ChangeVector = (string?)null
            });
        }

        var request = new { Commands = commands };
        var requestJson = System.Text.Json.JsonSerializer.Serialize(request);
        var content = new StringContent(requestJson, System.Text.Encoding.UTF8, "application/json");

        var response = await client.PostAsync($"/databases/{databaseName}/bulk_docs", content);
        response.EnsureSuccessStatusCode();
    }

    private async Task CreateVectorIndexAsync(HttpClient client, string databaseName)
    {
        Console.WriteLine($"[ClinicalWords] Creating vector index...");

        var indexDef = new
        {
            Name = "Words/ByEmbedding",
            Maps = new[] { "from w in docs.Words select new { w.Word, Vector = CreateVector(\"Embedding\", w.Embedding) }" },
            Configuration = new Dictionary<string, string>
            {
                ["Indexing.Static.Vectors.Encoder"] = "Hnsw"
            }
        };

        var json = System.Text.Json.JsonSerializer.Serialize(indexDef);
        var content = new StringContent(json, System.Text.Encoding.UTF8, "application/json");

        var response = await client.PutAsync($"/databases/{databaseName}/admin/indexes", content);
        if (response.IsSuccessStatusCode)
            Console.WriteLine($"[ClinicalWords] Vector index created: Words/ByEmbedding");
    }

    public static async Task<VectorWorkloadMetadata> LoadQueryVectorsAsync(string queryFilePath)
    {
        var json = await File.ReadAllTextAsync(queryFilePath);
        var queries = System.Text.Json.JsonSerializer.Deserialize<List<float[]>>(json)!;
        return new VectorWorkloadMetadata { QueryVectors = queries.ToArray(), FieldName = "Embedding" };
    }
}
