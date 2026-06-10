namespace RavenBench.Core.Workload;

/// <summary>
/// Metadata for vector search workloads, containing the query vectors and configuration.
/// </summary>
public sealed class VectorWorkloadMetadata
{
    /// <summary>
    /// The field name in documents containing the indexed vectors.
    /// </summary>
    public required string FieldName { get; init; }

    /// <summary>
    /// Array of query vectors to be randomly selected during benchmarking.
    /// Each vector should match the dimensionality of the indexed vectors.
    /// </summary>
    public required float[][] QueryVectors { get; init; }

    /// <summary>
    /// Dimensionality of the vectors (e.g., 128 for SIFT, 1536 for OpenAI ada-002).
    /// </summary>
    public int VectorDimensions { get; init; }

    /// <summary>
    /// Number of base vectors in the dataset (for reporting).
    /// </summary>
    public long BaseVectorCount { get; init; }

    /// <summary>
    /// Number of query vectors available.
    /// </summary>
    public int QueryVectorCount => QueryVectors.Length;

    /// <summary>
    /// Optional ground truth data for recall@K calculation.
    /// Dictionary mapping query index to ordered list of nearest neighbor document IDs.
    /// Computed via exact (brute-force) search and cached in the target database.
    /// </summary>
    public Dictionary<int, string[]>? GroundTruth { get; init; }

    /// <summary>
    /// The field name as it appears in the index (e.g., "Vector" when the index map is "Vector = CreateVector(p.Embedding)").
    /// Defaults to FieldName if not set. Used for building vector.search() queries.
    /// </summary>
    public string? IndexedFieldName { get; set; }

    /// <summary>
    /// The index name used for vector search queries (e.g., "SpherePassages/ByEmbedding-corax").
    /// Set by the dataset provider that created the index.
    /// </summary>
    public string? IndexName { get; set; }

    /// <summary>
    /// The collection name containing the vector documents (e.g., "SpherePassages", "Words").
    /// Used for building recall measurement queries.
    /// </summary>
    public string? CollectionName { get; set; }

    /// <summary>
    /// Optional callback to ensure the required vector index exists before querying.
    /// Called by RecallMeasurement before running ground truth or approximate queries.
    /// The callback receives (IDocumentStore, indexName) and should create the index if missing.
    /// </summary>
    public Func<object, string, Task>? EnsureIndexExists { get; set; }
}
