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
    /// Optional ground truth data for recall@k calculation.
    /// Dictionary mapping query index to list of nearest neighbor IDs.
    /// </summary>
    public Dictionary<int, int[]>? GroundTruth { get; init; }
}
