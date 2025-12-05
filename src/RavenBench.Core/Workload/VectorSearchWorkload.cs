namespace RavenBench.Core.Workload;

/// <summary>
/// Workload that generates vector search operations for benchmarking RavenDB's vector search capabilities.
/// Supports both approximate (HNSW) and exact search, with optional quantization.
/// </summary>
public sealed class VectorSearchWorkload : IWorkload
{
    private readonly VectorWorkloadMetadata _metadata;
    private readonly int _topK;
    private readonly float _minimumSimilarity;
    private readonly bool _useExactSearch;
    private readonly VectorQuantization _quantization;

    /// <summary>
    /// Creates a vector search workload for benchmarking RavenDB vector search performance.
    /// </summary>
    /// <param name="metadata">Metadata containing query vectors and field configuration</param>
    /// <param name="topK">Number of nearest neighbors to retrieve</param>
    /// <param name="minimumSimilarity">Minimum similarity threshold filter</param>
    /// <param name="useExactSearch">Use exact search instead of approximate HNSW</param>
    /// <param name="quantization">Vector quantization type</param>
    public VectorSearchWorkload(
        VectorWorkloadMetadata metadata,
        int topK = 10,
        float minimumSimilarity = 0.0f,
        bool useExactSearch = false,
        VectorQuantization quantization = VectorQuantization.None)
    {
        _metadata = metadata ?? throw new ArgumentNullException(nameof(metadata));

        if (metadata.QueryVectors.Length == 0)
            throw new ArgumentException("Metadata must contain at least one query vector", nameof(metadata));

        if (topK <= 0)
            throw new ArgumentOutOfRangeException(nameof(topK), topK, "TopK must be positive");

        if (minimumSimilarity < 0.0f || minimumSimilarity > 1.0f)
            throw new ArgumentOutOfRangeException(nameof(minimumSimilarity), minimumSimilarity,
                "Minimum similarity must be between 0.0 and 1.0");

        _topK = topK;
        _minimumSimilarity = minimumSimilarity;
        _useExactSearch = useExactSearch;
        _quantization = quantization;
    }

    /// <summary>
    /// Generates the next vector search operation by randomly selecting a query vector.
    /// </summary>
    public OperationBase NextOperation(Random rng)
    {
        var queryIndex = rng.Next(_metadata.QueryVectorCount);
        var queryVector = _metadata.QueryVectors[queryIndex];

        return new VectorSearchOperation
        {
            QueryVector = queryVector,
            FieldName = _metadata.FieldName,
            TopK = _topK,
            MinimumSimilarity = _minimumSimilarity,
            UseExactSearch = _useExactSearch,
            Quantization = _quantization
        };
    }
}
