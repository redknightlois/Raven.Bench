using System.Collections.Generic;

namespace RavenBench.Core.Workload;

public interface IWorkload
{
    OperationBase NextOperation(Random rng);

    /// <summary>
    /// Creates a warmup workload for this workload.
    /// Returns null if this workload is already read-only and can be used for warmup as-is.
    /// Returns a read-only variant if this workload performs writes/updates.
    /// </summary>
    /// <param name="preloadCount">Number of documents preloaded in the database.</param>
    /// <param name="distribution">Key distribution to use for warmup reads.</param>
    IWorkload? CreateWarmupWorkload(long preloadCount, IKeyDistribution distribution);
}

public abstract class OperationBase
{
}

public class ReadOperation : OperationBase
{
    public required string Id { get; init; }
}

/// <summary>
/// Represents a parameterized RQL query operation.
/// For legacy id-based queries, use QueryText="from @all_docs where id() = $id" with Parameters["id"] = docId.
/// For equality queries, use QueryText="from Users where Name = $name" with Parameters["name"] = value.
/// </summary>
public class QueryOperation : OperationBase
{
    /// <summary>
    /// The RQL query text with parameter placeholders (e.g., "from Users where Name = $name").
    /// </summary>
    public required string QueryText { get; init; }

    /// <summary>
    /// Query parameters to bind (e.g., { "name": "John" }).
    /// </summary>
    public required IReadOnlyDictionary<string, object?> Parameters { get; init; }

    /// <summary>
    /// Optional expected index name for validation/reporting.
    /// </summary>
    public string? ExpectedIndex { get; init; }

    // Legacy compatibility: provide Id property that extracts from parameters for backward compatibility
    public string? Id => Parameters.TryGetValue("id", out var idValue) ? idValue?.ToString() : null;
}

/// <summary>
/// Represents a vector search operation using RavenDB's vector search capabilities.
/// Supports both approximate (HNSW) and exact vector search with quantization options.
/// </summary>
public class VectorSearchOperation : OperationBase
{
    /// <summary>
    /// The query vector for similarity search.
    /// </summary>
    public required float[] QueryVector { get; init; }

    /// <summary>
    /// The field name containing the indexed vectors (e.g., "Embedding").
    /// </summary>
    public required string FieldName { get; init; }

    /// <summary>
    /// Number of nearest neighbors to return (default: 10).
    /// </summary>
    public int TopK { get; init; } = 10;

    /// <summary>
    /// Minimum similarity threshold (0.0-1.0). Results below this threshold are filtered out.
    /// </summary>
    public float MinimumSimilarity { get; init; } = 0.0f;

    /// <summary>
    /// Use exact search instead of approximate HNSW search (slower but more accurate).
    /// </summary>
    public bool UseExactSearch { get; init; } = false;

    /// <summary>
    /// Vector quantization type for the search.
    /// </summary>
    public VectorQuantization Quantization { get; init; } = VectorQuantization.None;

    /// <summary>
    /// Optional expected index name for validation/reporting.
    /// </summary>
    public string? ExpectedIndex { get; init; }

    /// <summary>
    /// Builds the RQL embedding selector based on quantization type.
    /// Uses RavenDB's vector quantization functions for efficient search.
    /// </summary>
    public string GetEmbeddingSelector()
    {
        if (string.IsNullOrWhiteSpace(FieldName))
            throw new InvalidOperationException("FieldName cannot be null or empty");

        return Quantization switch
        {
            VectorQuantization.Int8 => $"embedding.f32_i8('{FieldName}')",
            VectorQuantization.Binary => $"embedding.f32_i1('{FieldName}')",
            _ => $"'{FieldName}'"
        };
    }

    /// <summary>
    /// Builds the complete RQL query for this vector search operation.
    /// Includes quantization, exact search mode, and similarity threshold.
    /// Uses explicit index names to enable proper staleness control.
    /// </summary>
    public string ToRqlQuery()
    {
        var embeddingSelector = GetEmbeddingSelector();
        var searchClause = $"vector.search({embeddingSelector}, $vector)";

        if (UseExactSearch)
            searchClause = $"exact({searchClause})";

        var whereClause = searchClause;
        if (MinimumSimilarity > 0)
            whereClause += " >= $minSimilarity";

        // Select index based on quantization type
        var indexName = Quantization switch
        {
            VectorQuantization.Int8 => "Words/ByEmbeddingInt8",
            VectorQuantization.Binary => "Words/ByEmbeddingBinary",
            _ => "Words/ByEmbedding"
        };

        return $"from index '{indexName}' where {whereClause}";
    }
}

/// <summary>
/// Vector quantization types supported by RavenDB.
/// </summary>
public enum VectorQuantization
{
    /// <summary>
    /// Full precision float32 vectors (no quantization).
    /// </summary>
    None,

    /// <summary>
    /// 8-bit integer quantization (f32_i8). Reduces memory by ~4x.
    /// </summary>
    Int8,

    /// <summary>
    /// Binary quantization (f32_i1). Reduces memory by ~32x.
    /// </summary>
    Binary
}

public class InsertOperation<T> : OperationBase
{
    public required string Id { get; init; }
    public required T Payload { get; init; }
}

public class UpdateOperation<T> : OperationBase
{
    public required string Id { get; init; }
    public required T Payload { get; init; }
}

public class BulkInsertOperation<T> : OperationBase
{
    public required List<DocumentToWrite<T>> Documents { get; init; }
}
