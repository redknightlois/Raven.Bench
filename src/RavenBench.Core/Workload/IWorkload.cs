using System.Collections.Generic;

namespace RavenBench.Core.Workload;

public interface IWorkload
{
    OperationBase NextOperation(Random rng);
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

