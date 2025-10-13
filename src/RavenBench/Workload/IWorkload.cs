using System.Collections.Generic;

namespace RavenBench.Workload;

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

public class QueryOperation : OperationBase
{
    public required string Id { get; init; }
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

