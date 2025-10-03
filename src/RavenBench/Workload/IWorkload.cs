namespace RavenBench.Workload;

public interface IWorkload
{
    Operation NextOperation(Random rng);
}

public enum OperationType
{
    ReadById,
    Insert,
    Update,
    Query
}

public readonly struct Operation(OperationType type, string id, string? payload)
{
    public OperationType Type { get; } = type;
    public string Id { get; } = id;
    public string? Payload { get; } = payload;
}
