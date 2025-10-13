namespace RavenBench.Workload;

public class DocumentToWrite<T>
{
    public required string Id { get; init; }
    public required T Document { get; init; }
}
