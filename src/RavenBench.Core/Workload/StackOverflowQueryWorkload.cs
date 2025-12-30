namespace RavenBench.Core.Workload;

/// <summary>
/// Workload replicating consistent-questions.lua pattern:
/// Parameterized queries against questions collection using sampled document IDs
/// </summary>
public sealed class StackOverflowQueryWorkload : IWorkload
{
    private readonly int[] _questionIds;
    private readonly bool _useVoronPath;

    /// <summary>
    /// Creates a StackOverflow query workload using sampled question IDs.
    /// </summary>
    /// <param name="metadata">Workload metadata containing sampled document IDs</param>
    /// <param name="useVoronPath">If true, uses direct Voron lookup (from @all_docs where id() = $id).
    /// If false, uses index-based lookup (from Questions where Id = $id).</param>
    public StackOverflowQueryWorkload(StackOverflowWorkloadMetadata metadata, bool useVoronPath = true)
    {
        if (metadata.QuestionIds.Length == 0)
        {
            throw new ArgumentException("Metadata must contain sampled question IDs");
        }

        _questionIds = metadata.QuestionIds;
        _useVoronPath = useVoronPath;
    }

    public OperationBase NextOperation(Random rng)
    {
        var questionId = _questionIds[rng.Next(_questionIds.Length)];
        var docId = $"questions/{questionId}";

        var queryText = _useVoronPath
            ? "from @all_docs where id() = $id"
            : "from Questions where Id = $id";

        return new QueryOperation
        {
            QueryText = queryText,
            Parameters = new Dictionary<string, object?> { ["id"] = docId }
        };
    }

    public IWorkload? CreateWarmupWorkload(long preloadCount, IKeyDistribution distribution)
    {
        // StackOverflowQueryWorkload samples real document IDs, use it directly for warmup
        return null;
    }
}
