namespace RavenBench.Workload;

/// <summary>
/// Workload replicating consistent-questions.lua pattern:
/// Parameterized queries against questions collection using sampled document IDs
/// </summary>
public sealed class StackOverflowQueryWorkload : IWorkload
{
    private readonly int[] _questionIds;

    /// <summary>
    /// Creates a StackOverflow query workload using sampled question IDs.
    /// </summary>
    /// <param name="metadata">Workload metadata containing sampled document IDs</param>
    public StackOverflowQueryWorkload(StackOverflowWorkloadMetadata metadata)
    {
        if (metadata.QuestionIds.Length == 0)
        {
            throw new ArgumentException("Metadata must contain sampled question IDs");
        }

        _questionIds = metadata.QuestionIds;
    }

    public OperationBase NextOperation(Random rng)
    {
        var questionId = _questionIds[rng.Next(_questionIds.Length)];
        return new QueryOperation { Id = $"questions/{questionId}" };
    }
}
