namespace RavenBench.Workload;

/// <summary>
/// Workload replicating full-random-reads.lua pattern:
/// Random reads from questions/{id} or users/{id} (50/50 mix) using sampled document IDs
/// </summary>
public sealed class StackOverflowReadWorkload : IWorkload
{
    private readonly int[] _questionIds;
    private readonly int[] _userIds;

    /// <summary>
    /// Creates a StackOverflow read workload using sampled document IDs.
    /// </summary>
    /// <param name="metadata">Workload metadata containing sampled document IDs</param>
    public StackOverflowReadWorkload(StackOverflowWorkloadMetadata metadata)
    {
        if (metadata.QuestionIds.Length == 0 || metadata.UserIds.Length == 0)
        {
            throw new ArgumentException("Metadata must contain sampled question and user IDs");
        }

        _questionIds = metadata.QuestionIds;
        _userIds = metadata.UserIds;
    }

    public OperationBase NextOperation(Random rng)
    {
        // 50/50 split between questions and users (matching full-random-reads.lua)
        if (rng.Next(2) == 0)
        {
            var questionId = _questionIds[rng.Next(_questionIds.Length)];
            return new ReadOperation { Id = $"questions/{questionId}" };
        }
        else
        {
            var userId = _userIds[rng.Next(_userIds.Length)];
            return new ReadOperation { Id = $"users/{userId}" };
        }
    }
}
