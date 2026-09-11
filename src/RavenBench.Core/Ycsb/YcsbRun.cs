using RavenBench.Core.Workload;

namespace RavenBench.Core.Ycsb;

/// <summary>
/// The run sequence a ycsb scenario drives: load fills the keyspace, C/A/B are the YCSB core
/// workloads and insert-stream extends the keyspace with single-document inserts. These are the
/// YCSB definitions, not a choice this codebase makes, so their mixes are pinned as values.
/// </summary>
public enum YcsbRunKind
{
    Load,
    WorkloadC,
    WorkloadA,
    WorkloadB,
    InsertStream
}

public static class YcsbRunKinds
{
    public static string ToResultName(this YcsbRunKind kind) => kind switch
    {
        YcsbRunKind.Load => "load",
        YcsbRunKind.WorkloadC => "C",
        YcsbRunKind.WorkloadA => "A",
        YcsbRunKind.WorkloadB => "B",
        YcsbRunKind.InsertStream => "insert-stream",
        _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, null)
    };

    /// <summary>100% read by id.</summary>
    public static readonly WorkloadMix WorkloadC = WorkloadMix.FromWeights(read: 100, write: 0, update: 0);

    /// <summary>50% reads, 50% one-field updates.</summary>
    public static readonly WorkloadMix WorkloadA = WorkloadMix.FromWeights(read: 50, write: 0, update: 50);

    /// <summary>95% reads, 5% one-field updates.</summary>
    public static readonly WorkloadMix WorkloadB = WorkloadMix.FromWeights(read: 95, write: 0, update: 5);

    /// <summary>The run sequence, in order: load, then the three YCSB core workloads, then insert-stream.</summary>
    public static readonly YcsbRunKind[] Sequence =
    {
        YcsbRunKind.Load, YcsbRunKind.WorkloadC, YcsbRunKind.WorkloadA, YcsbRunKind.WorkloadB, YcsbRunKind.InsertStream
    };
}

/// <summary>
/// A run of the sequence needs a keyspace it does not have. Raised before any operation is
/// issued: a workload run never quietly loads the shortfall and never substitutes another
/// operation kind for one it cannot serve.
/// </summary>
public sealed class InsufficientKeyspaceException : InvalidOperationException
{
    public InsufficientKeyspaceException(string message) : base(message)
    {
    }
}
