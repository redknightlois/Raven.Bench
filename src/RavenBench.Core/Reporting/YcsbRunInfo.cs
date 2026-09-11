using RavenBench.Core.Ycsb;

namespace RavenBench.Core.Reporting;

/// <summary>
/// The durability setting applied to the target for this run. Name and value are separate fields
/// because each product spells it differently (RavenDB's default, PostgreSQL's
/// synchronous_commit, Mongo's write concern).
/// </summary>
public sealed record DurabilityParity
{
    public required string Setting { get; init; }
    public required string Value { get; init; }
}

/// <summary>
/// What a ycsb result carries on top of a Raven.Bench summary: which run of the sequence produced
/// it, the scenario as resolved for that run, and the target that ran it. Absent from a result
/// produced by any other command.
/// </summary>
public sealed record YcsbRunInfo
{
    public required string Run { get; init; }
    public required YcsbScenario ResolvedScenario { get; init; }
    public required string ProductName { get; init; }
    public required string ServerVersion { get; init; }
    public required DurabilityParity Durability { get; init; }
}
