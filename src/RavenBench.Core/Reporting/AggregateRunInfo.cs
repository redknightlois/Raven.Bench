using RavenBench.Core.Aggregate;

namespace RavenBench.Core.Reporting;

/// <summary>A build figure with the source it was read from, or the named reason it is unavailable; never a zero by default.</summary>
public sealed record SourcedFigure(double? Value, string Unit, string? Source, string? Unavailable)
{
    public static SourcedFigure Read(double value, string unit, string source) => new(value, unit, source, null);
    public static SourcedFigure Missing(string unit, string reason) => new(null, unit, null, reason);
}

/// <summary>The build run: load and index times, and the server figures during the build.</summary>
public sealed record AggregateBuildInfo(
    double WallTimeSeconds,
    double LoadSeconds,
    double IndexSeconds,
    IReadOnlyList<string> Indexes,
    SourcedFigure ServerCpuPercent,
    SourcedFigure ServerDiskWrittenBytes,
    OnDiskSize OnDiskSize);

/// <summary>Answers and stale answers of one step, from the stale flag of each response.</summary>
public sealed record AggregateStepAnswers(int StepIndex, long Answers, long StaleAnswers)
{
    public static AggregateStepAnswers From(int stepIndex, StepResult step) => new(stepIndex, step.QueryOperations ?? 0, step.StaleQueryCount ?? 0);
}

/// <summary>
/// One query run: the shape, the ceiling the closed loop found, and the fixed rate that ran at <see cref="FixedRateFraction"/> of it.
/// The fixed rate is null when the ceiling leaves no whole rate below it, and the run then has no fixed-rate step. The index name is
/// null on a product that serves the shape without a per-shape index.
/// </summary>
public sealed record AggregateQueryInfo(
    string Shape,
    string? IndexName,
    int TopN,
    string? Filter,
    string QueryPolicy,
    double ClosedLoopRate,
    int? FixedRate,
    double FixedRateFraction,
    bool ClientBound,
    IReadOnlyList<AggregateStepAnswers> StepAnswers);

/// <summary>
/// The under-write run: the quiet step and the under-write step at one query rate, the bulk writers
/// (<c>Writer</c>), the probe writer (<c>Probe</c>), and freshness, which comes from the probe alone.
/// </summary>
public sealed record AggregateUnderWriteInfo(
    double QueryRate,
    string QueryPolicy,
    double QuietThroughput,
    double QuietP99Ms,
    double UnderWriteThroughput,
    double UnderWriteP99Ms,
    HeldWriteRate Writer,
    string WriteLatencyDefinition,
    FreshnessInfo Freshness,
    bool ClientBound,
    IReadOnlyList<AggregateStepAnswers> StepAnswers,
    HeldWriteRate Probe);

/// <summary>
/// Present only on a result from the aggregate entry point: the run, the resolved scenario and
/// every override, the target, the emitted set, and the run's own rows.
/// </summary>
public sealed record AggregateRunInfo
{
    public required string Run { get; init; }
    public required AggregateScenario ResolvedScenario { get; init; }

    /// <summary>Every option given on the command line for a scenario key, and the value it set.</summary>
    public required IReadOnlyDictionary<string, string> Overrides { get; init; }

    public required string Target { get; init; }
    public required string ProductName { get; init; }
    public required string ServerVersion { get; init; }
    public string? ImageReference { get; init; }
    public string? ImageDigest { get; init; }
    public required DurabilityParity Durability { get; init; }
    public required AggregateDataSetSummary DataSet { get; init; }
    public required string FilterCategory { get; init; }
    public required ServerColumnAvailability ServerColumns { get; init; }

    public AggregateBuildInfo? Build { get; init; }
    public AggregateQueryInfo? Query { get; init; }
    public AggregateUnderWriteInfo? UnderWrite { get; init; }
}
