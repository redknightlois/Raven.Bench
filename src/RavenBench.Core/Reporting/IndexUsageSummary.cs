namespace RavenBench.Core.Reporting;

/// <summary>
/// Summarizes index usage for a single index (used in top-N summaries).
/// </summary>
public sealed class IndexUsageSummary
{
    public required string IndexName { get; init; }
    public required long UsageCount { get; init; }
    public double? UsagePercent { get; init; }
}