using RavenBench.Core.Reporting;

namespace RavenBench.Reporter.Models;

/// <summary>
/// Root model for multi-run comparison reports. Contains aligned benchmark runs
/// with cross-run metrics and delta calculations relative to the baseline.
/// </summary>
public sealed class ComparisonModel
{
    /// <summary>
    /// The baseline run (first summary, or explicitly selected).
    /// All deltas are calculated relative to this run.
    /// </summary>
    public required RunComparison Baseline { get; init; }

    /// <summary>
    /// Comparison runs (1-2 additional summaries) to contrast against baseline.
    /// </summary>
    public required List<RunComparison> Contenders { get; init; }

    /// <summary>
    /// Aligned concurrency steps across all runs. Each snapshot contains
    /// metrics from all runs at a specific concurrency level (null if missing).
    /// </summary>
    public required List<ConcurrencySnapshot> AlignedSteps { get; init; }

    /// <summary>
    /// Cross-run latency contrasts for detailed tail analysis.
    /// Compares best vs best, best vs second-best, etc.
    /// </summary>
    public required List<CrossRunContrast> LatencyContrasts { get; init; }

    /// <summary>
    /// Cross-run throughput contrasts.
    /// </summary>
    public required List<CrossRunContrast> ThroughputContrasts { get; init; }

    /// <summary>
    /// Cross-run error rate contrasts.
    /// </summary>
    public required List<CrossRunContrast> ErrorRateContrasts { get; init; }

    /// <summary>
    /// Cross-run resource cost contrasts (CPU, memory).
    /// </summary>
    public required List<CrossRunContrast> ResourceContrasts { get; init; }

    /// <summary>
    /// Auto-generated takeaways based on delta thresholds.
    /// </summary>
    public required List<string> KeyTakeaways { get; init; }
}

/// <summary>
/// Single run in a comparison, with metadata and its best/second-best steps.
/// </summary>
public sealed class RunComparison
{
    /// <summary>
    /// Label for this run (from --labels or auto-generated).
    /// </summary>
    public required string Label { get; init; }

    /// <summary>
    /// The full benchmark summary.
    /// </summary>
    public required BenchmarkSummary Summary { get; init; }

    /// <summary>
    /// The step with the best quality score (throughput / p999).
    /// </summary>
    public StepResult? BestStep { get; init; }

    /// <summary>
    /// The step with the second-best quality score.
    /// </summary>
    public StepResult? SecondBestStep { get; init; }

    /// <summary>
    /// Quality score for the best step (throughput / p999).
    /// </summary>
    public double BestQualityScore { get; init; }
}

/// <summary>
/// Metrics from all runs at a specific concurrency level.
/// Allows "same load" comparisons even if best steps differ.
/// </summary>
public sealed class ConcurrencySnapshot
{
    /// <summary>
    /// The concurrency level for this snapshot.
    /// </summary>
    public required int Concurrency { get; init; }

    /// <summary>
    /// Metrics from each run at this concurrency (indexed same as Baseline + Contenders).
    /// Null if a run doesn't have data at this concurrency.
    /// </summary>
    public required List<StepMetrics?> RunMetrics { get; init; }
}

/// <summary>
/// Extracted metrics from a single step for comparison purposes.
/// </summary>
public sealed class StepMetrics
{
    public required double Throughput { get; init; }
    public required double P95 { get; init; }
    public required double P99 { get; init; }
    public required double P999 { get; init; }
    public required double ErrorRate { get; init; }
    public double? ClientCpu { get; init; }
    public double? ServerCpu { get; init; }
    public long? ServerMemoryMB { get; init; }
}

/// <summary>
/// Cross-run comparison showing absolute and percentage deltas.
/// Used for latency, throughput, error rate, and resource cost contrasts.
/// </summary>
public sealed class CrossRunContrast
{
    /// <summary>
    /// Label for the baseline run in this contrast.
    /// </summary>
    public required string BaselineLabel { get; init; }

    /// <summary>
    /// Label for the contender run in this contrast.
    /// </summary>
    public required string ContenderLabel { get; init; }

    /// <summary>
    /// Metric being compared (e.g., "P99 Latency", "Throughput", "Error Rate").
    /// </summary>
    public required string MetricName { get; init; }

    /// <summary>
    /// Baseline value.
    /// </summary>
    public required double BaselineValue { get; init; }

    /// <summary>
    /// Contender value.
    /// </summary>
    public required double ContenderValue { get; init; }

    /// <summary>
    /// Absolute delta (contender - baseline).
    /// </summary>
    public required double AbsoluteDelta { get; init; }

    /// <summary>
    /// Percentage delta ((contender - baseline) / baseline * 100).
    /// </summary>
    public required double PercentageDelta { get; init; }

    /// <summary>
    /// Concurrency level where baseline value was observed.
    /// </summary>
    public int? BaselineConcurrency { get; init; }

    /// <summary>
    /// Concurrency level where contender value was observed.
    /// </summary>
    public int? ContenderConcurrency { get; init; }

    /// <summary>
    /// Additional context (e.g., "Best vs Best", "Best vs Second-Best").
    /// </summary>
    public string? Context { get; init; }
}