using RavenBench.Core.Reporting;
using RavenBench.Reporter.Models;

namespace RavenBench.Reporter.Commands;

/// <summary>
/// Builds comparison models from benchmark summaries.
/// </summary>
public static class ComparisonModelBuilder
{
    /// <summary>
    /// Builds a comparison model from multiple benchmark summaries.
    /// </summary>
    /// <param name="summaries">The benchmark summaries to compare (2-3).</param>
    /// <param name="labels">Labels for each summary (must match count).</param>
    /// <param name="baselineIndex">Index of the baseline summary (default 0).</param>
    /// <returns>The comparison model.</returns>
    public static ComparisonModel Build(IReadOnlyList<BenchmarkSummary> summaries, IReadOnlyList<string> labels, int baselineIndex = 0)
    {
        if (summaries.Count < 2 || summaries.Count > 3)
            throw new ArgumentException("Must provide 2-3 summaries for comparison.", nameof(summaries));

        if (labels.Count != summaries.Count)
            throw new ArgumentException("Labels count must match summaries count.", nameof(labels));

        RunCompatibilityChecker.EnsureComparable(summaries.ToArray());

        var runs = new List<RunComparison>();
        for (int i = 0; i < summaries.Count; i++)
        {
            var run = BuildRunComparison(summaries[i], labels[i]);
            runs.Add(run);
        }

        var baseline = runs[baselineIndex];
        var contenders = runs.Where((_, i) => i != baselineIndex).ToList();

        var alignedSteps = BuildAlignedSteps(runs);
        var latencyContrasts = BuildLatencyContrasts(baseline, contenders);
        var throughputContrasts = BuildThroughputContrasts(baseline, contenders);
        var errorRateContrasts = BuildErrorRateContrasts(baseline, contenders);
        var resourceContrasts = BuildResourceContrasts(baseline, contenders);
        var keyTakeaways = GenerateKeyTakeaways(latencyContrasts, throughputContrasts, errorRateContrasts, resourceContrasts, baseline, contenders);

        return new ComparisonModel
        {
            Baseline = baseline,
            Contenders = contenders,
            AlignedSteps = alignedSteps,
            LatencyContrasts = latencyContrasts,
            ThroughputContrasts = throughputContrasts,
            ErrorRateContrasts = errorRateContrasts,
            ResourceContrasts = resourceContrasts,
            KeyTakeaways = keyTakeaways
        };
    }

    private static RunComparison BuildRunComparison(BenchmarkSummary summary, string label)
    {
        var steps = summary.Steps.ToList();

        // Find best and second-best steps by quality score: throughput / p999
        StepResult? bestStep = null;
        StepResult? secondBestStep = null;
        double bestScore = 0;
        double secondBestScore = 0;

        foreach (var step in steps)
        {
            double p999 = step.Raw.P999;
            if (p999 <= 0) continue; // Skip invalid steps

            double score = step.Throughput / p999;
            if (score > bestScore)
            {
                secondBestStep = bestStep;
                secondBestScore = bestScore;
                bestStep = step;
                bestScore = score;
            }
            else if (score > secondBestScore)
            {
                secondBestStep = step;
                secondBestScore = score;
            }
        }

        return new RunComparison
        {
            Label = label,
            Summary = summary,
            BestStep = bestStep,
            SecondBestStep = secondBestStep,
            BestQualityScore = bestScore
        };
    }

    /// <summary>
    /// Builds latency contrasts (P99) between baseline and contenders.
    /// Creates best-vs-best, best-vs-second-best, and second-best-vs-best comparisons.
    /// </summary>
    private static List<CrossRunContrast> BuildLatencyContrasts(RunComparison baseline, List<RunComparison> contenders)
    {
        var contrasts = new List<CrossRunContrast>();

        foreach (var contender in contenders)
        {
            // Best vs Best (P99)
            if (baseline.BestStep != null && contender.BestStep != null)
            {
                contrasts.Add(CreateContrast(
                    baseline.Label, contender.Label,
                    "P99 Latency (ms)",
                    baseline.BestStep.Raw.P99, contender.BestStep.Raw.P99,
                    baseline.BestStep.Concurrency, contender.BestStep.Concurrency,
                    "Best vs Best"));
            }

            // Best vs Second-Best (P99)
            if (baseline.BestStep != null && contender.SecondBestStep != null)
            {
                contrasts.Add(CreateContrast(
                    $"{baseline.Label} (best)", $"{contender.Label} (2nd best)",
                    "P99 Latency (ms)",
                    baseline.BestStep.Raw.P99, contender.SecondBestStep.Raw.P99,
                    baseline.BestStep.Concurrency, contender.SecondBestStep.Concurrency,
                    "Best vs Second-Best"));
            }

            // Second-Best vs Best (P99)
            if (baseline.SecondBestStep != null && contender.BestStep != null)
            {
                contrasts.Add(CreateContrast(
                    $"{baseline.Label} (2nd best)", $"{contender.Label} (best)",
                    "P99 Latency (ms)",
                    baseline.SecondBestStep.Raw.P99, contender.BestStep.Raw.P99,
                    baseline.SecondBestStep.Concurrency, contender.BestStep.Concurrency,
                    "Second-Best vs Best"));
            }
        }

        return contrasts;
    }

    /// <summary>
    /// Builds throughput contrasts between baseline and contenders.
    /// </summary>
    private static List<CrossRunContrast> BuildThroughputContrasts(RunComparison baseline, List<RunComparison> contenders)
    {
        var contrasts = new List<CrossRunContrast>();

        foreach (var contender in contenders)
        {
            if (baseline.BestStep != null && contender.BestStep != null)
            {
                contrasts.Add(CreateContrast(
                    baseline.Label, contender.Label,
                    "Throughput (ops/s)",
                    baseline.BestStep.Throughput, contender.BestStep.Throughput,
                    baseline.BestStep.Concurrency, contender.BestStep.Concurrency,
                    "Best vs Best"));
            }
        }

        return contrasts;
    }

    /// <summary>
    /// Builds error rate contrasts between baseline and contenders.
    /// </summary>
    private static List<CrossRunContrast> BuildErrorRateContrasts(RunComparison baseline, List<RunComparison> contenders)
    {
        var contrasts = new List<CrossRunContrast>();

        foreach (var contender in contenders)
        {
            if (baseline.BestStep != null && contender.BestStep != null)
            {
                contrasts.Add(CreateContrast(
                    baseline.Label, contender.Label,
                    "Error Rate (%)",
                    baseline.BestStep.ErrorRate * 100, contender.BestStep.ErrorRate * 100,
                    baseline.BestStep.Concurrency, contender.BestStep.Concurrency,
                    "Best vs Best"));
            }
        }

        return contrasts;
    }

    /// <summary>
    /// Builds resource cost contrasts (CPU, memory) between baseline and contenders.
    /// </summary>
    private static List<CrossRunContrast> BuildResourceContrasts(RunComparison baseline, List<RunComparison> contenders)
    {
        var contrasts = new List<CrossRunContrast>();

        foreach (var contender in contenders)
        {
            if (baseline.BestStep != null && contender.BestStep != null)
            {
                // Server CPU contrast
                if (baseline.BestStep.ServerCpu.HasValue && contender.BestStep.ServerCpu.HasValue)
                {
                    contrasts.Add(CreateContrast(
                        baseline.Label, contender.Label,
                        "Server CPU (%)",
                        baseline.BestStep.ServerCpu.Value, contender.BestStep.ServerCpu.Value,
                        baseline.BestStep.Concurrency, contender.BestStep.Concurrency,
                        "Best vs Best"));
                }

                // Server Memory contrast
                if (baseline.BestStep.ServerMemoryMB.HasValue && contender.BestStep.ServerMemoryMB.HasValue)
                {
                    contrasts.Add(CreateContrast(
                        baseline.Label, contender.Label,
                        "Server Memory (MB)",
                        baseline.BestStep.ServerMemoryMB.Value, contender.BestStep.ServerMemoryMB.Value,
                        baseline.BestStep.Concurrency, contender.BestStep.Concurrency,
                        "Best vs Best"));
                }
            }
        }

        return contrasts;
    }

    /// <summary>
    /// Creates a single cross-run contrast with delta calculations.
    /// </summary>
    /// <remarks>
    /// Percentage delta is calculated as: ((contender - baseline) / baseline) * 100
    /// When baseline is zero, percentage delta is set to zero to avoid division by zero.
    /// This can happen in edge cases where metrics are not available or invalid.
    /// </remarks>
    private static CrossRunContrast CreateContrast(
        string baselineLabel, string contenderLabel,
        string metricName,
        double baselineValue, double contenderValue,
        int? baselineConcurrency, int? contenderConcurrency,
        string? context)
    {
        double absoluteDelta = contenderValue - baselineValue;
        double percentageDelta = baselineValue != 0 ? (absoluteDelta / baselineValue) * 100 : 0;

        return new CrossRunContrast
        {
            BaselineLabel = baselineLabel,
            ContenderLabel = contenderLabel,
            MetricName = metricName,
            BaselineValue = baselineValue,
            ContenderValue = contenderValue,
            AbsoluteDelta = absoluteDelta,
            PercentageDelta = percentageDelta,
            BaselineConcurrency = baselineConcurrency,
            ContenderConcurrency = contenderConcurrency,
            Context = context
        };
    }

    /// <summary>
    /// Builds aligned concurrency snapshots across all runs.
    /// Each snapshot contains metrics from all runs at a specific concurrency level.
    /// </summary>
    private static List<ConcurrencySnapshot> BuildAlignedSteps(List<RunComparison> runs)
    {
        // Collect all unique concurrency levels across all runs
        var allConcurrencies = runs
            .SelectMany(r => r.Summary.Steps.Select(s => s.Concurrency))
            .Distinct()
            .OrderBy(c => c)
            .ToList();

        var snapshots = new List<ConcurrencySnapshot>();
        foreach (int concurrency in allConcurrencies)
        {
            var runMetrics = new List<StepMetrics?>();
            foreach (var run in runs)
            {
                var step = run.Summary.Steps.FirstOrDefault(s => s.Concurrency == concurrency);
                if (step == null)
                {
                    runMetrics.Add(null);
                }
                else
                {
                    runMetrics.Add(new StepMetrics
                    {
                        Throughput = step.Throughput,
                        P95 = step.Raw.P95,
                        P99 = step.Raw.P99,
                        P999 = step.Raw.P999,
                        ErrorRate = step.ErrorRate,
                        ClientCpu = step.ClientCpu,
                        ServerCpu = step.ServerCpu,
                        ServerMemoryMB = step.ServerMemoryMB
                    });
                }
            }

            snapshots.Add(new ConcurrencySnapshot
            {
                Concurrency = concurrency,
                RunMetrics = runMetrics
            });
        }

        return snapshots;
    }

    /// <summary>
    /// Generates key takeaways from cross-run contrasts.
    /// Highlights biggest improvements/regressions in throughput, latency, error rate, and knee concurrency.
    /// </summary>
    private static List<string> GenerateKeyTakeaways(
        List<CrossRunContrast> latencyContrasts,
        List<CrossRunContrast> throughputContrasts,
        List<CrossRunContrast> errorRateContrasts,
        List<CrossRunContrast> resourceContrasts,
        RunComparison baseline,
        List<RunComparison> contenders)
    {
        var takeaways = new List<string>();

        // Find biggest throughput improvement/regression
        var throughputChanges = throughputContrasts
            .Where(c => c.PercentageDelta != 0)
            .OrderByDescending(c => Math.Abs(c.PercentageDelta))
            .ToList();

        if (throughputChanges.Any())
        {
            var biggest = throughputChanges.First();
            string direction = biggest.PercentageDelta > 0 ? "improvement" : "regression";
            takeaways.Add($"Biggest throughput {direction}: {Math.Abs(biggest.PercentageDelta):F1}% ({biggest.ContenderLabel} vs {biggest.BaselineLabel})");
        }

        // Find biggest latency improvement/regression (lower is better, so sign is inverted)
        var latencyChanges = latencyContrasts
            .Where(c => c.PercentageDelta != 0 && c.Context == "Best vs Best") // Focus on best-vs-best for main takeaway
            .OrderByDescending(c => Math.Abs(c.PercentageDelta))
            .ToList();

        if (latencyChanges.Any())
        {
            var biggest = latencyChanges.First();
            string direction = biggest.PercentageDelta < 0 ? "improvement" : "regression"; // Lower latency is better
            takeaways.Add($"Biggest latency {direction}: {Math.Abs(biggest.PercentageDelta):F1}% ({biggest.ContenderLabel} vs {biggest.BaselineLabel})");
        }

        // Error rate changes
        var errorChanges = errorRateContrasts
            .Where(c => c.AbsoluteDelta != 0)
            .OrderByDescending(c => Math.Abs(c.AbsoluteDelta))
            .ToList();

        if (errorChanges.Any())
        {
            var biggest = errorChanges.First();
            string direction = biggest.AbsoluteDelta < 0 ? "decrease" : "increase";
            takeaways.Add($"Error rate {direction}: {Math.Abs(biggest.AbsoluteDelta):F3}% ({biggest.ContenderLabel} vs {biggest.BaselineLabel})");
        }

        // Knee concurrency differences
        var kneeDifferences = contenders
            .Where(c => baseline.BestStep != null && c.BestStep != null)
            .Select(c => new
            {
                Label = c.Label,
                Difference = c.BestStep!.Concurrency - baseline.BestStep!.Concurrency
            })
            .Where(d => d.Difference != 0)
            .OrderByDescending(d => Math.Abs(d.Difference))
            .ToList();

        if (kneeDifferences.Any())
        {
            var biggest = kneeDifferences.First();
            string direction = biggest.Difference > 0 ? "higher" : "lower";
            takeaways.Add($"Knee concurrency {direction} by {Math.Abs(biggest.Difference)} ({biggest.Label} vs {baseline.Label})");
        }

        // Limit to top 3-5 takeaways
        return takeaways.Take(5).ToList();
    }
}