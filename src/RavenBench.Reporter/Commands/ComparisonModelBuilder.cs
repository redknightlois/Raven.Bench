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
    /// <param name="summaries">The benchmark summaries to compare (2+).</param>
    /// <param name="labels">Labels for each summary (must match count).</param>
    /// <param name="baselineIndex">Index of the baseline summary (default 0).</param>
    /// <returns>The comparison model.</returns>
    public static ComparisonModel Build(IReadOnlyList<BenchmarkSummary> summaries, IReadOnlyList<string> labels, int baselineIndex = 0)
    {
        if (summaries.Count < 2)
            throw new ArgumentException("Must provide at least 2 summaries for comparison.", nameof(summaries));

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

        bool isRateBased = runs.Any(r => r.Summary.Steps.Any(s => s.TargetThroughput.HasValue));
        var alignedSteps = BuildAlignedSteps(runs, isRateBased);
        var latencyContrasts = BuildLatencyContrasts(baseline, contenders);
        var throughputContrasts = BuildThroughputContrasts(baseline, contenders);
        var errorRateContrasts = BuildErrorRateContrasts(baseline, contenders);
        var resourceContrasts = BuildResourceContrasts(baseline, contenders);
        var keyTakeaways = GenerateKeyTakeaways(latencyContrasts, throughputContrasts, errorRateContrasts, baseline, contenders);

        return new ComparisonModel
        {
            Baseline = baseline,
            Contenders = contenders,
            AlignedSteps = alignedSteps,
            LatencyContrasts = latencyContrasts,
            ThroughputContrasts = throughputContrasts,
            ErrorRateContrasts = errorRateContrasts,
            ResourceContrasts = resourceContrasts,
            KeyTakeaways = keyTakeaways,
            AxisLabel = isRateBased ? "Target RPS" : "Concurrency"
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
                    ContrastContext.BestVsBest));
            }

            // Best vs Second-Best (P99)
            if (baseline.BestStep != null && contender.SecondBestStep != null)
            {
                contrasts.Add(CreateContrast(
                    $"{baseline.Label} (best)", $"{contender.Label} (2nd best)",
                    "P99 Latency (ms)",
                    baseline.BestStep.Raw.P99, contender.SecondBestStep.Raw.P99,
                    baseline.BestStep.Concurrency, contender.SecondBestStep.Concurrency,
                    ContrastContext.BestVsSecondBest));
            }

            // Second-Best vs Best (P99)
            if (baseline.SecondBestStep != null && contender.BestStep != null)
            {
                contrasts.Add(CreateContrast(
                    $"{baseline.Label} (2nd best)", $"{contender.Label} (best)",
                    "P99 Latency (ms)",
                    baseline.SecondBestStep.Raw.P99, contender.BestStep.Raw.P99,
                    baseline.SecondBestStep.Concurrency, contender.BestStep.Concurrency,
                    ContrastContext.SecondBestVsBest));
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
                    ContrastContext.BestVsBest));
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
                    ContrastContext.BestVsBest));
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
                        ContrastContext.BestVsBest));
                }

                // Server Memory contrast
                if (baseline.BestStep.ServerMemoryMB.HasValue && contender.BestStep.ServerMemoryMB.HasValue)
                {
                    contrasts.Add(CreateContrast(
                        baseline.Label, contender.Label,
                        "Server Memory (MB)",
                        baseline.BestStep.ServerMemoryMB.Value, contender.BestStep.ServerMemoryMB.Value,
                        baseline.BestStep.Concurrency, contender.BestStep.Concurrency,
                        ContrastContext.BestVsBest));
                }
            }
        }

        return contrasts;
    }

    /// <summary>
    /// Creates a single cross-run contrast with delta calculations.
    /// Percentage delta is null when the baseline value is zero.
    /// </summary>
    private static CrossRunContrast CreateContrast(
        string baselineLabel, string contenderLabel,
        string metricName,
        double baselineValue, double contenderValue,
        int? baselineConcurrency, int? contenderConcurrency,
        ContrastContext context)
    {
        double absoluteDelta = contenderValue - baselineValue;
        double? percentageDelta = baselineValue != 0 ? (absoluteDelta / baselineValue) * 100 : null;

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
    /// Builds aligned snapshots across all runs, one per load level.
    /// Rate-based runs align by target throughput; closed-loop runs align by concurrency.
    /// </summary>
    private static List<ConcurrencySnapshot> BuildAlignedSteps(List<RunComparison> runs, bool isRateBased)
    {
        Func<StepResult, double?> keySelector = isRateBased
            ? s => s.TargetThroughput
            : s => s.Concurrency;

        var keys = runs
            .SelectMany(r => r.Summary.Steps.Select(keySelector))
            .Where(k => k.HasValue)
            .Select(k => k!.Value)
            .Distinct()
            .OrderBy(k => k)
            .ToList();

        var snapshots = new List<ConcurrencySnapshot>();
        foreach (double key in keys)
        {
            var runMetrics = new List<StepMetrics?>();
            foreach (var run in runs)
            {
                var step = run.Summary.Steps.FirstOrDefault(s =>
                    keySelector(s) is double k && Math.Abs(k - key) < 0.01);
                runMetrics.Add(step == null ? null : ToStepMetrics(step));
            }

            snapshots.Add(new ConcurrencySnapshot
            {
                Concurrency = key,
                RunMetrics = runMetrics
            });
        }

        return snapshots;
    }

    private static StepMetrics ToStepMetrics(StepResult step)
    {
        return new StepMetrics
        {
            Throughput = step.Throughput,
            P95 = step.Raw.P95,
            P99 = step.Raw.P99,
            P999 = step.Raw.P999,
            ErrorRate = step.ErrorRate,
            ClientCpu = step.ClientCpu,
            ServerCpu = step.ServerCpu,
            ServerMemoryMB = step.ServerMemoryMB
        };
    }

    /// <summary>
    /// Generates key takeaways from cross-run contrasts.
    /// Highlights biggest changes in throughput, latency, error rate, and best-quality concurrency.
    /// </summary>
    private static List<string> GenerateKeyTakeaways(
        List<CrossRunContrast> latencyContrasts,
        List<CrossRunContrast> throughputContrasts,
        List<CrossRunContrast> errorRateContrasts,
        RunComparison baseline,
        List<RunComparison> contenders)
    {
        var takeaways = new List<string>();

        var biggestThroughput = BiggestChange(throughputContrasts);
        if (biggestThroughput != null)
        {
            string direction = biggestThroughput.AbsoluteDelta > 0 ? "improvement" : "regression";
            takeaways.Add($"Biggest throughput {direction}: {FormatMagnitude(biggestThroughput)} ({biggestThroughput.ContenderLabel} vs {biggestThroughput.BaselineLabel})");
        }

        var biggestLatency = BiggestChange(latencyContrasts.Where(c => c.Context == ContrastContext.BestVsBest));
        if (biggestLatency != null)
        {
            string direction = biggestLatency.AbsoluteDelta < 0 ? "improvement" : "regression";
            takeaways.Add($"Biggest latency {direction}: {FormatMagnitude(biggestLatency)} ({biggestLatency.ContenderLabel} vs {biggestLatency.BaselineLabel})");
        }

        var biggestError = errorRateContrasts
            .Where(c => c.AbsoluteDelta != 0)
            .OrderByDescending(c => Math.Abs(c.AbsoluteDelta))
            .FirstOrDefault();

        if (biggestError != null)
        {
            string direction = biggestError.AbsoluteDelta < 0 ? "decrease" : "increase";
            takeaways.Add($"Error rate {direction}: {Math.Abs(biggestError.AbsoluteDelta):F3}% ({biggestError.ContenderLabel} vs {biggestError.BaselineLabel})");
        }

        var bestConcurrencyDifferences = contenders
            .Where(c => baseline.BestStep != null && c.BestStep != null)
            .Select(c => new
            {
                Label = c.Label,
                Difference = c.BestStep!.Concurrency - baseline.BestStep!.Concurrency
            })
            .Where(d => d.Difference != 0)
            .OrderByDescending(d => Math.Abs(d.Difference))
            .ToList();

        if (bestConcurrencyDifferences.Any())
        {
            var biggest = bestConcurrencyDifferences.First();
            string direction = biggest.Difference > 0 ? "higher" : "lower";
            takeaways.Add($"Best-quality concurrency {direction} by {Math.Abs(biggest.Difference)} ({biggest.Label} vs {baseline.Label})");
        }

        return takeaways;
    }

    /// <summary>
    /// Picks the contrast with the largest relative change; a null percentage delta (zero baseline) ranks highest.
    /// </summary>
    private static CrossRunContrast? BiggestChange(IEnumerable<CrossRunContrast> contrasts)
    {
        return contrasts
            .Where(c => c.AbsoluteDelta != 0)
            .OrderByDescending(c => c.PercentageDelta.HasValue ? Math.Abs(c.PercentageDelta.Value) : double.PositiveInfinity)
            .FirstOrDefault();
    }

    private static string FormatMagnitude(CrossRunContrast contrast)
    {
        return contrast.PercentageDelta.HasValue
            ? $"{Math.Abs(contrast.PercentageDelta.Value):F1}%"
            : $"{Math.Abs(contrast.AbsoluteDelta):F1} from a zero baseline";
    }
}