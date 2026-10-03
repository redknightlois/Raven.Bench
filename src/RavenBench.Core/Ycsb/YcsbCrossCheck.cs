namespace RavenBench.Core.Ycsb;

/// <summary>The repetitions of one row through one transport mode, and the statistic of each.</summary>
public sealed record YcsbCrossCheckSide(string Mode, IReadOnlyList<string> Results, IReadOnlyList<double> Values)
{
    /// <summary>The median of the repetitions; the mean of the two middle values for an even count.</summary>
    public double Median
    {
        get
        {
            var sorted = Values.Order().ToArray();
            var middle = sorted.Length / 2;
            return sorted.Length % 2 == 1 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2;
        }
    }

    /// <summary>The spread of the repetitions: the largest value minus the smallest.</summary>
    public double Range => Values.Max() - Values.Min();
}

/// <summary>One row compared across two modes: both sides, the difference of their medians, and the noise verdict.</summary>
public sealed record YcsbCrossCheckComparison(
    string RowKey,
    string Statistic,
    string NoiseRule,
    YcsbCrossCheckSide Reference,
    YcsbCrossCheckSide Candidate,
    double Difference,
    double Noise,
    bool WithinNoise);

/// <summary>A cross-check that cannot measure the run-to-run noise of a row, so it gives no verdict.</summary>
public sealed class YcsbCrossCheckException(string message) : InvalidOperationException(message);

/// <summary>
/// Compares one ycsb row run through two transport modes against the run-to-run noise of the
/// repetitions. The noise of a row is the larger of the two modes' ranges, so a difference counts
/// as real only when it exceeds the spread either mode shows against itself.
/// </summary>
public static class YcsbCrossCheck
{
    /// <summary>The fewest repetitions per mode that give a range; one repetition has no spread.</summary>
    public const int MinimumRepetitions = 2;

    /// <summary>The noise rule, stated in every comparison and in the ycsb README.</summary>
    public const string NoiseRule =
        "within noise when |median(candidate) - median(reference)| <= max(range(reference), range(candidate)), " +
        "where range is the largest minus the smallest repetition of the row in one mode; " +
        "each mode needs at least 2 valid repetitions";

    /// <summary>Compares the two sides of one row. Fewer than two valid repetitions on either side fails instead of giving a verdict.</summary>
    public static YcsbCrossCheckComparison Compare(string rowKey, YcsbCrossCheckSide reference, YcsbCrossCheckSide candidate)
    {
        foreach (var side in new[] { reference, candidate })
        {
            if (side.Values.Count != side.Results.Count)
                throw new ArgumentException($"Mode '{side.Mode}' names {side.Results.Count} results for {side.Values.Count} values.");
            if (side.Values.Count < MinimumRepetitions)
                throw new YcsbCrossCheckException(
                    $"Row '{rowKey}' has {side.Values.Count} valid repetition(s) through '{side.Mode}'; measuring the run-to-run noise needs at least {MinimumRepetitions}. Raise Repetitions or remove the cause of the invalid steps.");
        }

        var difference = candidate.Median - reference.Median;
        var noise = Math.Max(reference.Range, candidate.Range);
        return new YcsbCrossCheckComparison(rowKey, YcsbMedianSelector.StatisticName, NoiseRule, reference, candidate,
            difference, noise, Math.Abs(difference) <= noise);
    }
}
