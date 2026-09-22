using System.Globalization;
using RavenBench.Core.Reporting;

namespace RavenBench.Core.Ycsb;

/// <summary>
/// What tells one run of a ycsb invocation from every other one: which run of the sequence it is,
/// the load shape that produced it, the key distribution it addressed, the fixed rate it ran at
/// when it had one, and which repetition of its row it is. A row is every repetition that shares
/// the first four.
/// </summary>
public sealed record YcsbRunIdentity
{
    public required YcsbRunKind Kind { get; init; }
    public required LoadShape Shape { get; init; }
    public required string Distribution { get; init; }
    public double? Rate { get; init; }
    public required int Repetition { get; init; }

    /// <summary>The load shape as the result records it.</summary>
    public string ShapeName => Shape.ToString().ToLowerInvariant();

    /// <summary>The repetitions of one row share this key; repetitions of different rows are never compared.</summary>
    public string RowKey =>
        $"{Kind.ToResultName()}|{ShapeName}|{Distribution}|{Rate?.ToString("R", CultureInfo.InvariantCulture) ?? "-"}";

    /// <summary>
    /// The name that tells this run's result file and histogram artifacts from every other run's
    /// of the same invocation. It carries the whole identity, because the run kind alone repeats.
    /// </summary>
    public string ResultName
    {
        get
        {
            // A rate run is named by its rate, which already says the shape.
            var shape = Rate.HasValue
                ? "rate" + Rate.Value.ToString("0.###", CultureInfo.InvariantCulture)
                : ShapeName;

            return $"{Kind.ToResultName()}-{shape}-{Distribution.ToLowerInvariant()}-rep{Repetition}";
        }
    }
}

/// <summary>
/// The set of runs one ycsb invocation produces, built from the scenario alone so a reader can
/// predict the number of results from the file.
/// </summary>
public static class YcsbRunPlan
{
    /// <summary>The runs that follow workload C in the closed-loop ramp of one repetition.</summary>
    private static readonly YcsbRunKind[] RemainingRampKinds =
    {
        YcsbRunKind.WorkloadA, YcsbRunKind.WorkloadB, YcsbRunKind.InsertStream
    };

    /// <summary>
    /// The set, in the order it runs: the keyspace is loaded once, then every repetition runs the
    /// closed-loop ramp (workload C under each named distribution, then A, B and insert-stream
    /// under the first named distribution) and one fixed-rate workload C per named rate. The load
    /// run is multiplied by nothing, so a repetition, a second distribution and a second rate all
    /// address the one loaded keyspace.
    ///
    /// Count = 1 + repetitions * (distributions + 3 + rates).
    /// </summary>
    public static IReadOnlyList<YcsbRunIdentity> Build(YcsbScenario scenario)
    {
        scenario.Validate();

        var distributions = scenario.ResolvedDistributions;
        var primary = distributions[0];

        var plan = new List<YcsbRunIdentity>
        {
            new()
            {
                Kind = YcsbRunKind.Load,
                Shape = LoadShape.Closed,
                Distribution = primary,
                Repetition = 1
            }
        };

        for (int repetition = 1; repetition <= scenario.ResolvedRepetitions; repetition++)
        {
            foreach (var distribution in distributions)
                plan.Add(Workload(YcsbRunKind.WorkloadC, LoadShape.Closed, distribution, rate: null, repetition));

            foreach (var kind in RemainingRampKinds)
                plan.Add(Workload(kind, LoadShape.Closed, primary, rate: null, repetition));

            foreach (var rate in scenario.ResolvedRates)
                plan.Add(Workload(YcsbRunKind.WorkloadC, LoadShape.Rate, primary, rate, repetition));
        }

        return plan;
    }

    private static YcsbRunIdentity Workload(YcsbRunKind kind, LoadShape shape, string distribution, double? rate, int repetition) =>
        new()
        {
            Kind = kind,
            Shape = shape,
            Distribution = distribution,
            Rate = rate,
            Repetition = repetition
        };
}

/// <summary>
/// Picks the one repetition of every row a reader should publish, so a reader who opens one result
/// file learns whether it is the median of its row.
/// </summary>
public static class YcsbMedianSelector
{
    /// <summary>The measured scalar the median is taken over, named in the result beside the marking.</summary>
    public const string StatisticName = "MaxStepThroughput";

    /// <summary>The statistic itself: the highest throughput any step of one result reached.</summary>
    public static double Statistic(IReadOnlyList<StepResult> steps) =>
        steps.Count == 0 ? 0.0 : steps.Max(s => s.Throughput);

    /// <summary>
    /// The median result of each row, by the statistic over that row's repetitions.
    ///
    /// A repetition whose steps the client-bound guard marked invalid is a number the plan forbids
    /// publishing, so it is left out of the selection; a row whose every repetition is invalid
    /// carries no median at all. With an even number of candidates there is no single middle
    /// element, so the lower of the two middle results is the median: the published row is then the
    /// more conservative of the two.
    /// </summary>
    public static IReadOnlyList<T> SelectMedians<T>(
        IEnumerable<T> results,
        Func<T, string> rowKey,
        Func<T, IReadOnlyList<StepResult>> steps)
    {
        var medians = new List<T>();

        foreach (var row in results.GroupBy(rowKey))
        {
            // OrderBy is stable, so repetitions of equal statistic keep their run order and the
            // selection names one of them.
            var candidates = row
                .Where(r => steps(r).Count > 0 && steps(r).All(s => s.InvalidReason == null))
                .OrderBy(r => Statistic(steps(r)))
                .ToList();

            if (candidates.Count == 0)
                continue;

            medians.Add(candidates[(candidates.Count - 1) / 2]);
        }

        return medians;
    }
}
