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
/// The on-disk size of what a load run left behind, as the product itself reports it. Either the
/// product's own statistic named its figure, or the named fact that this product exposes none.
/// </summary>
public sealed record OnDiskSize
{
    /// <summary>The product statistic the figure is read from. Absent when the product exposes none.</summary>
    public string? Metric { get; init; }

    /// <summary>The size the product reported, in bytes. Never an estimate. Absent when the product exposes none.</summary>
    public long? Bytes { get; init; }

    /// <summary>The named fact that this product exposes no on-disk size. Absent when a figure is present.</summary>
    public string? Unavailable { get; init; }

    /// <summary>The figure a product reported, with the name of the statistic it came from.</summary>
    public static OnDiskSize Reported(string metric, long bytes) => new() { Metric = metric, Bytes = bytes };

    /// <summary>The statement a product that exposes no on-disk size leaves in the result.</summary>
    public static OnDiskSize NotExposed(string productName) =>
        new() { Unavailable = $"{productName} exposes no on-disk size to this harness." };
}

/// <summary>
/// Which server columns this harness run actually collected for the product that ran, stated
/// positively so a reader never has to infer availability from an absent key. It is derived from
/// the steps the run produced: a column is named only when at least one step carries a value for it.
/// </summary>
public sealed record ServerColumnAvailability
{
    public required string Product { get; init; }

    /// <summary>The step columns at least one step of this result carries. Empty when the harness collected none.</summary>
    public required IReadOnlyList<string> Columns { get; init; }

    /// <summary>The same fact in words, so an empty list never reads as a missing measurement.</summary>
    public required string Statement { get; init; }

    /// <summary>The source of each collected CPU and memory column, with " (host-wide)" for a node_exporter figure.</summary>
    public IReadOnlyDictionary<string, string> Sources { get; init; } = new Dictionary<string, string>();

    /// <summary>Derives the statement from the steps that were produced.</summary>
    public static ServerColumnAvailability FromSteps(string productName, IReadOnlyList<StepResult> steps)
    {
        var columns = new List<string>();
        if (steps.Any(s => s.ServerCpu.HasValue))
            columns.Add(nameof(StepResult.ServerCpu));
        if (steps.Any(s => s.ServerMemoryMB.HasValue))
            columns.Add(nameof(StepResult.ServerMemoryMB));
        if (steps.Any(s => s.ServerRequestsPerSec.HasValue))
            columns.Add(nameof(StepResult.ServerRequestsPerSec));

        var sources = new Dictionary<string, string>();
        AddSource(sources, nameof(StepResult.ServerCpu), steps.Where(s => s.ServerCpu.HasValue).Select(s => (s.ServerCpuSource, s.ServerMetricsHostWide)));
        AddSource(sources, nameof(StepResult.ServerMemoryMB), steps.Where(s => s.ServerMemoryMB.HasValue).Select(s => (s.ServerMemorySource, s.ServerMetricsHostWide)));

        return new ServerColumnAvailability
        {
            Product = productName,
            Columns = columns,
            Sources = sources,
            Statement = columns.Count == 0
                ? $"{productName} has no server column from this harness run."
                : $"{productName} server columns collected by this harness run: {string.Join(", ", columns)}."
        };
    }

    private static void AddSource(Dictionary<string, string> sources, string column, IEnumerable<(string? Source, bool? HostWide)> filled)
    {
        var names = filled
            .Select(f => f.HostWide == true ? $"{f.Source} (host-wide)" : f.Source ?? "unnamed")
            .Distinct()
            .ToList();
        if (names.Count > 0)
            sources[column] = string.Join(", ", names);
    }
}

/// <summary>
/// What a ycsb result carries on top of a Raven.Bench summary: which run of the sequence produced
/// it, the scenario as resolved for that run, and the target that ran it. Absent from a result
/// produced by any other command.
/// </summary>
public sealed record YcsbRunInfo
{
    public required string Run { get; init; }

    /// <summary>The load shape that produced this result: "closed" for the ramp, "rate" for a fixed rate.</summary>
    public required string Shape { get; init; }

    /// <summary>The key distribution this run actually used, not the scenario's first value.</summary>
    public required string Distribution { get; init; }

    /// <summary>The fixed rate this run ran at. Absent for a closed-loop run, which has none.</summary>
    public double? Rate { get; init; }

    /// <summary>Which repetition of its row this result is, counting from one.</summary>
    public required int Repetition { get; init; }

    /// <summary>
    /// True on the one repetition of this row the median rule selected, so the median of a row is
    /// identifiable from one file. False on a row whose every repetition was client-bound.
    /// </summary>
    public required bool IsRowMedian { get; init; }

    /// <summary>The statistic the median of a row is taken over, named so a reader can recompute it.</summary>
    public required string MedianStatistic { get; init; }

    public required YcsbScenario ResolvedScenario { get; init; }
    public required string ProductName { get; init; }
    public required string ServerVersion { get; init; }
    public required DurabilityParity Durability { get; init; }

    /// <summary>
    /// The image reference that ran a containerized target. Absent for the external RavenDB
    /// target, because that server did not run in a container the benchmark used.
    /// </summary>
    public string? ImageReference { get; init; }

    /// <summary>
    /// The repo digest of the image that served the run. Absent when the target did not run in a
    /// container.
    /// </summary>
    public string? ImageDigest { get; init; }

    /// <summary>
    /// The on-disk size of what the load left behind, read after the load ramp returned. Present on
    /// the load result alone; the C, A and B runs do not change the loaded set.
    /// </summary>
    public OnDiskSize? LoadedSize { get; init; }

    /// <summary>Which server columns this harness run collected for this product.</summary>
    public required ServerColumnAvailability ServerColumns { get; init; }
}
