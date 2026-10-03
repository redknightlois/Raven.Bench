using System.Text.Json;
using RavenBench.Core;

namespace RavenBench.Core.Reporting;

/// <summary>
/// Checks if multiple benchmark summaries are compatible for comparison.
/// Compatible runs must have the same workload: profile, dataset and its size, query profile, load shape, key distribution,
/// entry point and its resolved scenario, and the machine they ran on.
/// Transport and HTTP version are ALLOWED to differ - that's the point of comparison!
/// </summary>
/// <remarks>
/// The comparison feature is designed to contrast different configurations:
/// - Client vs Raw transport
/// - HTTP/1.1 vs HTTP/2.0 vs HTTP/3.0
/// - Different compression settings
/// - Before/after performance changes
///
/// Therefore, we only enforce compatibility on workload characteristics,
/// not on transport/protocol configuration which is what we want to compare.
/// </remarks>
public static class RunCompatibilityChecker
{
    // The one list of workload fields that must match: the check and its message both read it.
    private static readonly (string Name, Func<BenchmarkSummary, object?> Value)[] WorkloadFields =
    {
        ("workload profile", s => s.Options.Profile),
        ("dataset", s => s.Options.Dataset),
        ("dataset profile", s => s.Options.DatasetProfile),
        ("dataset size", s => s.Options.DatasetSize),
        ("query profile", s => s.Options.QueryProfile),
        ("load shape", s => s.Options.Shape),
        ("key distribution", s => s.Options.Distribution),
        ("entry point", s => s.Ycsb != null ? "ycsb" : s.Vector != null ? "vector" : s.Aggregate != null ? "aggregate" : "raven-bench"),
        ("run", s => s.Ycsb?.Run ?? s.Vector?.Run ?? s.Aggregate?.Run),
        ("scenario", s => Json(s.Ycsb?.ResolvedScenario ?? s.Vector?.ResolvedScenario ?? (object?)s.Aggregate?.ResolvedScenario)),
        // The harness commit and the database image are what a before/after comparison changes; the rest is the machine.
        ("machine fingerprint", s => Json(s.MachineFingerprint is { } m ? m with { HarnessCommit = "", DatabaseImage = null } : null)),
    };

    // Value equality over nested collections, which record equality does not give.
    private static string? Json(object? value) => value == null ? null : JsonSerializer.Serialize(value, value.GetType());

    /// <summary>
    /// Determines if the provided benchmark summaries can be compared.
    /// </summary>
    /// <param name="summaries">The benchmark summaries to check.</param>
    /// <returns>True if all summaries are compatible; otherwise, false.</returns>
    public static bool AreComparable(params BenchmarkSummary[] summaries) =>
        Incompatibilities(summaries).Count == 0;

    /// <summary>
    /// Ensures that the provided benchmark summaries are compatible, throwing an exception if not.
    /// </summary>
    /// <param name="summaries">The benchmark summaries to check.</param>
    /// <exception cref="InvalidOperationException">Thrown if the summaries are not compatible.</exception>
    public static void EnsureComparable(params BenchmarkSummary[] summaries)
    {
        var incompatibilities = Incompatibilities(summaries);
        if (incompatibilities.Count == 0)
            return;

        throw new InvalidOperationException(
            "Benchmark summaries are not compatible for comparison. " +
            $"They must have the same {string.Join(", ", WorkloadFields.Select(f => f.Name))}.\n" +
            string.Join("\n", incompatibilities));
    }

    /// <summary>
    /// Every workload field in which a run differs from the first run.
    /// Transport, HTTP version, and compression settings are allowed to differ.
    /// </summary>
    private static List<string> Incompatibilities(BenchmarkSummary[] summaries)
    {
        var incompatibilities = new List<string>();
        for (int i = 1; i < summaries.Length; i++)
        {
            foreach (var (name, value) in WorkloadFields)
            {
                var expected = value(summaries[0]);
                var actual = value(summaries[i]);
                if (Equals(expected, actual) == false)
                    incompatibilities.Add($"Run {i + 1} has different {name}: {expected} vs {actual}");
            }
        }

        return incompatibilities;
    }
}
