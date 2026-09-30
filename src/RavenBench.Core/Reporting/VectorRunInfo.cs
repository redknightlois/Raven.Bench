using RavenBench.Core.Vector;

namespace RavenBench.Core.Reporting;

/// <summary>The set a vector run used, the cap on its base vectors, and where its truth came from.</summary>
public sealed record VectorDatasetInfo(string Name, string Metric, int Dimensions, int? VectorCountCap, long LoadedBaseVectors, string Fingerprint, string TruthSource);

/// <summary>
/// One point of the recall curve: the product knob, its value, and what it measured. Recall divides
/// by k, so a query that returned fewer than k rows scores its missing rows as misses.
/// </summary>
public sealed record VectorEffortPoint(string Label, string Knob, double Value, double Recall, double QueriesPerSecond, int StepIndex, long ReturnedRows, int QueriesShortOfK)
{
    /// <summary>The 99th percentile of the sequential query latencies at this setting, in milliseconds.</summary>
    public double? QueryP99Ms { get; init; }
}

/// <summary>
/// The constrained run: load and recall repeated with the database container's memory limited to a
/// fraction of the raw set size. The limit is the value the container reports, not the value requested.
/// </summary>
public sealed record VectorConstrainedInfo(double Fraction, long RawSetBytes, long RequestedLimitBytes, long MemoryLimitBytes, long MemorySwapLimitBytes, string LimitSource);

/// <summary>The load row: wall time to a queryable index, peak server memory and the stored size.</summary>
public sealed record VectorLoadInfo(
    double WallTimeSeconds,
    long? PeakServerMemoryMB,
    string? PeakServerMemorySource,
    string? PeakServerMemoryUnavailable,
    OnDiskSize StoredSize);

/// <summary>The recall curve and the setting the threshold selected.</summary>
public sealed record VectorRecallInfo(int K, double Threshold, IReadOnlyList<VectorEffortPoint> Curve, VectorEffortPoint? Selected, string Statement);

/// <summary>The readers run: the closed loop found a sustained rate, and the fixed-rate step ran at it.</summary>
public sealed record VectorReadersInfo(int Readers, double ClosedLoopRate, double FixedRate, bool ClientBound);

/// <summary>The filtered run: how the labels were drawn, and the rows every query returned.</summary>
public sealed record VectorFilteredInfo(
    double Selectivity,
    long LabelledVectors,
    string LabelSource,
    IReadOnlyList<int> RowCountPerQuery,
    int QueriesShortOfK,
    double Recall,
    string RecallStatement);

/// <summary>The under-insert run: the insert slice, the query recall against the quiet run, and how the truth was built.</summary>
public sealed record VectorUnderInsertInfo(
    double InsertRate,
    double QueryRate,
    long Inserted,
    double QueryP99Ms,
    double Recall,
    double QuietRecall,
    int ScoredQueries,
    string TruthStatement);

/// <summary>
/// The pgvector recall over the published index kind, build options and search setting, compared
/// with the published figure. Every field is set on every row; a capped row states a verdict that is not "near".
/// </summary>
public sealed record VectorCrossCheckInfo(
    string Dataset,
    double PublishedRecall,
    string Source,
    string PublishedSettings,
    string PublishedIndexKind,
    IReadOnlyDictionary<string, int> PublishedBuildOptions,
    IReadOnlyDictionary<string, string> BuildSession,
    string SearchKnob,
    int SearchValue,
    string MeasuredIndexDefinition,
    string DefaultBuild,
    string TruthSource,
    long SearchedVectors,
    double MeasuredRecall,
    string Evidence,
    string EvidenceSha256,
    double Tolerance,
    string Verdict);

/// <summary>
/// Present only on a result from the vector entry point: which run this is, the resolved scenario
/// and every override, the target and its settings as the server reports them, and the run's own rows.
/// </summary>
public sealed record VectorRunInfo
{
    public required string Run { get; init; }
    public required VectorScenario ResolvedScenario { get; init; }

    /// <summary>Every option given on the command line for a scenario key, and the value it set.</summary>
    public required IReadOnlyDictionary<string, string> Overrides { get; init; }

    public required string Target { get; init; }
    public required string ProductName { get; init; }
    public required string ServerVersion { get; init; }
    public string? ImageReference { get; init; }
    public string? ImageDigest { get; init; }
    public required DurabilityParity Durability { get; init; }

    /// <summary>The index definition and server settings in force, as the product reports them.</summary>
    public required IReadOnlyDictionary<string, string> ProductSettings { get; init; }

    /// <summary>The storage the round runs: full float32 vectors, or a labelled quantized mode.</summary>
    public required string VectorStorage { get; init; }

    /// <summary>The row label: the target and its vector storage, so a quantized row never reads as a float32 row.</summary>
    public required string RowLabel { get; init; }

    public required VectorDatasetInfo Dataset { get; init; }
    public required ServerColumnAvailability ServerColumns { get; init; }

    public VectorLoadInfo? Load { get; init; }
    public VectorRecallInfo? Recall { get; init; }
    public VectorReadersInfo? Readers { get; init; }
    public VectorFilteredInfo? Filtered { get; init; }
    public VectorUnderInsertInfo? UnderInsert { get; init; }
    public VectorCrossCheckInfo? CrossCheck { get; init; }
    public VectorConstrainedInfo? Constrained { get; init; }

    /// <summary>The effort readers, filtered and under-insert ran at, and why.</summary>
    public VectorEffortPoint? EffortInForce { get; init; }
    public string? EffortStatement { get; init; }
}

public static class VectorBuildState
{
    public const string AtDefaultBuild = "the pgvector default build";

    /// <summary>
    /// States whether the published index kind and build options match the round-one index definition as
    /// <c>pg_get_indexdef</c> reports it, which names the access method and carries a <c>WITH (...)</c> clause only for set options.
    /// </summary>
    public static string DefaultBuild(string publishedKind, IReadOnlyDictionary<string, int> publishedOptions, string defaultIndexDefinition)
    {
        var match = System.Text.RegularExpressions.Regex.Match(defaultIndexDefinition, @"\bUSING\s+(\w+)", System.Text.RegularExpressions.RegexOptions.IgnoreCase);
        if (match.Success == false)
            throw new InvalidOperationException($"The default index definition '{defaultIndexDefinition}' names no access method.");
        var defaultHasOptions = defaultIndexDefinition.Contains(" WITH (", StringComparison.OrdinalIgnoreCase);
        if (string.Equals(match.Groups[1].Value, publishedKind, StringComparison.OrdinalIgnoreCase) && publishedOptions.Count == 0 && defaultHasOptions == false)
            return AtDefaultBuild;
        var published = publishedOptions.Count == 0 ? "no build options" : "WITH (" + string.Join(", ", publishedOptions.Select(o => $"{o.Key}={o.Value}")) + ")";
        return $"not {AtDefaultBuild}: the published point is {publishedKind} with {published}; the default build is '{defaultIndexDefinition}'";
    }
}
