using System.Text.Json;
using System.Text.Json.Serialization;
using RavenBench.Core.Transport;

namespace RavenBench.Core.Vector;

/// <summary>
/// A vector scenario file is missing, unparsable, missing a required key, names an unknown key, or
/// holds a value outside its domain. The message names the offending file or key.
/// </summary>
public sealed class VectorScenarioException : Exception
{
    public VectorScenarioException(string message) : base(message)
    {
    }

    public VectorScenarioException(string message, Exception innerException) : base(message, innerException)
    {
    }
}

/// <summary>
/// The under-insert slice, the insert rate over the warmup and the duration, is not smaller than the base
/// vectors it is drawn from. The message names each scenario key and option that sets the slice and the value it needs.
/// </summary>
public sealed class InsertSliceTooLargeException(string message) : Exception(message);

/// <summary>The three search-effort settings of one product, as the product's own knob and values.</summary>
public sealed record VectorEffortSettings
{
    public required string Knob { get; init; }
    public required int Low { get; init; }

    /// <summary>The vendor's shipped default for the knob.</summary>
    public required int Default { get; init; }

    public required int High { get; init; }

    [JsonIgnore]
    public IReadOnlyList<(string Label, int Value)> All => [("low", Low), ("default", Default), ("high", High)];
}

/// <summary>
/// The published figure the pgvector recall is compared with, and the index the published run used.
/// The harness rebuilds that index kind with those options and searches at that setting, so the
/// comparison is like with like. The figure and its source are data copied from the published results.
/// </summary>
public sealed record VectorCrossCheck
{
    public required string Dataset { get; init; }

    /// <summary>The published recall@k.</summary>
    public required double PublishedRecall { get; init; }

    public required string Source { get; init; }

    /// <summary>The build and search settings of the published run, as the source states them.</summary>
    public required string PublishedSettings { get; init; }

    /// <summary>The index kind of the published point, for example <c>ivfflat</c> or <c>hnsw</c>.</summary>
    public required string PublishedIndexKind { get; init; }

    /// <summary>The build options of the published index, for example <c>lists</c> for IVFFlat.</summary>
    public required Dictionary<string, int> PublishedBuildOptions { get; init; }

    /// <summary>
    /// Session settings applied only while the cross-check index is built, for example <c>maintenance_work_mem</c>,
    /// which an IVFFlat build needs to hold its lists. Round one never runs under them.
    /// </summary>
    public required Dictionary<string, string> BuildSession { get; init; }

    /// <summary>The value of the index kind's search knob in the published run, for example <c>ivfflat.probes</c>.</summary>
    public required int PublishedSearchValue { get; init; }

    /// <summary>A repository file holding the published series as captured, relative to the scenario file.</summary>
    public required string Evidence { get; init; }

    /// <summary>The SHA-256 of the evidence file, lower-case hex.</summary>
    public required string EvidenceSha256 { get; init; }

    /// <summary>The absolute recall difference that still counts as near.</summary>
    public required double Tolerance { get; init; }
}

/// <summary>
/// Every parameter the vector benchmark runs under. Every key is required, so no numeric default
/// hides in code for a parameter the scenario owns.
/// </summary>
public sealed record VectorScenario
{
    public required string Dataset { get; init; }

    /// <summary>
    /// Loads only the first this many base vectors of the set; null loads the whole set. A capped run
    /// computes its truth by brute force over the capped base.
    /// </summary>
    public required int? VectorCountCap { get; init; }

    /// <summary>The directory the pinned set files live in, outside the repository.</summary>
    public required string DataDirectory { get; init; }

    public required int Seed { get; init; }
    public required int QueryCount { get; init; }
    public required int K { get; init; }

    /// <summary>
    /// The truth depth read before the insert slice is removed from it. It must leave at least k
    /// neighbours per query once the slice ids are dropped.
    /// </summary>
    public required int TruthDepth { get; init; }

    public required double RecallThreshold { get; init; }

    /// <summary>The effort settings per product family: "ravendb" and "pgvector".</summary>
    public required Dictionary<string, VectorEffortSettings> Efforts { get; init; }

    /// <summary>The concurrent readers; a pgvector pool holds one connection per reader.</summary>
    public required int Readers { get; init; }

    public required double FilterSelectivity { get; init; }
    public required double InsertRate { get; init; }

    /// <summary>The fixed query rate the under-insert run holds while the inserts run.</summary>
    public required double UnderInsertQueryRate { get; init; }
    public required string Warmup { get; init; }
    public required string Duration { get; init; }
    public required VectorCrossCheck CrossCheck { get; init; }

    /// <summary>The vectors the under-insert run inserts: the insert rate over the measured duration and its warmup.</summary>
    public int InsertCount(TimeSpan warmup, TimeSpan duration) => (int)Math.Ceiling(InsertRate * (warmup + duration).TotalSeconds);

    /// <summary>
    /// Throws <see cref="InsertSliceTooLargeException"/> when the insert slice leaves no loaded vector of
    /// <paramref name="baseCount"/>. The run calls it before anything is fetched or loaded.
    /// </summary>
    public void RequireInsertSliceBelow(long baseCount, TimeSpan warmup, TimeSpan duration)
    {
        var slice = InsertCount(warmup, duration);
        if (slice < baseCount)
            return;
        var seconds = (warmup + duration).TotalSeconds;
        // Rounded down, so each named value keeps the slice below the base.
        static string AtMost(double value) => (Math.Floor(value * 1000) / 1000).ToString(System.Globalization.CultureInfo.InvariantCulture);
        throw new InsertSliceTooLargeException(
            $"The under-insert slice of {slice} vectors (InsertRate {InsertRate} x (Warmup {Warmup} + Duration {Duration})) must be smaller than the {baseCount} base vectors. " +
            $"Set one of: scenario key 'InsertRate' (--insert-rate) to at most {AtMost((baseCount - 1) / seconds)}; " +
            $"scenario keys 'Warmup' and 'Duration' (--warmup, --duration) to at most {AtMost((baseCount - 1) / InsertRate)}s together; " +
            $"scenario key 'VectorCountCap' (--vector-count-cap) to at least {slice + 1}, on a set that holds that many base vectors.");
    }

    public VectorEffortSettings EffortsFor(string family) =>
        Efforts.TryGetValue(family, out var settings)
            ? settings
            : throw new VectorScenarioException($"Scenario key 'Efforts' has no entry for '{family}'.");

    public void Validate()
    {
        Require(QueryCount > 0, nameof(QueryCount), QueryCount, "a positive query count");
        Require(K > 0, nameof(K), K, "a positive k");
        Require(TruthDepth >= K, nameof(TruthDepth), TruthDepth, "a depth of at least k");
        Require(RecallThreshold is > 0 and <= 1, nameof(RecallThreshold), RecallThreshold, "a recall in (0, 1]");
        Require(Readers > 0, nameof(Readers), Readers, "a positive reader count");
        Require(VectorCountCap is null or > 0, nameof(VectorCountCap), VectorCountCap, "null or a positive vector count");
        Require(FilterSelectivity is > 0 and < 1, nameof(FilterSelectivity), FilterSelectivity, "a fraction in (0, 1)");
        Require(UnderInsertQueryRate >= 1, nameof(UnderInsertQueryRate), UnderInsertQueryRate, "a rate of at least one query per second");
        Require(InsertRate > 0, nameof(InsertRate), InsertRate, "a positive insert rate");
        Require(string.IsNullOrWhiteSpace(DataDirectory) == false, nameof(DataDirectory), DataDirectory, "a directory");
        foreach (var (family, e) in Efforts)
            Require(e.Low > 0 && e.Low <= e.Default && e.Default <= e.High, $"Efforts.{family}", $"{e.Low}/{e.Default}/{e.High}", "0 < Low <= Default <= High");
        Require(CrossCheck.PublishedRecall is > 0 and <= 1, "CrossCheck.PublishedRecall", CrossCheck.PublishedRecall, "a published recall in (0, 1]");
        Require(CrossCheck.Tolerance > 0, "CrossCheck.Tolerance", CrossCheck.Tolerance, "a positive tolerance");
        Require(PgVectorTransport.SearchKnobs.ContainsKey(CrossCheck.PublishedIndexKind), "CrossCheck.PublishedIndexKind", CrossCheck.PublishedIndexKind,
            $"one of {string.Join(", ", PgVectorTransport.SearchKnobs.Keys)}");
        Require(CrossCheck.PublishedSearchValue > 0, "CrossCheck.PublishedSearchValue", CrossCheck.PublishedSearchValue, "a positive knob value");
    }

    private static void Require(bool holds, string key, object? value, string domain)
    {
        if (holds == false)
            throw new VectorScenarioException($"Scenario key '{key}' is '{value}'; it must be {domain}.");
    }

    private static readonly JsonSerializerOptions ReadOptions = new() { UnmappedMemberHandling = JsonUnmappedMemberHandling.Disallow };

    public static VectorScenario Load(string path)
    {
        if (File.Exists(path) == false)
            throw new VectorScenarioException($"Scenario file not found: '{path}'.");
        try
        {
            var scenario = JsonSerializer.Deserialize<VectorScenario>(File.ReadAllText(path), ReadOptions)
                           ?? throw new VectorScenarioException($"Scenario file '{path}' holds no object.");
            scenario.Validate();
            return scenario;
        }
        catch (JsonException ex)
        {
            // A missing required key surfaces here with the key's name in the message.
            throw new VectorScenarioException($"Scenario file '{path}' is not a valid vector scenario: {ex.Message}", ex);
        }
    }
}
