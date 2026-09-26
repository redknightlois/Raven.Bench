using System.Text.Json;

namespace RavenBench.Core.Aggregate;

/// <summary>
/// Every parameter the aggregate benchmark runs under. Every key is required in the scenario file;
/// the only default values live in <c>benchmarks/aggregate/scenario.json</c>.
/// </summary>
public sealed record AggregateScenario
{
    public required int Seed { get; init; }
    public required long DocumentCount { get; init; }
    public required int DocumentSize { get; init; }
    public required int CategoryCardinality { get; init; }
    public required int RegionCardinality { get; init; }
    public required string Distribution { get; init; }

    /// <summary>The zipfian exponent; null for the uniform distribution. The key is required either way.</summary>
    public required double? DistributionExponent { get; init; }

    /// <summary>The top N of count-by-category.</summary>
    public required int CountTopN { get; init; }

    /// <summary>The top N of sum-by-region and filtered-group.</summary>
    public required int RegionTopN { get; init; }

    /// <summary>The share of documents the filtered-group category should hold; the run picks the emitted category nearest to it.</summary>
    public required double FilterSelectivity { get; init; }

    /// <summary>The closed-loop concurrency, and the worker count of the fixed-rate runs.</summary>
    public required int Concurrency { get; init; }

    /// <summary>The updates per second the under-write bulk writers are asked to hold.</summary>
    public required double WriteRate { get; init; }

    /// <summary>The most bulk updates in flight at once during under-write.</summary>
    public required int Writers { get; init; }

    /// <summary>The fixed count-by-category query rate of the quiet and the under-write steps.</summary>
    public required double UnderWriteQueryRate { get; init; }

    public required string Warmup { get; init; }
    public required string Duration { get; init; }

    /// <summary>How long build waits for every RavenDB map-reduce index to report non-stale.</summary>
    public required string NonStaleTimeout { get; init; }

    /// <summary>The directory the emitted set's manifest is written to, outside the repository.</summary>
    public required string DataDirectory { get; init; }

    public AggregateDataSpec DataSpec() => new(Seed, DocumentCount, DocumentSize, CategoryCardinality, RegionCardinality,
        GroupDistribution.Parse(Distribution, DistributionExponent));

    /// <summary>Throws an <see cref="AggregateScenarioException"/> naming the first parameter outside its domain.</summary>
    public void Validate()
    {
        DataSpec();
        Require(CountTopN >= 1, "countTopN", CountTopN, "at least 1");
        Require(RegionTopN >= 1, "regionTopN", RegionTopN, "at least 1");
        Require(FilterSelectivity is > 0 and < 1, "filterSelectivity", FilterSelectivity, "a fraction in (0, 1)");
        Require(Concurrency >= 1, "concurrency", Concurrency, "at least 1");
        Require(WriteRate > 0, "writeRate", WriteRate, "a positive rate");
        Require(Writers >= 1, "writers", Writers, "at least 1");
        Require(UnderWriteQueryRate >= 1, "underWriteQueryRate", UnderWriteQueryRate, "at least one query per second");
        Require(string.IsNullOrWhiteSpace(DataDirectory) == false, "dataDirectory", DataDirectory, "a directory");
        foreach (var (name, value) in new[] { ("warmup", Warmup), ("duration", Duration), ("nonStaleTimeout", NonStaleTimeout) })
            Require(string.IsNullOrWhiteSpace(value) == false, name, value, "a duration such as 500ms, 30s or 5m");
    }

    /// <summary>The expanded data directory; throws when it resolves inside the repository.</summary>
    public string ResolveDataDirectory(string repositoryRoot)
    {
        var full = Path.GetFullPath(Environment.ExpandEnvironmentVariables(DataDirectory));
        var root = Path.GetFullPath(repositoryRoot).TrimEnd(Path.DirectorySeparatorChar) + Path.DirectorySeparatorChar;
        if ((full + Path.DirectorySeparatorChar).StartsWith(root, StringComparison.Ordinal))
            throw new AggregateScenarioException("dataDirectory", $"'{full}' is inside the repository '{repositoryRoot}'; generated data lives outside it");
        return full;
    }

    private static void Require(bool holds, string parameter, object? value, string domain)
    {
        if (holds == false)
            throw new AggregateScenarioException(parameter, $"is '{value}'; it must be {domain}");
    }

    private static readonly string[] Keys =
    [
        "seed", "documentCount", "documentSize", "categoryCardinality", "regionCardinality", "distribution", "distributionExponent",
        "countTopN", "regionTopN", "filterSelectivity", "concurrency", "writeRate", "writers", "underWriteQueryRate", "warmup", "duration",
        "nonStaleTimeout", "dataDirectory"
    ];

    public static AggregateScenario Load(string path)
    {
        if (File.Exists(path) == false)
            throw new FileNotFoundException($"Aggregate scenario file not found: '{path}'.", path);
        using var doc = JsonDocument.Parse(File.ReadAllText(path));
        return FromJson(doc.RootElement);
    }

    /// <summary>Reads every key; a missing key, an unknown key or a value of the wrong kind throws naming the parameter.</summary>
    public static AggregateScenario FromJson(JsonElement root)
    {
        foreach (var property in root.EnumerateObject())
            if (Keys.Contains(property.Name, StringComparer.Ordinal) == false)
                throw new AggregateScenarioException(property.Name, "is not an aggregate scenario parameter");

        JsonElement Get(string name, JsonValueKind kind)
        {
            if (root.TryGetProperty(name, out var value) == false)
                throw new AggregateScenarioException(name, "is required");
            return value.ValueKind == kind ? value : throw new AggregateScenarioException(name, $"must be a {kind}, got {value.ValueKind}");
        }

        if (root.TryGetProperty("distributionExponent", out var exponent) == false)
            throw new AggregateScenarioException("distributionExponent", "is required; null for the uniform distribution");

        var scenario = new AggregateScenario
        {
            Seed = Get("seed", JsonValueKind.Number).GetInt32(),
            DocumentCount = Get("documentCount", JsonValueKind.Number).GetInt64(),
            DocumentSize = Get("documentSize", JsonValueKind.Number).GetInt32(),
            CategoryCardinality = Get("categoryCardinality", JsonValueKind.Number).GetInt32(),
            RegionCardinality = Get("regionCardinality", JsonValueKind.Number).GetInt32(),
            Distribution = Get("distribution", JsonValueKind.String).GetString()!,
            DistributionExponent = exponent.ValueKind == JsonValueKind.Null ? null : Get("distributionExponent", JsonValueKind.Number).GetDouble(),
            CountTopN = Get("countTopN", JsonValueKind.Number).GetInt32(),
            RegionTopN = Get("regionTopN", JsonValueKind.Number).GetInt32(),
            FilterSelectivity = Get("filterSelectivity", JsonValueKind.Number).GetDouble(),
            Concurrency = Get("concurrency", JsonValueKind.Number).GetInt32(),
            WriteRate = Get("writeRate", JsonValueKind.Number).GetDouble(),
            Writers = Get("writers", JsonValueKind.Number).GetInt32(),
            UnderWriteQueryRate = Get("underWriteQueryRate", JsonValueKind.Number).GetDouble(),
            Warmup = Get("warmup", JsonValueKind.String).GetString()!,
            Duration = Get("duration", JsonValueKind.String).GetString()!,
            NonStaleTimeout = Get("nonStaleTimeout", JsonValueKind.String).GetString()!,
            DataDirectory = Get("dataDirectory", JsonValueKind.String).GetString()!
        };
        scenario.Validate();
        return scenario;
    }
}
