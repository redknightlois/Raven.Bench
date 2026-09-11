using System.Text.Json;
using System.Text.Json.Serialization;

namespace RavenBench.Core.Ycsb;

/// <summary>
/// A scenario file is missing, unparsable, missing a required key, names a key the reader does
/// not recognise, or holds a value outside its domain. The message names the offending file or key.
/// </summary>
public sealed class YcsbScenarioException : Exception
{
    public YcsbScenarioException(string message) : base(message)
    {
    }

    public YcsbScenarioException(string message, Exception innerException) : base(message, innerException)
    {
    }
}

/// <summary>
/// Every parameter a ycsb run needs, read from one JSON file: the only structured-document format
/// this tree already parses. Every key is required except the optional rate, so a scenario file
/// carries the values a run uses and the reader holds no default of its own.
/// </summary>
public sealed record YcsbScenario
{
    public required int Seed { get; init; }

    /// <summary>Product this scenario targets: "ravendb", "mongodb" or "documentdb".</summary>
    public required string Target { get; init; }

    /// <summary>Size of the keyspace the load run fills and the C/A/B/insert-stream runs address.</summary>
    public required int DocumentCount { get; init; }

    /// <summary>Document size, in the format <c>--doc-size</c> accepts (e.g. "1KB").</summary>
    public required string DocumentSize { get; init; }

    /// <summary>Concurrency, in the format <c>--step</c> accepts: a ramp "8..64x2" or a fixed "32..32".</summary>
    public required string Concurrency { get; init; }

    /// <summary>Optional fixed request rate; when set, every run uses the rate load shape.</summary>
    public double? Rate { get; init; }

    public required string Distribution { get; init; }

    public required string Warmup { get; init; }

    public required string Duration { get; init; }

    private static readonly JsonSerializerOptions ReadOptions = new()
    {
        UnmappedMemberHandling = JsonUnmappedMemberHandling.Disallow
    };

    public static YcsbScenario Load(string path)
    {
        if (File.Exists(path) == false)
            throw new YcsbScenarioException($"Scenario file not found: '{path}'.");

        try
        {
            return JsonSerializer.Deserialize<YcsbScenario>(File.ReadAllText(path), ReadOptions)
                   ?? throw new YcsbScenarioException($"Scenario file '{path}' holds no object.");
        }
        catch (JsonException ex)
        {
            throw new YcsbScenarioException($"Scenario file '{path}' is not a valid scenario: {ex.Message}", ex);
        }
    }
}
