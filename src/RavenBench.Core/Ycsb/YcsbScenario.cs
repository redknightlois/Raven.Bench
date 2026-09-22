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

    /// <summary>
    /// Product this scenario targets: "ravendb" (the external development server), "ravendb-6"
    /// and "ravendb-7" (the containerized RavenDB services), "postgresql", "mongodb" or
    /// "documentdb". The three RavenDB values select the same transport.
    /// </summary>
    public required string Target { get; init; }

    /// <summary>Size of the keyspace the load run fills and the C/A/B/insert-stream runs address.</summary>
    public required int DocumentCount { get; init; }

    /// <summary>Document size, in the format <c>--doc-size</c> accepts (e.g. "1KB").</summary>
    public required string DocumentSize { get; init; }

    /// <summary>Concurrency, in the format <c>--step</c> accepts: a ramp "8..64x2" or a fixed "32..32".</summary>
    public required string Concurrency { get; init; }

    /// <summary>
    /// The one-element spelling of <see cref="Rates"/>: a scenario carries one or the other,
    /// never both.
    /// </summary>
    public double? Rate { get; init; }

    /// <summary>
    /// The fixed rates to run after the closed-loop ramp, in operations per second. Every value is
    /// greater than zero. An absent or empty list means no fixed-rate run.
    /// </summary>
    public double[]? Rates { get; init; }

    /// <summary>
    /// The distributions workload C runs under: a non-empty list of "uniform", "zipfian" and
    /// "latest". The first value is the one every run other than C uses. An absent list means the
    /// single <see cref="Distribution"/> value alone.
    /// </summary>
    public string[]? Distributions { get; init; }

    /// <summary>
    /// How many times every workload row is repeated. One or more. Every repetition is kept as its
    /// own result and exactly one of them is marked as the median of its row.
    /// </summary>
    public int? Repetitions { get; init; }

    public required string Distribution { get; init; }

    public required string Warmup { get; init; }

    public required string Duration { get; init; }

    /// <summary>The rates the invocation runs after the ramp, with the single-rate key mapped onto the list.</summary>
    [JsonIgnore]
    public IReadOnlyList<double> ResolvedRates =>
        Rates ?? (Rate.HasValue ? new[] { Rate.Value } : Array.Empty<double>());

    /// <summary>The distributions workload C runs under; the first is the one every other run uses.</summary>
    [JsonIgnore]
    public IReadOnlyList<string> ResolvedDistributions => Distributions ?? new[] { Distribution };

    /// <summary>How many repetitions every workload row runs; one when the scenario names none.</summary>
    [JsonIgnore]
    public int ResolvedRepetitions => Repetitions ?? 1;

    /// <summary>
    /// Rejects a value outside the domain of the key that carries it, naming the key. A scenario
    /// resolved against the command line passes through this too, so an override is held to the
    /// same domain the file is.
    /// </summary>
    public void Validate()
    {
        if (Rate.HasValue && Rates != null)
            throw new YcsbScenarioException("Scenario keys 'Rate' and 'Rates' are both named; 'Rate' is the one-element spelling of 'Rates', so name one of them.");

        if (Rate is <= 0)
            throw new YcsbScenarioException($"Scenario key 'Rate' is '{Rate}'; a rate is an operations-per-second value greater than zero.");

        if (Rates != null)
        {
            foreach (var rate in Rates)
            {
                if (rate <= 0 || double.IsNaN(rate))
                    throw new YcsbScenarioException($"Scenario key 'Rates' holds '{rate}'; every rate is an operations-per-second value greater than zero.");
            }
        }

        if (Distributions != null)
        {
            if (Distributions.Length == 0)
                throw new YcsbScenarioException("Scenario key 'Distributions' is empty; name at least one distribution or leave the key out.");

            foreach (var distribution in Distributions)
            {
                if (Enum.TryParse<KeyDistributionKind>(distribution, ignoreCase: true, out _) == false)
                    throw new YcsbScenarioException($"Scenario key 'Distributions' holds '{distribution}'; valid distributions are uniform, zipfian and latest.");
            }
        }

        if (Repetitions is < 1)
            throw new YcsbScenarioException($"Scenario key 'Repetitions' is '{Repetitions}'; a row repeats at least once.");
    }

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
            var scenario = JsonSerializer.Deserialize<YcsbScenario>(File.ReadAllText(path), ReadOptions)
                           ?? throw new YcsbScenarioException($"Scenario file '{path}' holds no object.");
            scenario.Validate();
            return scenario;
        }
        catch (JsonException ex)
        {
            throw new YcsbScenarioException($"Scenario file '{path}' is not a valid scenario: {ex.Message}", ex);
        }
    }
}
