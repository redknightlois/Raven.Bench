using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using RavenBench.Core.Workload;

namespace RavenBench.Core.Aggregate;

/// <summary>A scenario parameter of the aggregate document set that is missing or out of range.</summary>
public sealed class AggregateScenarioException(string parameter, string message)
    : ArgumentException($"Aggregate scenario parameter '{parameter}': {message}", parameter)
{
    public string Parameter { get; } = parameter;
}

/// <summary>
/// How group values are drawn. <c>uniform</c> gives every value the same weight; <c>zipfian</c>
/// makes the first value the hottest, with the named exponent setting the skew.
/// </summary>
public sealed record GroupDistribution(string Name, double? Exponent)
{
    public const string Uniform = "uniform";
    public const string Zipfian = "zipfian";

    public static GroupDistribution Parse(string? name, double? exponent)
    {
        if (string.IsNullOrWhiteSpace(name))
            throw new AggregateScenarioException("distribution", "is required");
        return name switch
        {
            Uniform when exponent is null => new(Uniform, null),
            Uniform => throw new AggregateScenarioException("distributionExponent", "applies only to the zipfian distribution"),
            Zipfian when exponent is > 0 and < 1 => new(Zipfian, exponent),
            Zipfian => throw new AggregateScenarioException("distributionExponent", $"the zipfian distribution needs an exponent in (0, 1), got {exponent?.ToString() ?? "none"}"),
            _ => throw new AggregateScenarioException("distribution", $"unknown distribution '{name}'; known are '{Uniform}' and '{Zipfian}'")
        };
    }

    internal IKeyDistribution Create() => Name == Zipfian ? new ZipfianDistribution(Exponent!.Value) : new UniformDistribution();

    public override string ToString() => Exponent is null ? Name : $"{Name}({Exponent.Value.ToString(System.Globalization.CultureInfo.InvariantCulture)})";
}

/// <summary>The parameters that fix the aggregate document set. Every parameter is required.</summary>
public sealed record AggregateDataSpec
{
    public int Seed { get; }
    public long DocumentCount { get; }
    public int DocumentSizeBytes { get; }
    public int CategoryCardinality { get; }
    public int RegionCardinality { get; }
    public GroupDistribution Distribution { get; }

    public AggregateDataSpec(int seed, long? documentCount, int? documentSizeBytes, int? categoryCardinality, int? regionCardinality, GroupDistribution? distribution)
    {
        Seed = seed;
        DocumentCount = AtLeastOne(documentCount, "documentCount");
        DocumentSizeBytes = (int)AtLeastOne(documentSizeBytes, "documentSize");
        CategoryCardinality = (int)AtLeastOne(categoryCardinality, "categoryCardinality");
        RegionCardinality = (int)AtLeastOne(regionCardinality, "regionCardinality");
        Distribution = distribution ?? throw new AggregateScenarioException("distribution", "is required");
    }

    /// <summary>
    /// Reads the set from a scenario object with the keys <c>documentCount</c>, <c>documentSize</c>
    /// (bytes), <c>categoryCardinality</c>, <c>regionCardinality</c>, <c>distribution</c> and, for
    /// zipfian only, <c>distributionExponent</c>.
    /// </summary>
    public static AggregateDataSpec FromJson(JsonElement scenario, int seed) => new(
        seed,
        Number(scenario, "documentCount")?.GetInt64(),
        Number(scenario, "documentSize")?.GetInt32(),
        Number(scenario, "categoryCardinality")?.GetInt32(),
        Number(scenario, "regionCardinality")?.GetInt32(),
        GroupDistribution.Parse(
            scenario.TryGetProperty("distribution", out var d) && d.ValueKind == JsonValueKind.String ? d.GetString() : null,
            Number(scenario, "distributionExponent")?.GetDouble()));

    private static JsonElement? Number(JsonElement scenario, string name)
    {
        if (scenario.TryGetProperty(name, out var value) == false || value.ValueKind == JsonValueKind.Null)
            return null;
        return value.ValueKind == JsonValueKind.Number ? value : throw new AggregateScenarioException(name, $"must be a number, got {value.ValueKind}");
    }

    private static long AtLeastOne(long? value, string name) => value switch
    {
        null => throw new AggregateScenarioException(name, "is required"),
        < 1 => throw new AggregateScenarioException(name, $"must be at least 1, got {value}"),
        _ => value.Value
    };
}

/// <summary>
/// One aggregate document. <see cref="Amount"/> is an integer, so a sum over it is exact and does
/// not depend on the order of addition. <see cref="Timestamp"/> is ISO-8601 UTC text, which orders
/// the same as the instant.
/// </summary>
public sealed record AggregateDocument(string Id, string Category, string Region, long Amount, string Timestamp, string Payload)
{
    public const string CategoryField = "category";
    public const string RegionField = "region";
    public const string AmountField = "amount";
    public const string TimestampField = "timestamp";
    public const string PayloadField = "payload";

    /// <summary>The RavenDB collection and the MongoDB collection that hold the set.</summary>
    public const string RavenDbCollection = "Aggregates";
    public const string MongoCollection = "aggregate";
}

/// <summary>The emitted set as the result records it.</summary>
public sealed record AggregateDataSetSummary(long Count, string Checksum, string Distribution, int CategoryCardinality, int RegionCardinality);

/// <summary>
/// Emits the aggregate document set from the spec. The same spec emits the same documents in the
/// same order on every call; both products load exactly this sequence.
/// </summary>
public sealed class AggregateDataSet(AggregateDataSpec spec)
{
    /// <summary>The largest integer amount a document carries.</summary>
    public const long MaxAmount = 100_000;

    private static readonly DateTime Epoch = new(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    public AggregateDataSpec Spec { get; } = spec;

    /// <summary>A group value by its 1-based rank; fixed width, so ordinal and numeric rank orders agree.</summary>
    public static string CategoryKey(int rank, int cardinality) => "c" + rank.ToString().PadLeft(cardinality.ToString().Length, '0');

    public static string RegionKey(int rank, int cardinality) => "r" + rank.ToString().PadLeft(cardinality.ToString().Length, '0');

    public IEnumerable<AggregateDocument> Generate()
    {
        var random = new Random(SeedMixer.Derive(Spec.Seed, "aggregate"));
        var categories = Spec.Distribution.Create();
        var regions = Spec.Distribution.Create();
        // The fixed fields take about 140 bytes of JSON; the payload fills the rest of the requested size.
        int payloadLength = Math.Max(0, Spec.DocumentSizeBytes - 140);
        var payload = new char[payloadLength];
        for (long i = 0; i < Spec.DocumentCount; i++)
        {
            var category = CategoryKey(categories.NextKey(random, Spec.CategoryCardinality), Spec.CategoryCardinality);
            var region = RegionKey(regions.NextKey(random, Spec.RegionCardinality), Spec.RegionCardinality);
            long amount = random.NextInt64(1, MaxAmount + 1);
            var timestamp = Epoch.AddSeconds(i).ToString("yyyy-MM-ddTHH:mm:ssZ");
            for (int c = 0; c < payload.Length; c++)
                payload[c] = (char)('a' + random.Next(26));
            yield return new AggregateDocument("aggregates/" + i, category, region, amount, timestamp, new string(payload));
        }
    }

    /// <summary>
    /// Counts the set and hashes the emitted field values in emitted order, SHA-256 over one
    /// tab-separated line per document. A product's stored form plays no part.
    /// </summary>
    public AggregateDataSetSummary Summarize() => Summarize(Generate());

    public AggregateDataSetSummary Summarize(IEnumerable<AggregateDocument> documents)
    {
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        long count = 0;
        foreach (var d in documents)
        {
            hash.AppendData(Encoding.UTF8.GetBytes($"{d.Id}\t{d.Category}\t{d.Region}\t{d.Amount}\t{d.Timestamp}\t{d.Payload}\n"));
            count++;
        }
        return new AggregateDataSetSummary(count, Convert.ToHexStringLower(hash.GetHashAndReset()), Spec.Distribution.ToString(),
            Spec.CategoryCardinality, Spec.RegionCardinality);
    }
}
