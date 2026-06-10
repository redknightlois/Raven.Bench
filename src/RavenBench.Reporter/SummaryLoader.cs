using System.Text.Json;
using System.Text.Json.Serialization;
using RavenBench.Core.Reporting;

namespace RavenBench.Reporter;

/// <summary>
/// Loads benchmark summary from JSON file.
/// </summary>
public static class SummaryLoader
{
    public const int ExpectedSchemaVersion = 1;

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        Converters = { new JsonStringEnumConverter() }
    };

    /// <summary>
    /// Loads a benchmark summary from the specified JSON file path.
    /// </summary>
    /// <param name="summaryPath">The path to the summary JSON file.</param>
    /// <returns>The loaded benchmark summary.</returns>
    /// <exception cref="FileNotFoundException">Thrown if the file does not exist.</exception>
    /// <exception cref="JsonException">Thrown if the JSON is invalid.</exception>
    /// <exception cref="InvalidDataException">Thrown if the schema version is missing or unsupported.</exception>
    public static async Task<BenchmarkSummary> LoadAsync(string summaryPath)
    {
        if (File.Exists(summaryPath) == false)
            throw new FileNotFoundException($"Summary file not found: {summaryPath}");

        await using var stream = File.OpenRead(summaryPath);
        using var document = await JsonDocument.ParseAsync(stream);

        int schemaVersion = ReadSchemaVersion(document.RootElement);
        if (schemaVersion != ExpectedSchemaVersion)
            throw new InvalidDataException(
                $"Summary file '{summaryPath}' has schema version {schemaVersion} but this reporter expects {ExpectedSchemaVersion}. " +
                "Regenerate the summary with a matching RavenBench version (run with --out-json).");

        var summary = document.RootElement.Deserialize<BenchmarkSummary>(JsonOptions);
        return summary ?? throw new JsonException("Failed to deserialize benchmark summary.");
    }

    private static int ReadSchemaVersion(JsonElement root)
    {
        if (root.ValueKind != JsonValueKind.Object)
            return 0;

        foreach (var property in root.EnumerateObject())
        {
            if (string.Equals(property.Name, "SchemaVersion", StringComparison.OrdinalIgnoreCase) &&
                property.Value.ValueKind == JsonValueKind.Number)
            {
                return property.Value.GetInt32();
            }
        }

        return 0;
    }
}
