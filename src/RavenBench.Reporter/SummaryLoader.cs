using System.Text.Json;
using System.Text.Json.Serialization;
using RavenBench.Core.Reporting;

namespace RavenBench.Reporter;

/// <summary>
/// Loads benchmark summary from JSON file.
/// </summary>
public static class SummaryLoader
{
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
    public static async Task<BenchmarkSummary> LoadAsync(string summaryPath)
    {
        if (!File.Exists(summaryPath))
            throw new FileNotFoundException($"Summary file not found: {summaryPath}");

        await using var stream = File.OpenRead(summaryPath);
        var summary = await JsonSerializer.DeserializeAsync<BenchmarkSummary>(stream, JsonOptions);
        return summary ?? throw new JsonException("Failed to deserialize benchmark summary.");
    }
}