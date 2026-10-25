using System.Globalization;
using System.Net.Http;
using System.Text.Json;
using System.Collections.Concurrent;
using Raven.Client.Documents;
using RavenBench.Core;

namespace RavenBench.Core.Metrics;

/// <summary>
/// Collects server metrics from RavenDB admin endpoints using the client's authenticated HttpClient;
/// a plain HttpClient is rejected by the admin endpoints with 400 Bad Request.
/// </summary>
public static class RavenServerMetricsCollector
{
    // Stores are cached for the process lifetime; callers share one store per endpoint.
    private static readonly ConcurrentDictionary<string, Lazy<DocumentStore>> _stores = new();
    // A server's core count does not change while it runs, so it is read once per endpoint.
    private static readonly ConcurrentDictionary<string, int> _serverCores = new();

    /// <summary>
    /// Reads the server's cumulative process CPU time and its core count.
    /// The CPU percent is left to the caller, which averages it over a window of its own.
    /// </summary>
    public static async Task<ServerMetrics> CollectAsync(string baseUrl, string database, string? httpVersion = null)
    {
        try
        {
            var store = GetStore(baseUrl, database, httpVersion);
            var httpClient = store.GetRequestExecutor().HttpClient;

            var cpuStatsTask = httpClient.GetStringAsync($"{baseUrl}/admin/debug/cpu/stats");
            var coresTask = GetServerCoresAsync(httpClient, baseUrl);

            await Task.WhenAll(cpuStatsTask, coresTask);

            return Parse(await cpuStatsTask, await coresTask);
        }
        catch (Exception ex)
        {
            return new ServerMetrics
            {
                IsValid = false,
                ErrorMessage = $"Server metrics collection failed: {ex.Message}"
            };
        }
    }

    /// <summary>
    /// Reads the server's working set; null when the read fails or the server does not report it.
    /// The memory endpoint walks the page tables of the server process, which stalls the server's
    /// requests while it runs, so a caller reads it outside a measurement window.
    /// </summary>
    public static async Task<long?> CollectMemoryMBAsync(string baseUrl, string database, string? httpVersion = null)
    {
        try
        {
            var httpClient = GetStore(baseUrl, database, httpVersion).GetRequestExecutor().HttpClient;
            return ParseMemoryMB(await httpClient.GetStringAsync($"{baseUrl}/admin/debug/memory/stats"));
        }
        catch (Exception)
        {
            // Server metrics are supplementary, as for the CPU poll: a failed read leaves the memory figure unknown.
            return null;
        }
    }

    /// <summary>Builds a sample from the body of the CPU debug endpoint.</summary>
    internal static ServerMetrics Parse(string cpuStatsJson, int? serverCores)
    {
        var cpuStats = JsonSerializer.Deserialize<CpuStatsResult>(cpuStatsJson);
        return new ServerMetrics
        {
            ServerProcessorTime = ParseProcessorTime(cpuStats?.CpuStats?.FirstOrDefault()?.TotalProcessorTime),
            ServerCores = serverCores
        };
    }

    /// <summary>The working set in the body of the memory debug endpoint.</summary>
    internal static long? ParseMemoryMB(string memoryStatsJson) =>
        ExtractMemoryMB(JsonSerializer.Deserialize<MemoryStatsResult>(memoryStatsJson)?.MemoryInformation?.WorkingSet);

    private static DocumentStore GetStore(string baseUrl, string database, string? httpVersion)
    {
        var key = $"{baseUrl}|{database}|{httpVersion}";
        return _stores.GetOrAdd(key, _ => new Lazy<DocumentStore>(
            () => HttpHelper.CreateFromVersionString(baseUrl, database, httpVersion))).Value;
    }

    // The core count the server process sees; null when the server does not report it.
    private static async Task<int?> GetServerCoresAsync(HttpClient httpClient, string baseUrl)
    {
        if (_serverCores.TryGetValue(baseUrl, out var cached))
            return cached;

        try
        {
            var response = await httpClient.GetStringAsync($"{baseUrl}/cluster/node-info");
            using var doc = JsonDocument.Parse(response);
            if (doc.RootElement.TryGetProperty("NumberOfCores", out var cores) && cores.TryGetInt32(out var count) && count > 0)
                return _serverCores[baseUrl] = count;
        }
        catch (Exception ex) when (ex is HttpRequestException or JsonException)
        {
            // An unknown core count leaves the server CPU figure unknown; the CPU time is still valid.
        }
        return null;
    }

    // Null when the working set is missing, unparseable or in an unknown unit.
    internal static long? ExtractMemoryMB(string? workingSetString)
    {
        if (string.IsNullOrEmpty(workingSetString))
            return null;

        // Parses strings like "3.231 GBytes" or "512.5 MBytes".
        var parts = workingSetString.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length >= 2 && double.TryParse(parts[0], NumberStyles.Float, CultureInfo.InvariantCulture, out var value))
        {
            return parts[1].ToLowerInvariant() switch
            {
                "gbytes" => (long)(value * 1024),
                "mbytes" => (long)value,
                "kbytes" => (long)(value / 1024),
                "bytes" => (long)(value / (1024 * 1024)),
                _ => null
            };
        }
        return null;
    }

    private static TimeSpan? ParseProcessorTime(string? totalProcessorTime) =>
        TimeSpan.TryParse(totalProcessorTime, CultureInfo.InvariantCulture, out var value) ? value : null;
}

internal sealed class MemoryStatsResult
{
    public MemoryInformation? MemoryInformation { get; set; }
}

internal sealed class MemoryInformation
{
    public string? WorkingSet { get; set; }
}

internal sealed class CpuStatsResult
{
    public CpuStatEntry[]? CpuStats { get; set; }
}

internal sealed class CpuStatEntry
{
    public string? TotalProcessorTime { get; set; }
}
