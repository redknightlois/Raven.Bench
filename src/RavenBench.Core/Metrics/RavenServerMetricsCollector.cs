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
    private static readonly ConcurrentDictionary<string, CpuSample> _previousCpuSamples = new();
    // Stores are cached for the process lifetime; callers share one store per endpoint.
    private static readonly ConcurrentDictionary<string, Lazy<DocumentStore>> _stores = new();

    public static async Task<ServerMetrics> CollectAsync(string baseUrl, string database, string? httpVersion = null)
    {
        try
        {
            var store = GetStore(baseUrl, database, httpVersion);
            var httpClient = store.GetRequestExecutor().HttpClient;

            var memoryStatsTask = GetMemoryStatsAsync(httpClient, baseUrl);
            var cpuStatsTask = GetCpuStatsAsync(httpClient, baseUrl);

            await Task.WhenAll(memoryStatsTask, cpuStatsTask);

            var memoryStats = await memoryStatsTask;
            var cpuStats = await cpuStatsTask;

            return new ServerMetrics
            {
                CpuUsagePercent = CalculateCpuUsagePercent(cpuStats, baseUrl),
                MemoryUsageMB = ExtractMemoryMB(memoryStats.MemoryInformation?.WorkingSet)
            };
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

    private static DocumentStore GetStore(string baseUrl, string database, string? httpVersion)
    {
        var key = $"{baseUrl}|{database}|{httpVersion}";
        return _stores.GetOrAdd(key, _ => new Lazy<DocumentStore>(() =>
        {
            var store = new DocumentStore
            {
                Urls = new[] { baseUrl },
                Database = database
            };

            if (string.IsNullOrEmpty(httpVersion) == false)
            {
                HttpHelper.ConfigureHttpVersion(store, httpVersion);
            }

            store.Initialize();
            return store;
        })).Value;
    }

    private static async Task<MemoryStatsResult> GetMemoryStatsAsync(HttpClient httpClient, string baseUrl)
    {
        var response = await httpClient.GetStringAsync($"{baseUrl}/admin/debug/memory/stats");
        using var doc = JsonDocument.Parse(response);
        return doc.RootElement.Deserialize<MemoryStatsResult>() ?? new MemoryStatsResult();
    }

    private static long ExtractMemoryMB(string? workingSetString)
    {
        if (string.IsNullOrEmpty(workingSetString))
            return 0;

        // Parses strings like "3.231 GBytes" or "512.5 MBytes".
        var parts = workingSetString.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length >= 2 && double.TryParse(parts[0], out var value))
        {
            return parts[1].ToLowerInvariant() switch
            {
                "gbytes" => (long)(value * 1024),
                "mbytes" => (long)value,
                "kbytes" => (long)(value / 1024),
                "bytes" => (long)(value / (1024 * 1024)),
                _ => (long)value
            };
        }
        return 0;
    }

    private static async Task<CpuStatsResult> GetCpuStatsAsync(HttpClient httpClient, string baseUrl)
    {
        var response = await httpClient.GetStringAsync($"{baseUrl}/admin/debug/cpu/stats");
        using var doc = JsonDocument.Parse(response);
        return doc.RootElement.Deserialize<CpuStatsResult>() ?? new CpuStatsResult();
    }

    private static double? CalculateCpuUsagePercent(CpuStatsResult cpuStats, string serverKey)
    {
        var currentSample = ExtractCpuSample(cpuStats);
        if (currentSample == null)
            return null;

        var previousSample = _previousCpuSamples.GetValueOrDefault(serverKey);
        _previousCpuSamples[serverKey] = currentSample;

        if (previousSample == null)
            return null;

        var totalProcessorTimeDiff = currentSample.TotalProcessorTime - previousSample.TotalProcessorTime;
        var timeDiff = currentSample.Timestamp - previousSample.Timestamp;

        if (timeDiff.TotalMilliseconds <= 0 || totalProcessorTimeDiff.TotalMilliseconds < 0)
            return null;

        var availableCpuTime = timeDiff.TotalMilliseconds * Environment.ProcessorCount;
        var cpuUsagePercent = (totalProcessorTimeDiff.TotalMilliseconds / availableCpuTime) * 100.0;

        return Math.Max(0, Math.Min(100, cpuUsagePercent));
    }

    private static CpuSample? ExtractCpuSample(CpuStatsResult cpuStats)
    {
        var cpuStatEntry = cpuStats.CpuStats?.FirstOrDefault();
        if (cpuStatEntry?.TotalProcessorTime == null)
            return null;

        if (TimeSpan.TryParse(cpuStatEntry.TotalProcessorTime, out var totalProcessorTime) == false)
            return null;

        return new CpuSample
        {
            TotalProcessorTime = totalProcessorTime,
            Timestamp = DateTime.UtcNow
        };
    }
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
    public string? ProcessName { get; set; }
    public string? TotalProcessorTime { get; set; }
    public string? UserProcessorTime { get; set; }
    public string? PrivilegedProcessorTime { get; set; }
}

internal sealed class CpuSample
{
    public TimeSpan TotalProcessorTime { get; set; }
    public DateTime Timestamp { get; set; }
}
