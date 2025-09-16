using System.Net.Http;
using System.Text.Json;
using System.Collections.Concurrent;
using System.Diagnostics;
using Raven.Client.Documents;
using RavenBench.Util;

namespace RavenBench.Metrics;

/// <summary>
/// Shared implementation for collecting server metrics from RavenDB admin endpoints.
/// Uses RavenDB client's authenticated HttpClient to avoid 400 Bad Request errors.
/// </summary>
public static class RavenServerMetricsCollector
{
    private static readonly ConcurrentDictionary<string, CpuSample> _previousCpuSamples = new();
    private static readonly ConcurrentDictionary<string, IoSample> _previousIoSamples = new();

    public static async Task<ServerMetrics> CollectAsync(string baseUrl, string database, string? httpVersion = null)
    {
        try
        {
            // Use a temporary RavenDB client to get properly configured HttpClient for admin endpoints
            using var tempStore = new DocumentStore
            {
                Urls = new[] { baseUrl },
                Database = database
            };

            // Configure HTTP version if specified
            if (!string.IsNullOrEmpty(httpVersion))
            {
                var normalizedVersion = HttpHelper.NormalizeHttpVersion(httpVersion);
                var httpVersionInfo = HttpHelper.GetRequestVersionInfo(normalizedVersion);

                tempStore.Conventions.CreateHttpClient = (handler) =>
                {
                    var client = new HttpClient(new HttpHelper.HttpVersionHandler(handler, httpVersionInfo))
                    {
                        Timeout = Timeout.InfiniteTimeSpan
                    };
                    return client;
                };
            }

            tempStore.Initialize();
            var httpClient = tempStore.GetRequestExecutor().HttpClient;
            
            // Collect server metrics from correct endpoints using authenticated client
            var memoryStatsTask = GetMemoryStatsAsync(httpClient, baseUrl);
            var cpuStatsTask = GetCpuStatsAsync(httpClient, baseUrl);
            var ioStatsTask = GetIoStatsAsync(httpClient, baseUrl);
            
            await Task.WhenAll(memoryStatsTask, cpuStatsTask, ioStatsTask);
            
            var memoryStats = await memoryStatsTask;
            var cpuStats = await cpuStatsTask;
            var ioStats = await ioStatsTask;

            // Extract memory usage from working set
            var memoryMB = ExtractMemoryMB(memoryStats.MemoryInformation?.WorkingSet);
            
            // Calculate CPU usage percentage
            var cpuUsagePercent = CalculateCpuUsagePercent(cpuStats, baseUrl);
            
            // Calculate IO metrics
            var ioMetrics = CalculateIoMetrics(ioStats, baseUrl);

            return new ServerMetrics
            {
                CpuUsagePercent = cpuUsagePercent,
                MemoryUsageMB = memoryMB,
                ActiveConnections = null, // admin/connections might need auth - not available
                RequestsPerSecond = null, // Not available in these endpoints
                IoReadOperations = ioMetrics.ReadOperationsPerSec,
                IoWriteOperations = ioMetrics.WriteOperationsPerSec,
                ReadThroughputKb = ioMetrics.ReadThroughputKb,
                WriteThroughputKb = ioMetrics.WriteThroughputKb,
                QueueLength = null // Not available in current format
            };
        }
        catch (Exception ex)
        {
            // Return metrics with null values and error information
            return new ServerMetrics
            {
                IsValid = false,
                ErrorMessage = $"Server metrics collection failed: {ex.Message}"
            };
        }
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

        // Parse strings like "3.231 GBytes", "512.5 MBytes", etc.
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
        // Get current CPU timing data
        var currentSample = ExtractCpuSample(cpuStats);
        if (currentSample == null)
            return null;

        // Get previous sample for this server
        var previousSample = _previousCpuSamples.GetValueOrDefault(serverKey);
        
        // Store current sample for next calculation
        _previousCpuSamples[serverKey] = currentSample;

        // Need previous sample to calculate CPU usage
        if (previousSample == null)
            return null;

        // Calculate CPU usage percentage
        var totalProcessorTimeDiff = currentSample.TotalProcessorTime - previousSample.TotalProcessorTime;
        var timeDiff = currentSample.Timestamp - previousSample.Timestamp;

        if (timeDiff.TotalMilliseconds <= 0 || totalProcessorTimeDiff.TotalMilliseconds < 0)
            return null;

        // Calculate percentage based on elapsed time and available CPU time
        // CPU percentage = (processor time used / total available CPU time) * 100
        var availableCpuTime = timeDiff.TotalMilliseconds * Environment.ProcessorCount;
        var cpuUsagePercent = (totalProcessorTimeDiff.TotalMilliseconds / availableCpuTime) * 100.0;

        // Clamp to reasonable range
        return Math.Max(0, Math.Min(100, cpuUsagePercent));
    }

    private static CpuSample? ExtractCpuSample(CpuStatsResult cpuStats)
    {
        var cpuStatEntry = cpuStats.CpuStats?.FirstOrDefault();
        if (cpuStatEntry?.TotalProcessorTime == null)
            return null;

        // Parse TimeSpan from string format (e.g., "00:57:11.9531250")
        if (!TimeSpan.TryParse(cpuStatEntry.TotalProcessorTime, out var totalProcessorTime))
            return null;

        return new CpuSample
        {
            TotalProcessorTime = totalProcessorTime,
            Timestamp = DateTime.UtcNow
        };
    }

    private static async Task<IoMetricsResponse> GetIoStatsAsync(HttpClient httpClient, string baseUrl)
    {
        try
        {
            var response = await httpClient.GetStringAsync($"{baseUrl}/admin/debug/io-metrics");
            
            using var doc = JsonDocument.Parse(response);
            var result = doc.RootElement.Deserialize<IoMetricsResponse>() ?? new IoMetricsResponse();
            
            return result;
        }
        catch (Exception)
        {
            // Return empty result if IO metrics endpoint fails
            return new IoMetricsResponse();
        }
    }

    private static IoMetricsCalculated CalculateIoMetrics(IoMetricsResponse ioStats, string serverKey)
    {
        try
        {
            // Get current IO metrics data - now calculated directly from recent operations
            var currentSample = ExtractIoSample(ioStats);
            if (currentSample == null)
                return new IoMetricsCalculated();

            // Return metrics directly since we calculated rates in ExtractIoSample
            return new IoMetricsCalculated
            {
                ReadOperationsPerSec = currentSample.ReadOperations > 0 ? (double)currentSample.ReadOperations : null,
                WriteOperationsPerSec = currentSample.WriteOperations > 0 ? (double)currentSample.WriteOperations : null,
                ReadThroughputKb = currentSample.ReadBytes > 0 ? currentSample.ReadBytes / 1024 : null,
                WriteThroughputKb = currentSample.WriteBytes > 0 ? currentSample.WriteBytes / 1024 : null
            };
        }
        catch (Exception)
        {
            return new IoMetricsCalculated();
        }
    }

    private static IoSample? ExtractIoSample(IoMetricsResponse ioStats)
    {
        if (ioStats?.Environments == null)
            return null;

        // Simple approach: take last 10 operations and calculate rates
        int readOpsCount = 0;
        int writeOpsCount = 0;
        long readBytesTotal = 0;
        long writeBytesTotal = 0;
        double totalDuration = 0;

        foreach (var environment in ioStats.Environments)
        {
            if (environment?.Files == null) continue;

            foreach (var file in environment.Files)
            {
                if (file?.Recent == null) continue;

                // Take the most recent 10 operations for rate calculation
                var recentOperations = file.Recent.TakeLast(10);

                foreach (var recent in recentOperations)
                {
                    if (recent?.Type == null || recent.Size == null) continue;

                    switch (recent.Type)
                    {
                        case "DataFlush":
                        case "DataSync":
                        case "JournalWrite":
                            writeOpsCount++;
                            writeBytesTotal += recent.Size.Value;
                            totalDuration += recent.Duration ?? 0;
                            break;
                        case "Compression":
                            readOpsCount++;
                            readBytesTotal += recent.Size.Value;
                            totalDuration += recent.Duration ?? 0;
                            break;
                    }
                }
            }
        }

        // Convert to per-second rates
        if (totalDuration > 0)
        {
            var durationSeconds = totalDuration / 1000.0;
            var readOpsPerSec = readOpsCount / durationSeconds;
            var writeOpsPerSec = writeOpsCount / durationSeconds;
            var readBytesPerSec = readBytesTotal / durationSeconds;
            var writeBytesPerSec = writeBytesTotal / durationSeconds;

            return new IoSample
            {
                ReadOperations = (long)readOpsPerSec,
                WriteOperations = (long)writeOpsPerSec,
                ReadBytes = (long)readBytesPerSec,
                WriteBytes = (long)writeBytesPerSec,
                Timestamp = DateTime.UtcNow
            };
        }

        return new IoSample
        {
            ReadOperations = 0,
            WriteOperations = 0,
            ReadBytes = 0,
            WriteBytes = 0,
            Timestamp = DateTime.UtcNow
        };
    }
}


/// <summary>
/// Represents memory statistics from RavenDB's /admin/debug/memory/stats endpoint.
/// </summary>
internal sealed class MemoryStatsResult
{
    public MemoryInformation? MemoryInformation { get; set; }
}

internal sealed class MemoryInformation
{
    public string? WorkingSet { get; set; }
}

/// <summary>
/// Represents CPU statistics from RavenDB's /admin/debug/cpu/stats endpoint.
/// </summary>
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

/// <summary>
/// Represents a CPU sample for calculating usage percentage.
/// </summary>
internal sealed class CpuSample
{
    public TimeSpan TotalProcessorTime { get; set; }
    public DateTime Timestamp { get; set; }
}

/// <summary>
/// Represents IO statistics from RavenDB's /admin/debug/io-metrics endpoint.
/// Matches the actual JSON structure from RavenDB source.
/// </summary>
internal sealed class IoMetricsResponse
{
    public IoEnvironmentMetrics[]? Environments { get; set; }
    public object[]? Performances { get; set; }
}

internal sealed class IoEnvironmentMetrics
{
    public string? Path { get; set; }
    public string? Type { get; set; }
    public IoFileMetrics[]? Files { get; set; }
}

internal sealed class IoFileMetrics
{
    public string? File { get; set; }
    public string? Status { get; set; }
    public IoRecentMetrics[]? Recent { get; set; }
    public object[]? History { get; set; }
}

internal sealed class IoRecentMetrics
{
    public string? Start { get; set; }
    public long? Size { get; set; }
    public string? Type { get; set; }
    public double? Duration { get; set; }
    public string? HumaneSize { get; set; }
    public long? FileSize { get; set; }
    public string? HumaneFileSize { get; set; }
}

/// <summary>
/// Represents an IO sample for calculating rates.
/// </summary>
internal sealed class IoSample
{
    public long ReadOperations { get; set; }
    public long WriteOperations { get; set; }
    public long ReadBytes { get; set; }
    public long WriteBytes { get; set; }
    public DateTime Timestamp { get; set; }
}

/// <summary>
/// Represents calculated IO metrics rates.
/// </summary>
internal sealed class IoMetricsCalculated
{
    public double? ReadOperationsPerSec { get; set; }
    public double? WriteOperationsPerSec { get; set; }
    public long? ReadThroughputKb { get; set; }
    public long? WriteThroughputKb { get; set; }
}