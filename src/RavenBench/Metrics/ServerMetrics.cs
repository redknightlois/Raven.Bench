using System.Text.Json;

namespace RavenBench.Metrics;

/// <summary>
/// Represents server-side metrics collected from RavenDB endpoints.
/// Complements client-side metrics with server perspective on performance.
/// </summary>
public sealed class ServerMetrics
{
    public double? CpuUsagePercent { get; init; }
    public long? MemoryUsageMB { get; init; }
    public int? ActiveConnections { get; init; }
    public double? RequestsPerSecond { get; init; }
    public int? QueuedRequests { get; init; }
    public double? IoReadOperations { get; init; }
    public double? IoWriteOperations { get; init; }
    public long? ReadThroughputKb { get; init; }
    public long? WriteThroughputKb { get; init; }
    public long? QueueLength { get; init; }
    public DateTime Timestamp { get; init; } = DateTime.UtcNow;
    public bool IsValid { get; init; } = true;
    public string? ErrorMessage { get; init; }
}

// REVIEW: Not very fond on the way this is going. I would like to have something similar to ProcessCpuTracker
// in which I can start a polling mechanism that will check these numbers on intervals and allow me to get the 
// actual data and hide the async complication from the analysis code. 

/// <summary>
/// Collects server-side metrics from RavenDB administrative endpoints.
/// Provides server perspective to complement client-side benchmark metrics.
/// </summary>
public sealed class ServerMetricsCollector
{
    private readonly string _baseUrl;
    private readonly HttpClient _httpClient;

    public ServerMetricsCollector(string serverUrl)
    {
        _baseUrl = serverUrl.TrimEnd('/');
        _httpClient = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };
    }

    /// <summary>
    /// Collects server metrics from RavenDB admin endpoints.
    /// Throws exceptions if collection fails - caller should handle appropriately.
    /// </summary>
    public async Task<ServerMetrics> CollectAsync()
    {
        // Collect various server metrics from different endpoints
        var cpuTask = GetCpuUsageAsync();
        var memoryTask = GetMemoryUsageAsync();
        var connectionsTask = GetConnectionCountAsync();
        var requestsTask = GetRequestRateAsync();
        var ioTask = GetIoMetricsAsync();

        await Task.WhenAll(cpuTask, memoryTask, connectionsTask, requestsTask, ioTask);

        var ioMetrics = await ioTask;
        return new ServerMetrics
        {
            CpuUsagePercent = await cpuTask,
            MemoryUsageMB = await memoryTask,
            ActiveConnections = await connectionsTask,
            RequestsPerSecond = await requestsTask,
            QueuedRequests = 0, // Would need specific endpoint
            IoReadOperations = ioMetrics.IoReadOperations,
            IoWriteOperations = ioMetrics.IoWriteOperations,
            ReadThroughputKb = ioMetrics.ReadThroughputInKb,
            WriteThroughputKb = ioMetrics.WriteThroughputInKb,
            QueueLength = ioMetrics.QueueLength
        };
    }

    private async Task<double> GetCpuUsageAsync()
    {
        // RavenDB typically exposes metrics at /admin/stats endpoint
        var response = await _httpClient.GetStringAsync($"{_baseUrl}/admin/stats");
        
        // Parse CPU usage - this is a simplified example
        // Real implementation would need to parse actual RavenDB metrics format
        if (response.Contains("\"CpuUsage\":"))
        {
            var startIdx = response.IndexOf("\"CpuUsage\":", StringComparison.Ordinal) + 11;
            var endIdx = response.IndexOf(',', startIdx);
            if (endIdx == -1) endIdx = response.IndexOf('}', startIdx);
            
            if (double.TryParse(response.AsSpan(startIdx, endIdx - startIdx), out var cpu))
                return cpu;
        }
        return 0; // Default if not found
    }

    private async Task<long> GetMemoryUsageAsync()
    {
        var response = await _httpClient.GetStringAsync($"{_baseUrl}/admin/stats");
        
        // Parse memory usage - simplified example
        if (response.Contains("\"MemoryUsage\":"))
        {
            var startIdx = response.IndexOf("\"MemoryUsage\":", StringComparison.Ordinal) + 14;
            var endIdx = response.IndexOf(',', startIdx);
            if (endIdx == -1) endIdx = response.IndexOf('}', startIdx);
            
            if (long.TryParse(response.AsSpan(startIdx, endIdx - startIdx), out var memory))
                return memory / (1024 * 1024); // Convert to MB
        }
        return 0; // Default if not found
    }

    private async Task<int> GetConnectionCountAsync()
    {
        var response = await _httpClient.GetStringAsync($"{_baseUrl}/admin/connections");
        
        // Parse connection count - simplified example
        if (response.Contains("\"Count\":"))
        {
            var startIdx = response.IndexOf("\"Count\":", StringComparison.Ordinal) + 8;
            var endIdx = response.IndexOf(',', startIdx);
            if (endIdx == -1) endIdx = response.IndexOf('}', startIdx);
            
            if (int.TryParse(response.AsSpan(startIdx, endIdx - startIdx), out var count))
                return count;
        }
        return 0; // Default if not found
    }

    private async Task<double> GetRequestRateAsync()
    {
        var response = await _httpClient.GetStringAsync($"{_baseUrl}/admin/stats");
        
        // Parse requests per second - simplified example
        if (response.Contains("\"RequestsPerSec\":"))
        {
            var startIdx = response.IndexOf("\"RequestsPerSec\":", StringComparison.Ordinal) + 17;
            var endIdx = response.IndexOf(',', startIdx);
            if (endIdx == -1) endIdx = response.IndexOf('}', startIdx);
            
            if (double.TryParse(response.AsSpan(startIdx, endIdx - startIdx), out var rps))
                return rps;
        }
        return 0; // Default if not found
    }

    private async Task<IoStatsResult> GetIoMetricsAsync()
    {
        var response = await _httpClient.GetStringAsync($"{_baseUrl}/admin/debug/io-metrics");
        using var doc = JsonDocument.Parse(response);
        
        // Find first IoStatsResult in any environment
        var ioStatsElement = FindFirstIoStats(doc.RootElement);
        if (ioStatsElement.HasValue)
        {
            return ioStatsElement.Value.Deserialize<IoStatsResult>() ?? new IoStatsResult();
        }
        
        return new IoStatsResult();
    }
    
    private static JsonElement? FindFirstIoStats(JsonElement root)
    {
        if (root.TryGetProperty("Environments", out var environments))
        {
            foreach (var env in environments.EnumerateArray())
            {
                if (env.TryGetProperty("Files", out var files))
                {
                    foreach (var file in files.EnumerateArray())
                    {
                        if (file.TryGetProperty("IoStatsResult", out var ioStats))
                            return ioStats;
                    }
                }
            }
        }
        return null;
    }

    public void Dispose()
    {
        _httpClient.Dispose();
    }
}

/// <summary>
/// Polls server-side metrics from RavenDB endpoints during benchmark execution.
/// Similar to ProcessCpuTracker - provides synchronous access to metrics without async complexity.
/// </summary>
public sealed class ServerMetricsTracker : IDisposable
{
    private readonly RavenBench.Transport.ITransport _transport;
    private readonly Timer _timer;
    private readonly object _lock = new();
    
    private ServerMetrics _currentMetrics = new();
    private bool _isRunning;

    public ServerMetricsTracker(RavenBench.Transport.ITransport transport)
    {
        _transport = transport;
        _timer = new Timer(PollMetrics, null, Timeout.Infinite, Timeout.Infinite);
    }

    public void Start()
    {
        lock (_lock)
        {
            _isRunning = true;
            _timer.Change(0, 2000); // Poll every 2 seconds
        }
    }

    public void Stop()
    {
        lock (_lock)
        {
            _isRunning = false;
            _timer.Change(Timeout.Infinite, Timeout.Infinite);
        }
    }

    public ServerMetrics Current
    {
        get
        {
            lock (_lock)
            {
                return _currentMetrics;
            }
        }
    }

    private async void PollMetrics(object? state)
    {
        if (!_isRunning) return;
        
        try
        {
            var metrics = await _transport.GetServerMetricsAsync();
            lock (_lock)
            {
                _currentMetrics = metrics;
            }
        }
        catch
        {
            // Silently continue on failure - server metrics are supplementary
        }
    }

    public void Dispose()
    {
        Stop();
        _timer.Dispose();
        // Note: Don't dispose _transport - it's owned by the caller
    }
}

/// <summary>
/// Represents IO statistics result structure matching RavenDB's IoStatsResult.
/// </summary>
internal sealed class IoStatsResult
{
    public double IoReadOperations { get; set; }
    public double IoWriteOperations { get; set; }
    public long ReadThroughputInKb { get; set; }
    public long WriteThroughputInKb { get; set; }
    public long? QueueLength { get; set; }
}