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

