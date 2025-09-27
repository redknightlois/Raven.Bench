using System.Text.Json;
using RavenBench.Util;
using RavenBench.Metrics.Snmp;
using Lextm.SharpSnmpLib;

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

    public double? MachineCpu { get; init; }
    public double? ProcessCpu { get; init; }
    public long? ManagedMemoryMb { get; init; }
    public long? UnmanagedMemoryMb { get; init; }
    public DateTime Timestamp { get; init; } = DateTime.UtcNow;
    public bool IsValid { get; init; } = true;
    public string? ErrorMessage { get; init; }
}

/// <summary>
/// Polls server-side metrics from RavenDB endpoints during benchmark execution.
/// Follows ProcessCpuTracker pattern - provides synchronous access to metrics without async complexity.
/// </summary>
public sealed class ServerMetricsTracker : IDisposable
{
    private readonly RavenBench.Transport.ITransport _transport;
    private readonly RunOptions _options;
    private readonly Timer _timer;
    private readonly object _lock = new();

    private ServerMetrics _currentMetrics = new();
    private bool _isRunning;

    public ServerMetricsTracker(RavenBench.Transport.ITransport transport, RunOptions options)
    {
        _transport = transport;
        _options = options;
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
        if (_isRunning == false) return;

        try
        {
            var metrics = await _transport.GetServerMetricsAsync();

            if (_options.SnmpEnabled)
            {
                var (machineCpu, processCpu, managedMemoryMb, unmanagedMemoryMb) = await _transport.GetSnmpMetricsAsync();

                metrics = new ServerMetrics
                {
                    CpuUsagePercent = metrics.CpuUsagePercent,
                    MemoryUsageMB = metrics.MemoryUsageMB,
                    ActiveConnections = metrics.ActiveConnections,
                    RequestsPerSecond = metrics.RequestsPerSecond,
                    QueuedRequests = metrics.QueuedRequests,
                    IoReadOperations = metrics.IoReadOperations,
                    IoWriteOperations = metrics.IoWriteOperations,
                    ReadThroughputKb = metrics.ReadThroughputKb,
                    WriteThroughputKb = metrics.WriteThroughputKb,
                    QueueLength = metrics.QueueLength,
                    MachineCpu = machineCpu,
                    ProcessCpu = processCpu,
                    ManagedMemoryMb = managedMemoryMb,
                    UnmanagedMemoryMb = unmanagedMemoryMb,
                    Timestamp = metrics.Timestamp,
                    IsValid = metrics.IsValid,
                    ErrorMessage = metrics.ErrorMessage
                };
            }

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
