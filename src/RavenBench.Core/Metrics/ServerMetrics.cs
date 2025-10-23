using System.Text.Json;
using RavenBench.Core;
using RavenBench.Core.Metrics.Snmp;
using RavenBench.Core.Transport;
using Lextm.SharpSnmpLib;

namespace RavenBench.Core.Metrics;

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

    // SNMP gauge metrics
    public double? MachineCpu { get; init; }
    public double? ProcessCpu { get; init; }
    public long? ManagedMemoryMb { get; init; }
    public long? UnmanagedMemoryMb { get; init; }
    public long? DirtyMemoryMb { get; init; }
    public double? Load1Min { get; init; }
    public double? Load5Min { get; init; }
    public double? Load15Min { get; init; }

    // SNMP rate metrics (computed from counters)
    public double? SnmpIoReadOpsPerSec { get; init; }
    public double? SnmpIoWriteOpsPerSec { get; init; }
    public double? SnmpIoReadBytesPerSec { get; init; }
    public double? SnmpIoWriteBytesPerSec { get; init; }
    public double? ServerSnmpRequestsPerSec { get; init; }
    public double? SnmpErrorsPerSec { get; init; }

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
    private readonly Transport.ITransport _transport;
    private readonly RunOptions _options;
    private readonly Timer _timer;
    private readonly object _lock = new();
    private readonly SnmpCounterCache _counterCache = new();
    private readonly List<ServerMetrics> _metricsHistory = new();

    private ServerMetrics _currentMetrics = new();
    private bool _isRunning;

    public ServerMetricsTracker(Transport.ITransport transport, RunOptions options)
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
            var intervalMs = (int)_options.Snmp.PollInterval.TotalMilliseconds;
            _timer.Change(0, intervalMs);
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

            if (_options.Snmp.Enabled)
            {
                var snmpSample = await _transport.GetSnmpMetricsAsync(_options.Snmp, _options.Database);
                var snmpRates = _counterCache.ComputeRates(snmpSample);

                // If we have rates (not the first sample), merge them into metrics
                if (snmpRates != null)
                {
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

                        // SNMP gauge metrics
                        MachineCpu = snmpRates.MachineCpu,
                        ProcessCpu = snmpRates.ProcessCpu,
                        ManagedMemoryMb = snmpRates.ManagedMemoryMb,
                        UnmanagedMemoryMb = snmpRates.UnmanagedMemoryMb,
                        DirtyMemoryMb = snmpRates.DirtyMemoryMb,
                        Load1Min = snmpRates.Load1Min,
                        Load5Min = snmpRates.Load5Min,
                        Load15Min = snmpRates.Load15Min,

                        // SNMP rate metrics
                        SnmpIoReadOpsPerSec = snmpRates.IoReadOpsPerSec,
                        SnmpIoWriteOpsPerSec = snmpRates.IoWriteOpsPerSec,
                        SnmpIoReadBytesPerSec = snmpRates.IoReadBytesPerSec,
                        SnmpIoWriteBytesPerSec = snmpRates.IoWriteBytesPerSec,
                        ServerSnmpRequestsPerSec = snmpRates.ServerRequestsPerSec,
                        SnmpErrorsPerSec = snmpRates.ErrorsPerSec,

                        Timestamp = metrics.Timestamp,
                        IsValid = metrics.IsValid,
                        ErrorMessage = metrics.ErrorMessage
                    };
                }
            }

            lock (_lock)
            {
                _currentMetrics = metrics;

                // Store in history if SNMP is enabled (to avoid storing empty metrics)
                if (_options.Snmp.Enabled && metrics.IsValid)
                {
                    _metricsHistory.Add(metrics);
                }
            }
        }
        catch
        {
            // Silently continue on failure - server metrics are supplementary
        }
    }

    public List<ServerMetrics> GetHistory()
    {
        lock (_lock)
        {
            return new List<ServerMetrics>(_metricsHistory);
        }
    }

    public void Dispose()
    {
        Stop();
        _timer.Dispose();
        // Note: Don't dispose _transport - it's owned by the caller
    }
}
