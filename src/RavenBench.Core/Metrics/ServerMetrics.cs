using RavenBench.Core;
using RavenBench.Core.Metrics.Snmp;

namespace RavenBench.Core.Metrics;

/// <summary>
/// Represents server-side metrics collected from RavenDB endpoints.
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
            _timer.Change(0, Timeout.Infinite);
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

    // One-shot timer: rescheduled after each poll completes, so polls never overlap.
    private async void PollMetrics(object? state)
    {
        try
        {
            var metrics = await _transport.GetServerMetricsAsync();

            if (_options.Snmp.Enabled)
            {
                var snmpSample = await _transport.GetSnmpMetricsAsync(_options.Snmp, _options.Database);
                var snmpRates = _counterCache.ComputeRates(snmpSample);

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

                    MachineCpu = snmpSample.MachineCpu,
                    ProcessCpu = snmpSample.ProcessCpu,
                    ManagedMemoryMb = snmpSample.ManagedMemoryMb,
                    UnmanagedMemoryMb = snmpSample.UnmanagedMemoryMb,
                    DirtyMemoryMb = snmpSample.DirtyMemoryMb,
                    Load1Min = snmpSample.Load1Min,
                    Load5Min = snmpSample.Load5Min,
                    Load15Min = snmpSample.Load15Min,

                    // Counter-derived rates are null until a baseline sample exists.
                    SnmpIoReadOpsPerSec = snmpRates?.IoReadOpsPerSec,
                    SnmpIoWriteOpsPerSec = snmpRates?.IoWriteOpsPerSec,
                    SnmpIoReadBytesPerSec = snmpRates?.IoReadBytesPerSec,
                    SnmpIoWriteBytesPerSec = snmpRates?.IoWriteBytesPerSec,
                    ServerSnmpRequestsPerSec = snmpRates?.ServerRequestsPerSec,
                    SnmpErrorsPerSec = snmpRates?.ErrorsPerSec,

                    Timestamp = metrics.Timestamp,
                    IsValid = metrics.IsValid,
                    ErrorMessage = metrics.ErrorMessage
                };
            }

            lock (_lock)
            {
                if (_isRunning == false)
                    return;

                _currentMetrics = metrics;

                if (_options.Snmp.Enabled && metrics.IsValid)
                {
                    _metricsHistory.Add(metrics);
                }
            }
        }
        catch
        {
            // Server metrics are supplementary; polling continues on failure.
        }
        finally
        {
            lock (_lock)
            {
                if (_isRunning)
                {
                    _timer.Change((int)_options.Snmp.PollInterval.TotalMilliseconds, Timeout.Infinite);
                }
            }
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
        // _transport is owned by the caller; do not dispose it here.
    }
}
