using RavenBench.Core;
using RavenBench.Core.Metrics.Snmp;

namespace RavenBench.Core.Metrics;

/// <summary>
/// Represents server-side metrics collected from RavenDB endpoints.
/// </summary>
public sealed record ServerMetrics
{
    /// <summary>Server process CPU percent of all server cores, averaged from the first sample of the tracker window to this one; null when unknown.</summary>
    public double? CpuUsagePercent { get; init; }

    /// <summary>How <see cref="CpuUsagePercent"/> was computed: the core basis and the window.</summary>
    public string? CpuBasis { get; init; }

    /// <summary>Cumulative CPU time of the server process, as the server reports it.</summary>
    public TimeSpan? ServerProcessorTime { get; init; }

    /// <summary>Core count the server reports for itself; null when the server does not report it.</summary>
    public int? ServerCores { get; init; }

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
/// Each Start opens a new window: the server CPU figure averages over the samples taken since that Start only.
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
    private ServerMetrics? _windowStart;
    private bool _isRunning;
    private bool _pollInFlight;
    private int _window;

    public ServerMetricsTracker(Transport.ITransport transport, RunOptions options)
    {
        _transport = transport;
        _options = options;
        _timer = new Timer(PollMetrics, null, Timeout.Infinite, Timeout.Infinite);
    }

    /// <summary>Opens a new measurement window and starts polling.</summary>
    public void Start()
    {
        lock (_lock)
        {
            _isRunning = true;
            _window++;
            _windowStart = null;
            _currentMetrics = new ServerMetrics();
            // A poll still in flight from an earlier window reschedules the loop when it ends.
            if (_pollInFlight == false)
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

    /// <summary>
    /// The server CPU percent between two samples of the server process, against the server's own core count.
    /// Null when either sample lacks the CPU time, the server core count is unknown, or no time elapsed.
    /// </summary>
    public static double? CpuPercent(ServerMetrics start, ServerMetrics end)
    {
        var elapsed = end.Timestamp - start.Timestamp;
        if (start.ServerProcessorTime is not { } startCpu || end.ServerProcessorTime is not { } endCpu
            || end.ServerCores is not { } cores || elapsed <= TimeSpan.Zero || endCpu < startCpu)
            return null;

        return Math.Clamp((endCpu - startCpu) / (elapsed * cores) * 100.0, 0, 100);
    }

    // One-shot timer: at most one poll is in flight, and it reschedules the next one when it ends.
    private async void PollMetrics(object? state)
    {
        int window;
        lock (_lock)
        {
            if (_isRunning == false || _pollInFlight)
                return;
            _pollInFlight = true;
            window = _window;
        }

        // The poll's own exceptions are not the measured step's.
        using var suppress = RavenBench.Core.Diagnostics.FirstChanceExceptionTracker.Suppress();
        try
        {
            var metrics = await _transport.GetServerMetricsAsync();

            if (_options.Snmp.Enabled)
            {
                var snmpSample = await _transport.GetSnmpMetricsAsync(_options.Snmp, _options.Database);
                var snmpRates = _counterCache.ComputeRates(snmpSample);

                metrics = metrics with
                {
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
                };
            }

            lock (_lock)
            {
                if (_isRunning == false || window != _window)
                    return;

                if (metrics.ServerProcessorTime.HasValue)
                    _windowStart ??= metrics;
                _currentMetrics = metrics with
                {
                    CpuUsagePercent = _windowStart == null ? null : CpuPercent(_windowStart, metrics),
                    CpuBasis = metrics.ServerCores is { } cores
                        ? $"step average over {cores} server-reported cores"
                        : "unknown: the server did not report its core count"
                };

                // An admin poll that fails comes back invalid; its SNMP sample is kept regardless.
                if (_options.Snmp.Enabled)
                    _metricsHistory.Add(metrics);
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
                _pollInFlight = false;
                if (_isRunning)
                {
                    // A poll from an earlier window hands the loop to the current window at once.
                    _timer.Change(window == _window ? (int)_options.Snmp.PollInterval.TotalMilliseconds : 0, Timeout.Infinite);
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
