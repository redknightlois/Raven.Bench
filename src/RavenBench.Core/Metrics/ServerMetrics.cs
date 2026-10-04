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

/// <summary>One measurement window of the tracker, on the clock the SNMP sample timestamps use.</summary>
public readonly record struct MeasurementWindow(DateTime Start, DateTime End);

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
    private readonly List<MeasurementWindow> _windows = new();

    private ServerMetrics _currentMetrics = new();
    private ServerMetrics? _windowStart;
    private SnmpWindow _snmpWindow = new();
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
            _windows.Add(new MeasurementWindow(DateTime.UtcNow, DateTime.MaxValue));
            _windowStart = null;
            _snmpWindow = new SnmpWindow();
            // The first counter rate of a window must not use a sample taken before the window opened.
            _counterCache.Reset();
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
            if (_isRunning)
                _windows[^1] = _windows[^1] with { End = DateTime.UtcNow };
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

            // An admin poll that fails comes back invalid; the SNMP sample is kept regardless.
            var snmpSample = _options.Snmp.Enabled ? await _transport.GetSnmpMetricsAsync(_options.Snmp, _options.Database) : null;

            lock (_lock)
            {
                if (_isRunning == false || window != _window)
                    return;

                if (snmpSample is { IsEmpty: false })
                {
                    // Under the lock and after the window check, so a sample from an earlier window never becomes a baseline.
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

                        // Per poll interval; null until a baseline sample exists in this window.
                        SnmpIoReadOpsPerSec = snmpRates?.IoReadOpsPerSec,
                        SnmpIoWriteOpsPerSec = snmpRates?.IoWriteOpsPerSec,
                        SnmpIoReadBytesPerSec = snmpRates?.IoReadBytesPerSec,
                        SnmpIoWriteBytesPerSec = snmpRates?.IoWriteBytesPerSec,
                        ServerSnmpRequestsPerSec = snmpRates?.ServerRequestsPerSec,
                        SnmpErrorsPerSec = snmpRates?.ErrorsPerSec,
                    };
                    _metricsHistory.Add(metrics with { Timestamp = snmpSample.Timestamp });
                    _snmpWindow.Add(snmpSample, snmpRates);
                }

                if (metrics.ServerProcessorTime.HasValue)
                    _windowStart ??= metrics;
                _currentMetrics = _snmpWindow.Apply(metrics) with
                {
                    CpuUsagePercent = _windowStart == null ? null : CpuPercent(_windowStart, metrics),
                    CpuBasis = metrics.ServerCores is { } cores
                        ? $"step average over {cores} server-reported cores"
                        : "unknown: the server did not report its core count"
                };
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

    /// <summary>Every measurement window opened so far; a window still open ends at <see cref="DateTime.MaxValue"/>.</summary>
    public List<MeasurementWindow> GetWindows()
    {
        lock (_lock)
        {
            return new List<MeasurementWindow>(_windows);
        }
    }

    public List<ServerMetrics> GetHistory()
    {
        lock (_lock)
        {
            return new List<ServerMetrics>(_metricsHistory);
        }
    }

    /// <summary>
    /// The SNMP rates over one measurement window: requests from the counter delta between the window's first
    /// and last samples, and each IO and error rate time-weighted over the window's poll intervals.
    /// </summary>
    private sealed class SnmpWindow
    {
        private SnmpSample? _first;
        private SnmpSample? _last;
        private TimeWeighted _readOps, _writeOps, _readBytes, _writeBytes, _errors;

        public void Add(SnmpSample sample, SnmpRates? rates)
        {
            if (_last != null && rates != null)
            {
                var seconds = (sample.Timestamp - _last.Timestamp).TotalSeconds;
                _readOps.Add(rates.IoReadOpsPerSec, seconds);
                _writeOps.Add(rates.IoWriteOpsPerSec, seconds);
                _readBytes.Add(rates.IoReadBytesPerSec, seconds);
                _writeBytes.Add(rates.IoWriteBytesPerSec, seconds);
                _errors.Add(rates.ErrorsPerSec, seconds);
            }
            _first ??= sample;
            _last = sample;
        }

        public ServerMetrics Apply(ServerMetrics metrics) => metrics with
        {
            SnmpIoReadOpsPerSec = _readOps.Mean,
            SnmpIoWriteOpsPerSec = _writeOps.Mean,
            SnmpIoReadBytesPerSec = _readBytes.Mean,
            SnmpIoWriteBytesPerSec = _writeBytes.Mean,
            ServerSnmpRequestsPerSec = RequestsPerSec(),
            SnmpErrorsPerSec = _errors.Mean,
        };

        private double? RequestsPerSec()
        {
            if (_first?.TotalRequests is not { } start || _last?.TotalRequests is not { } end)
                return null;
            var seconds = (_last.Timestamp - _first.Timestamp).TotalSeconds;
            return seconds > 0 && end >= start ? (end - start) / seconds : null;
        }

        private struct TimeWeighted
        {
            private double _sum, _seconds;

            public void Add(double? value, double seconds)
            {
                if (value is not { } v || seconds <= 0)
                    return;
                _sum += v * seconds;
                _seconds += seconds;
            }

            public readonly double? Mean => _seconds > 0 ? _sum / _seconds : null;
        }
    }

    public void Dispose()
    {
        Stop();
        _timer.Dispose();
        // _transport is owned by the caller; do not dispose it here.
    }
}
