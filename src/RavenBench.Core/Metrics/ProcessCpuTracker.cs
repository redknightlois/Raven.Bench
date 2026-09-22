using System.Diagnostics;

namespace RavenBench.Core.Metrics;

/// <summary>
/// Tracks CPU consumption of the current process during benchmark execution.
/// Measures total processor time used relative to wall-clock time to calculate CPU utilization.
/// </summary>
public sealed class ProcessCpuTracker
{
    private TimeSpan _startCpu;
    private readonly Stopwatch _wall = new();
    private double _avgCpu;
    private bool _running;

    public void Reset()
    {
        _avgCpu = 0;
        _running = false;
    }

    public void Start()
    {
        var p = Process.GetCurrentProcess();
        _startCpu = p.TotalProcessorTime;
        _wall.Restart();
        _running = true;
    }

    /// <summary>
    /// Closes the window and computes the average. A second call is a no-op, so the average stays
    /// the reading of the window that was measured.
    /// </summary>
    public void Stop()
    {
        if (_running == false)
            return;
        _running = false;

        var p = Process.GetCurrentProcess();
        var endCpu = p.TotalProcessorTime;
        _wall.Stop();
        var cpuDelta = (endCpu - _startCpu).TotalMilliseconds;
        var wallDelta = _wall.Elapsed.TotalMilliseconds;
        var cores = Environment.ProcessorCount;
        _avgCpu = wallDelta > 0 ? Math.Min(1.0, Math.Max(0.0, (cpuDelta / wallDelta) / cores)) : 0.0;
    }

    public double AverageCpu => _avgCpu; // 0..1
}
