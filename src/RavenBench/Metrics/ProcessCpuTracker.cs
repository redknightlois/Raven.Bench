namespace RavenBench.Metrics;


/// <summary>
/// Tracks CPU consumption of the current process during benchmark execution.
/// Measures total processor time used relative to wall-clock time to calculate CPU utilization.
/// </summary>
public sealed class ProcessCpuTracker
{
    private TimeSpan _startCpu;
    private DateTime _startWall;
    private double _avgCpu;

    public void Reset()
    {
        _avgCpu = 0;
    }

    public void Start()
    {
        var p = System.Diagnostics.Process.GetCurrentProcess();
        _startCpu = p.TotalProcessorTime;
        _startWall = DateTime.UtcNow;
    }

    public void Stop()
    {
        var p = System.Diagnostics.Process.GetCurrentProcess();
        var endCpu = p.TotalProcessorTime;
        var endWall = DateTime.UtcNow;
        var cpuDelta = (endCpu - _startCpu).TotalMilliseconds;
        var wallDelta = (endWall - _startWall).TotalMilliseconds;
        var cores = Environment.ProcessorCount;
        _avgCpu = wallDelta > 0 ? Math.Min(1.0, Math.Max(0.0, (cpuDelta / wallDelta) / cores)) : 0.0;
    }

    public double AverageCpu => _avgCpu; // 0..1
}

