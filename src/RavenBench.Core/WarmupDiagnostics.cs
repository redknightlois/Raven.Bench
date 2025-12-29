using System;
using RavenBench.Core.Metrics;

namespace RavenBench.Core;

/// <summary>
/// Captures latency and throughput characteristics observed during a warmup iteration.
/// </summary>
public sealed class WarmupDiagnostics
{
    public static WarmupDiagnostics Empty { get; } = new WarmupDiagnostics(
        iteration: 0,
        duration: TimeSpan.Zero,
        throughput: 0,
        errorRate: 0,
        sampleCount: 0,
        p50Micros: 0,
        p95Micros: 0,
        p99Micros: 0,
        maxMicros: 0);

    public int Iteration { get; }
    public TimeSpan Duration { get; }
    public double Throughput { get; }
    public double ErrorRate { get; }
    public long SampleCount { get; }
    public double P50Micros { get; }
    public double P95Micros { get; }
    public double P99Micros { get; }
    public double MaxMicros { get; }

    public WarmupDiagnostics(
        int iteration,
        TimeSpan duration,
        double throughput,
        double errorRate,
        long sampleCount,
        double p50Micros,
        double p95Micros,
        double p99Micros,
        double maxMicros)
    {
        Iteration = iteration;
        Duration = duration;
        Throughput = throughput;
        ErrorRate = errorRate;
        SampleCount = sampleCount;
        P50Micros = p50Micros;
        P95Micros = p95Micros;
        P99Micros = p99Micros;
        MaxMicros = maxMicros;
    }

    public WarmupDiagnostics WithIteration(int iteration) => new WarmupDiagnostics(
        iteration,
        Duration,
        Throughput,
        ErrorRate,
        SampleCount,
        P50Micros,
        P95Micros,
        P99Micros,
        MaxMicros);

    public static WarmupDiagnostics FromRecorder(LatencyRecorder recorder, LoadGeneratorMetrics metrics, TimeSpan duration)
    {
        if (recorder == null)
            throw new ArgumentNullException(nameof(recorder));
        if (metrics == null)
            throw new ArgumentNullException(nameof(metrics));

        var snapshot = recorder.Snapshot();

        return new WarmupDiagnostics(
            iteration: 0,
            duration: duration,
            throughput: metrics.Throughput,
            errorRate: metrics.ErrorRate,
            sampleCount: metrics.OperationsCompleted,
            p50Micros: snapshot.GetPercentile(50),
            p95Micros: snapshot.GetPercentile(95),
            p99Micros: snapshot.GetPercentile(99),
            maxMicros: snapshot.MaxMicros);
    }
}
