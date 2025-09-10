using RavenBench.Util;

namespace RavenBench.Reporting;

public sealed class BenchmarkRun
{
    public required List<StepResult> Steps { get; init; }
    public required double MaxNetworkUtilization { get; init; }
    public required string ClientCompression { get; init; }
    public required string EffectiveHttpVersion { get; init; }
}

public sealed class BenchmarkSummary
{
    public required RunOptions Options { get; init; }
    public required List<StepResult> Steps { get; init; }
    public StepResult? Knee { get; init; }
    public required string Verdict { get; init; }
    public required string ClientCompression { get; init; }
    public required string EffectiveHttpVersion { get; init; }
    public string? Notes { get; init; }
}

public sealed class StepResult
{
    public int Concurrency { get; init; }
    public double Throughput { get; init; }
    public double ErrorRate { get; init; }
    public long BytesOut { get; init; }
    public long BytesIn { get; init; }

    public double P50Ms { get; set; }
    public double P90Ms { get; set; }
    public double P95Ms { get; set; }
    public double P99Ms { get; set; }

    public double ClientCpu { get; init; }
    public double NetworkUtilization { get; init; }
    
    // Server-side metrics from RavenDB
    public double? ServerCpu { get; init; }
    public long? ServerMemoryMB { get; init; }
    public double? ServerRequestsPerSec { get; init; }
    public long? ServerIoReadOps { get; init; }
    public long? ServerIoWriteOps { get; init; }
    public long? ServerIoReadKb { get; init; }
    public long? ServerIoWriteKb { get; init; }

    public string? Reason { get; set; }
}

