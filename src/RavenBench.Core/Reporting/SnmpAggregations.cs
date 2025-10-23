namespace RavenBench.Core.Reporting;

public sealed class SnmpAggregations
{
    public double? TotalSnmpIoWriteOps { get; init; }
    public double? AverageSnmpIoWriteOpsPerSec { get; init; }
    public double? TotalSnmpIoReadOps { get; init; }
    public double? AverageSnmpIoReadOpsPerSec { get; init; }
    public double? TotalSnmpIoWriteBytes { get; init; }
    public double? AverageSnmpIoWriteBytesPerSec { get; init; }
    public double? TotalSnmpIoReadBytes { get; init; }
    public double? AverageSnmpIoReadBytesPerSec { get; init; }
}