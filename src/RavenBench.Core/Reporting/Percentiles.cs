using System.Text.Json.Serialization;

namespace RavenBench.Core.Reporting;

public readonly struct Percentiles
{
    public double P50 { get; }
    public double P75 { get; }
    public double P90 { get; }
    public double P95 { get; }
    public double P99 { get; }
    public double P999 { get; }

    [JsonConstructor]
    public Percentiles(double p50, double p75, double p90, double p95, double p99, double p999)
    {
        P50 = p50;
        P75 = p75;
        P90 = p90;
        P95 = p95;
        P99 = p99;
        P999 = p999;
    }
}