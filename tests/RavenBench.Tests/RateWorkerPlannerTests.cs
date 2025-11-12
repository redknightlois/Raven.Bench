using FluentAssertions;
using RavenBench;
using RavenBench.Core;
using Xunit;

namespace RavenBench.Tests;

public sealed class RateWorkerPlannerTests
{
    private static RunOptions CreateOptions(int? rateWorkers = null) => new()
    {
        Url = "http://localhost:8080",
        Database = "bench",
        Profile = WorkloadProfile.Writes,
        RateWorkers = rateWorkers
    };

    [Fact]
    public void UsesManualOverride_WhenProvided()
    {
        var opts = CreateOptions(rateWorkers: 512);
        var workers = BenchmarkRunner.ResolveRateWorkerCount(opts, targetRps: 5000, baselineLatencyMicros: 0);
        workers.Should().Be(512);
    }

    [Fact]
    public void EstimatesWorkers_FromBaselineLatency()
    {
        var opts = CreateOptions();
        // 5000 RPS * 2ms baseline = 10 concurrency → 4x headroom = 40 workers
        var workers = BenchmarkRunner.ResolveRateWorkerCount(opts, targetRps: 5000, baselineLatencyMicros: 2000);
        workers.Should().Be(40);
    }

    [Fact]
    public void FallsBack_WhenCalibrationMissing()
    {
        var opts = CreateOptions();
        // Should clamp to minimum when fallback baseline is used
        var workers = BenchmarkRunner.ResolveRateWorkerCount(opts, targetRps: 1000, baselineLatencyMicros: 0);
        workers.Should().Be(32);
    }

    [Fact]
    public void Clamps_WhenEstimationExplodes()
    {
        var opts = CreateOptions();
        var workers = BenchmarkRunner.ResolveRateWorkerCount(opts, targetRps: 200000, baselineLatencyMicros: 50000);
        workers.Should().Be(16384);
    }
}
