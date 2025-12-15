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
        // 5000 RPS * 2ms baseline = 10 concurrency → 1.5x headroom = 15 workers → clamped to min 32
        var workers = BenchmarkRunner.ResolveRateWorkerCount(opts, targetRps: 5000, baselineLatencyMicros: 2000);
        workers.Should().Be(32);
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
        // 200000 RPS * 50ms = 10000 concurrency → 1.5x headroom = 15000 workers → clamped to max 16384
        var workers = BenchmarkRunner.ResolveRateWorkerCount(opts, targetRps: 200000, baselineLatencyMicros: 50000);
        workers.Should().Be(15000);
    }

    [Fact]
    public void UsesObservedServiceTime_WhenHigherThanBaseline()
    {
        var opts = CreateOptions();

        // Baseline RTT is tiny (0.2ms), but observed end-to-end service time is 5ms.
        // 16000 RPS * 5ms = 80 concurrency → 1.5x headroom = 120 workers
        var workers = BenchmarkRunner.ResolveRateWorkerCount(opts, targetRps: 16000, baselineLatencyMicros: 200, observedServiceTimeSeconds: 0.005);
        workers.Should().Be(120);
    }
}
