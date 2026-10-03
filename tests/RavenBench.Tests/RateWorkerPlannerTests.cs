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
        var workers = RateWorkerPlanner.ResolveRateWorkerCount(opts, targetRps: 5000, baselineLatencyMicros: 0);
        workers.Should().Be(512);
    }

    [Fact]
    public void EstimatesWorkers_FromBaselineLatency()
    {
        var opts = CreateOptions();
        // 5000 RPS * 2ms baseline = 10 concurrency → 1.5x headroom = 15 workers → clamped to min 32
        var workers = RateWorkerPlanner.ResolveRateWorkerCount(opts, targetRps: 5000, baselineLatencyMicros: 2000);
        workers.Should().Be(32);
    }

    [Fact]
    public void FallsBack_WhenCalibrationMissing()
    {
        var opts = CreateOptions();
        // Should clamp to minimum when fallback baseline is used
        var workers = RateWorkerPlanner.ResolveRateWorkerCount(opts, targetRps: 1000, baselineLatencyMicros: 0);
        workers.Should().Be(32);
    }

    [Fact]
    public void Clamps_WhenEstimationExplodes()
    {
        var opts = CreateOptions();
        // 200000 RPS * 50ms = 10000 concurrency → 1.5x headroom = 15000 workers → clamped to max 16384
        var workers = RateWorkerPlanner.ResolveRateWorkerCount(opts, targetRps: 200000, baselineLatencyMicros: 50000);
        workers.Should().Be(15000);
    }

    [Fact]
    public void UsesObservedServiceTime_WhenHigherThanBaseline()
    {
        var opts = CreateOptions();

        // Baseline RTT is tiny (0.2ms), but observed end-to-end service time is 5ms.
        // 16000 RPS * 5ms = 80 concurrency → 1.5x headroom = 120 workers
        var workers = RateWorkerPlanner.ResolveRateWorkerCount(opts, targetRps: 16000, baselineLatencyMicros: 200, observedServiceTimeSeconds: 0.005);
        workers.Should().Be(120);
    }

    [Fact]
    public void Steady_Service_Time_Keeps_Workers_At_The_Little_Law_Bound()
    {
        var opts = CreateOptions();
        int? previous = null;
        foreach (var rps in new[] { 1000, 2000, 3000, 4000 })
        {
            // A server that meets every rate with a 1 ms service time; bulk size does not enter the plan.
            var workers = RateWorkerPlanner.ResolveRateWorkerCount(opts, rps, baselineLatencyMicros: 1000, observedServiceTimeSeconds: previous.HasValue ? 0.001 : null, previous);
            workers.Should().BeLessThanOrEqualTo(RateWorkerPlanner.ResolveRateWorkerCount(opts, rps, 1000, observedServiceTimeSeconds: 0.001));
            previous = workers;
        }
    }

    [Fact]
    public void A_Faster_Later_Step_Lowers_The_Next_Plan()
    {
        var opts = CreateOptions();
        var afterSlow = RateWorkerPlanner.ResolveRateWorkerCount(opts, 50_000, 1000, observedServiceTimeSeconds: 0.050, previousAutoWorkers: 8192);
        var afterFast = RateWorkerPlanner.ResolveRateWorkerCount(opts, 50_000, 1000, observedServiceTimeSeconds: 0.002, previousAutoWorkers: afterSlow);

        afterFast.Should().Be(RateWorkerPlanner.ResolveRateWorkerCount(opts, 50_000, 1000, observedServiceTimeSeconds: 0.002));
        afterFast.Should().BeLessThan(afterSlow);
    }

    [Fact]
    public void Explicit_Workers_Ignore_The_Growth_Limit()
    {
        RateWorkerPlanner.ResolveRateWorkerCount(CreateOptions(rateWorkers: 512), 5000, 0, 0.1, previousAutoWorkers: 32).Should().Be(512);
        RateWorkerPlanner.ResolveRateWorkerCount(CreateOptions(), 50_000, 0, 0.1, previousAutoWorkers: 32).Should().Be(64);
    }
}
