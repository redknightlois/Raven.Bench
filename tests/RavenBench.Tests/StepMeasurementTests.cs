using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using RavenBench.Analysis;
using RavenBench.Core;
using RavenBench.Core.Metrics;
using RavenBench.Core.Reporting;
using RavenBench.Core.Workload;
using Xunit;

namespace RavenBench.Tests;

/// <summary>
/// The measurement contract of the shared step-execution path that the ycsb runs and the closed
/// and rate commands both take: the load host's CPU over the step's own post-warmup window, the
/// measured length of that window, and the client-bound marking that reads the one threshold.
/// </summary>
public class StepMeasurementTests
{
    private static RunOptions StepOptions(TimeSpan? duration = null, TimeSpan? warmup = null) => new()
    {
        Url = "http://localhost:8081",
        Database = "bench",
        Distribution = KeyDistributionKind.Uniform,
        Transport = TransportKind.Raw,
        Compression = CompressionMode.Identity,
        Warmup = warmup ?? TimeSpan.Zero,
        Duration = duration ?? TimeSpan.FromMilliseconds(200),
        Step = new StepPlan(2, 2, 2),
        Profile = WorkloadProfile.Mixed
    };

    private static IWorkload UpdateWorkload() =>
        new MixedProfileWorkload(WorkloadMix.FromWeights(0, 100, 0), new UniformDistribution(), 1024, seed: 42);

    [Fact]
    public async Task A_Step_That_Ran_Reports_The_Trackers_Reading_For_Its_Own_Window()
    {
        using var transport = new TestTransport(baseLatencyMs: 1);
        var workload = UpdateWorkload();
        var cpuTracker = new ProcessCpuTracker();
        var executor = new BenchmarkExecutor(StepOptions(), transport, workload, cpuTracker);

        var generator = new ClosedLoopLoadGenerator(transport, workload, 2, new Random(42));
        var (_, step) = await executor.ExecuteStepAsync(generator, 0, 2, CancellationToken.None);

        step.SampleCount.Should().BeGreaterThan(0, "the step must have done work for its CPU figure to mean anything");
        step.ClientCpu.Should().BeGreaterThan(0, "the figure is read after the window closes and the tracker stopped");
        step.ClientCpu.Should().Be(cpuTracker.AverageCpu, "the step reports the one tracker's reading for that window");
        step.ClientCpu.Should().BeLessOrEqualTo(1.0, "the scale is a 0..1 fraction of the load host's capacity");
    }

    [Fact]
    public async Task Each_Step_Reports_Its_Own_Window_Rather_Than_The_Accumulated_Run()
    {
        using var transport = new TestTransport(baseLatencyMs: 1);
        var workload = UpdateWorkload();
        var cpuTracker = new ProcessCpuTracker();
        var executor = new BenchmarkExecutor(StepOptions(), transport, workload, cpuTracker);
        var rng = new Random(42);

        var wall = System.Diagnostics.Stopwatch.StartNew();
        var steps = new List<StepResult>();
        for (var i = 0; i < 3; i++)
        {
            var generator = new ClosedLoopLoadGenerator(transport, workload, 2, rng);
            var (_, step) = await executor.ExecuteStepAsync(generator, i, 2, CancellationToken.None);
            steps.Add(step);
        }
        wall.Stop();

        steps.Should().OnlyContain(s => s.ClientCpu > 0);

        // Each step's window is its own disjoint slice of the run. A window held open across steps
        // would make the slices overlap, so their total would exceed the run's wall time.
        var total = steps.Aggregate(TimeSpan.Zero, (sum, s) => sum + s.MeasuredDuration!.Value);
        total.Should().BeLessOrEqualTo(wall.Elapsed);

        steps[^1].ClientCpu.Should().Be(cpuTracker.AverageCpu, "the tracker holds the last window alone");
    }

    [Fact]
    public async Task A_Bounded_Step_Reports_The_Measured_Window_Rather_Than_The_Configured_Cap()
    {
        var cap = TimeSpan.FromSeconds(30);
        using var transport = new TestTransport(baseLatencyMs: 1);
        var generator = new ScriptedLoadGenerator(measurement: () => Task.Delay(TimeSpan.FromMilliseconds(200)));
        var executor = new BenchmarkExecutor(StepOptions(duration: cap), transport, UpdateWorkload(), new ProcessCpuTracker());

        var (_, step) = await executor.ExecuteStepAsync(generator, 0, 2, CancellationToken.None);

        step.MeasuredDuration.Should().NotBeNull();
        step.MeasuredDuration!.Value.Should().BeGreaterThan(TimeSpan.Zero).And.BeLessThan(cap,
            "a fill that ends on its own reports the window it measured, never the configured cap");
    }

    [Fact]
    public async Task A_Step_Below_The_Threshold_Is_Not_Marked_Invalid()
    {
        using var transport = new TestTransport(baseLatencyMs: 5);
        var workload = UpdateWorkload();
        var executor = new BenchmarkExecutor(StepOptions(), transport, workload, new ProcessCpuTracker());

        var (_, step) = await executor.ExecuteStepAsync(
            new ClosedLoopLoadGenerator(transport, workload, 2, new Random(42)), 0, 2, CancellationToken.None);

        ClientSaturation.IsSaturated(step.ClientCpu).Should().BeFalse("a two-worker sleeping workload does not saturate the load host");
        step.InvalidReason.Should().BeNull();
        step.Reason.Should().BeNull("the marking never overloads the ramp-stop reason");
    }

    [Fact]
    public async Task The_Executor_Marks_A_Step_By_The_Guards_Rule_Over_Its_Own_Reading()
    {
        using var transport = new TestTransport(baseLatencyMs: 1);
        var workload = UpdateWorkload();
        var cpuTracker = new ProcessCpuTracker();
        var executor = new BenchmarkExecutor(StepOptions(), transport, workload, cpuTracker);

        var (_, step) = await executor.ExecuteStepAsync(
            new ClosedLoopLoadGenerator(transport, workload, 2, new Random(42)), 0, 2, CancellationToken.None);

        step.InvalidReason.Should().Be(ClientSaturation.MarkingFor(cpuTracker.AverageCpu),
            "the step's marking is the guard's rule applied to that window's own figure");
    }

    [Fact]
    public void The_Marking_And_The_Verdict_Read_The_Same_Threshold()
    {
        var atThreshold = new StepResult { Concurrency = 16, Throughput = 1000, ClientCpu = ClientSaturation.Threshold };
        var below = new StepResult { Concurrency = 16, Throughput = 1000, ClientCpu = ClientSaturation.Threshold / 2 };
        var opts = StepOptions();

        ClientSaturation.MarkingFor(atThreshold.ClientCpu).Should().NotBeNullOrEmpty();
        ResultAnalyzer.BuildVerdict(atThreshold, opts).Should().Be("client-limited (CPU)");

        ClientSaturation.MarkingFor(below.ClientCpu).Should().BeNull();
        ResultAnalyzer.BuildVerdict(below, opts).Should().NotBe("client-limited (CPU)");
    }

    [Fact]
    public void The_Console_Line_Names_The_Step_The_Run_And_The_Reason()
    {
        var reason = ClientSaturation.MarkingFor(ClientSaturation.Threshold)!;

        var ycsbLine = ClientSaturation.ConsoleLine(stepNumber: 3, concurrency: 64, runName: "C", reason);
        ycsbLine.Should().Contain("Step 3").And.Contain("64").And.Contain("'C'").And.Contain(reason);
        ycsbLine.Should().Contain("INVALID");

        // A closed or rate command writes one result, so the step label alone identifies it.
        var closedLine = ClientSaturation.ConsoleLine(stepNumber: 3, concurrency: 64, runName: null, reason);
        closedLine.Should().Contain("Step 3").And.Contain("64").And.Contain(reason);
        closedLine.Should().NotContain("run '");
    }

    /// <summary>A generator whose measurement phase is the test's own work.</summary>
    private sealed class ScriptedLoadGenerator : ILoadGenerator
    {
        private readonly Func<Task> _measurement;

        public ScriptedLoadGenerator(Func<Task> measurement)
        {
            _measurement = measurement;
        }

        public int Concurrency => 2;
        public double? TargetThroughput => null;
        public void SetBaselineLatency(long baselineLatencyMicros) { }
        public Task ExecuteWarmupAsync(TimeSpan duration, CancellationToken cancellationToken) => Task.CompletedTask;

        public async Task<(LatencyRecorder latencyRecorder, LoadGeneratorMetrics metrics)> ExecuteMeasurementAsync(
            TimeSpan duration, CancellationToken cancellationToken)
        {
            var wall = System.Diagnostics.Stopwatch.StartNew();
            await _measurement();
            wall.Stop();

            return (new LatencyRecorder(recordLatencies: true),
                new LoadGeneratorMetrics { Duration = wall.Elapsed, OperationsCompleted = 1, Throughput = 1 });
        }
    }
}
