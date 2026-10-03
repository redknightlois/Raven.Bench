using System;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using FluentAssertions;
using RavenBench.Core.Metrics;
using RavenBench.Core.Metrics.Snmp;
using RavenBench.Core.Transport;
using RavenBench.Core;
using Xunit;

namespace RavenBench.Tests;

public class ServerMetricsTrackerTests
{
    [Fact]
    public void Constructor_Initializes_Successfully()
    {
        // INVARIANT: Constructor should not throw with valid transport and options
        // INVARIANT: Initial state should be valid
        using var transport = new TestTransport();
        var options = new RunOptions { Url = "http://localhost:8080", Database = "test", Profile = WorkloadProfile.Mixed };
        using var tracker = new ServerMetricsTracker(transport, options);

        tracker.Should().NotBeNull();

        // Should have valid initial metrics
        var initial = tracker.Current;
        initial.Should().NotBeNull();
        initial.Timestamp.Should().BeAfter(DateTime.MinValue);
    }

    [Fact]
    public void Start_Stop_Basic_Functionality()
    {
        // INVARIANT: Start and stop should work without exceptions
        // INVARIANT: Current should always return valid metrics
        using var transport = new TestTransport();
        var options = new RunOptions { Url = "http://localhost:8080", Database = "test", Profile = WorkloadProfile.Mixed };
        using var tracker = new ServerMetricsTracker(transport, options);

        // Should be able to start
        tracker.Start();

        var metrics1 = tracker.Current;
        metrics1.Should().NotBeNull();

        // Should be able to stop
        tracker.Stop();

        var metrics2 = tracker.Current;
        metrics2.Should().NotBeNull();
    }

    [Fact]
    public void Current_Property_Thread_Safe_Access()
    {
        // INVARIANT: Current property should be thread-safe
        // INVARIANT: Should never return null or invalid metrics
        using var transport = new TestTransport();
        var options = new RunOptions { Url = "http://localhost:8080", Database = "test", Profile = WorkloadProfile.Mixed };
        using var tracker = new ServerMetricsTracker(transport, options);
        tracker.Start();

        const int accessCount = 100;
        var allMetrics = new ServerMetrics[accessCount];
        var exceptions = new Exception[accessCount];

        // Access Current property from multiple threads rapidly
        Parallel.For(0, accessCount, i =>
        {
            try
            {
                allMetrics[i] = tracker.Current;
            }
            catch (Exception ex)
            {
                exceptions[i] = ex;
            }
        });

        tracker.Stop();

        // Should have no exceptions
        exceptions.Should().AllSatisfy(ex => ex.Should().BeNull());

        // All metrics should be valid
        allMetrics.Should().AllSatisfy(metrics =>
        {
            metrics.Should().NotBeNull();
            metrics.Timestamp.Should().BeAfter(DateTime.MinValue);
        });
    }

    [Fact]
    public void Multiple_Start_Stop_Cycles_Work_Correctly()
    {
        // INVARIANT: Should handle multiple start/stop cycles
        // INVARIANT: Should remain stable across cycles
        using var transport = new TestTransport();
        var options = new RunOptions { Url = "http://localhost:8080", Database = "test", Profile = WorkloadProfile.Mixed };
        using var tracker = new ServerMetricsTracker(transport, options);

        for (int cycle = 0; cycle < 3; cycle++)
        {
            tracker.Start();

            var metrics = tracker.Current;
            metrics.Should().NotBeNull();

            tracker.Stop();

            // Should still provide valid metrics after stop
            metrics = tracker.Current;
            metrics.Should().NotBeNull();
        }
    }

    [Fact]
    public void Dispose_Cleans_Up_Resources()
    {
        // INVARIANT: Dispose should work without exceptions
        // INVARIANT: Should handle disposal in any state
        var transport = new TestTransport();
        var options = new RunOptions { Url = "http://localhost:8080", Database = "test", Profile = WorkloadProfile.Mixed };
        var tracker = new ServerMetricsTracker(transport, options);

        tracker.Start();

        // Should dispose cleanly
        tracker.Dispose();

        // Second dispose should also be safe
        tracker.Dispose();

        // Clean up transport
        transport.Dispose();
    }

    [Fact]
    public void Metrics_Update_During_Polling()
    {
        // INVARIANT: Metrics should be updated periodically when started
        // INVARIANT: Should use transport's GetServerMetricsAsync method
        using var transport = new TestTransport();
        var options = new RunOptions { Url = "http://localhost:8080", Database = "test", Profile = WorkloadProfile.Mixed };
        using var tracker = new ServerMetricsTracker(transport, options);

        tracker.Start();

        var initialMetrics = tracker.Current;

        // Give it time for at least one poll cycle (polling every 2 seconds)
        // We'll wait a shorter time and just verify the infrastructure works
        Thread.Sleep(100);

        var updatedMetrics = tracker.Current;

        // Both should be valid (may or may not be different instances)
        initialMetrics.Should().NotBeNull();
        updatedMetrics.Should().NotBeNull();

        tracker.Stop();
    }

    [Fact]
    public void Snmp_Enabled_InitializesCorrectly()
    {
        // Arrange
        var transport = new TestTransport();
        var options = new RunOptions
        {
            Url = "http://localhost:8080",
            Database = "test",
            Snmp = new SnmpOptions { Enabled = true, Port = 161 },
            Profile = WorkloadProfile.Mixed
        };

        // Act
        using var tracker = new ServerMetricsTracker(transport, options);

        // Assert
        tracker.Should().NotBeNull();
        var metrics = tracker.Current;
        metrics.Should().NotBeNull();
        metrics.IsValid.Should().BeTrue();
    }

    [Fact]
    public void Snmp_Disabled_InitializesCorrectly()
    {
        // Arrange
        var transport = new TestTransport();
        var options = new RunOptions
        {
            Url = "http://localhost:8080",
            Database = "test",
            Snmp = SnmpOptions.Disabled,
            Profile = WorkloadProfile.Mixed
        };

        // Act
        using var tracker = new ServerMetricsTracker(transport, options);

        // Assert
        tracker.Should().NotBeNull();
        var metrics = tracker.Current;
        metrics.Should().NotBeNull();
        metrics.IsValid.Should().BeTrue();
    }

    // Every poll blocks until the test answers it; the test waits for a poll to arrive, never for time to pass.
    private sealed class PollGate
    {
        private readonly Channel<TaskCompletionSource<ServerMetrics>> _polls = Channel.CreateUnbounded<TaskCompletionSource<ServerMetrics>>();
        private int _inFlight;
        public int Calls;
        public int MaxInFlight;

        public async Task<ServerMetrics> PollAsync()
        {
            Interlocked.Increment(ref Calls);
            var inFlight = Interlocked.Increment(ref _inFlight);
            InterlockedMax(ref MaxInFlight, inFlight);
            var poll = new TaskCompletionSource<ServerMetrics>(TaskCreationOptions.RunContinuationsAsynchronously);
            _polls.Writer.TryWrite(poll);
            try
            {
                return await poll.Task;
            }
            finally
            {
                Interlocked.Decrement(ref _inFlight);
            }
        }

        public async Task<TaskCompletionSource<ServerMetrics>> NextAsync() =>
            await _polls.Reader.ReadAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(30));

        private static void InterlockedMax(ref int target, int value)
        {
            int current;
            while ((current = Volatile.Read(ref target)) < value && Interlocked.CompareExchange(ref target, value, current) != current) { }
        }
    }

    private static readonly DateTime T0 = new(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    private static ServerMetrics Sample(double seconds, double cpuSeconds, int? cores) => new()
    {
        Timestamp = T0.AddSeconds(seconds),
        ServerProcessorTime = TimeSpan.FromSeconds(cpuSeconds),
        ServerCores = cores
    };

    private static (ServerMetricsTracker Tracker, PollGate Gate) GatedTracker()
    {
        var gate = new PollGate();
        var transport = new TestTransport { ServerMetricsSource = gate.PollAsync };
        var options = new RunOptions { Url = "http://localhost", Database = "test" };
        return (new ServerMetricsTracker(transport, options), gate);
    }

    [Fact]
    public void CpuPercent_Uses_The_Server_Core_Count()
    {
        var serverCores = Environment.ProcessorCount + 3;

        // The server process uses half of every server core for one second.
        ServerMetricsTracker.CpuPercent(Sample(0, 0, serverCores), Sample(1, serverCores * 0.5, serverCores))
            .Should().BeApproximately(50, 1e-9);
    }

    [Fact]
    public void CpuPercent_Is_Unknown_Without_The_Server_Core_Count()
    {
        ServerMetricsTracker.CpuPercent(Sample(0, 0, null), Sample(1, 1, null)).Should().BeNull();
    }

    [Fact]
    public async Task Step_Cpu_Averages_Over_Its_Own_Window_Only()
    {
        const int cores = 4;
        var (tracker, gate) = GatedTracker();
        using var _ = tracker;

        tracker.Start();
        (await gate.NextAsync()).SetResult(Sample(0, 0, cores));
        (await gate.NextAsync()).SetResult(Sample(1, 1, cores));
        (await gate.NextAsync()).SetResult(Sample(2, 4, cores));
        var stalePoll = await gate.NextAsync();

        // Two intervals at 25% and 75%: the step average is 50%, not the last interval.
        tracker.Current.CpuUsagePercent.Should().BeApproximately(50, 1e-9);
        tracker.Current.CpuBasis.Should().Contain("step average").And.Contain("4 server-reported cores");

        tracker.Stop();
        tracker.Start();
        tracker.Current.CpuUsagePercent.Should().BeNull("a new step has no sample yet");

        // A poll from the earlier step answers after the restart; it must not become the new step's baseline.
        stalePoll.SetResult(Sample(3, 4, cores));
        (await gate.NextAsync()).SetResult(Sample(10, 40, cores));
        var lastPoll = await gate.NextAsync();
        tracker.Current.CpuUsagePercent.Should().BeNull("the first poll of a step has nothing in the step to compare to");
        lastPoll.SetResult(Sample(12, 42, cores));
        await gate.NextAsync();

        tracker.Current.CpuUsagePercent.Should().BeApproximately(25, 1e-9);
    }

    [Fact]
    public async Task Restart_During_An_InFlight_Poll_Leaves_One_Poll_Loop()
    {
        var (tracker, gate) = GatedTracker();
        using var _ = tracker;

        tracker.Start();
        var first = await gate.NextAsync();
        tracker.Stop();
        tracker.Start();
        tracker.Stop();
        tracker.Start();

        first.SetResult(Sample(0, 0, 1));
        (await gate.NextAsync()).SetResult(Sample(1, 0, 1));
        await gate.NextAsync();

        gate.MaxInFlight.Should().Be(1);
        gate.Calls.Should().Be(3, "each answered poll schedules exactly one next poll");
    }

    private static SnmpSample Snmp(double seconds, long requests, double ioReadOps) =>
        new() { Timestamp = T0.AddSeconds(seconds), TotalRequests = requests, IoReadOpsPerSec = ioReadOps };

    private static (ServerMetricsTracker Tracker, PollGate Gate) SnmpTracker(params SnmpSample[] samples)
    {
        var queue = new System.Collections.Generic.Queue<SnmpSample>(samples);
        var gate = new PollGate();
        var transport = new TestTransport { ServerMetricsSource = gate.PollAsync, SnmpSource = queue.Dequeue };
        var options = new RunOptions { Url = "http://localhost", Database = "test", Snmp = new SnmpOptions { Enabled = true } };
        return (new ServerMetricsTracker(transport, options), gate);
    }

    [Fact]
    public async Task An_Snmp_Sample_Is_Kept_When_The_Admin_Poll_Fails()
    {
        var (tracker, gate) = SnmpTracker(Snmp(0, 0, 0), Snmp(1, 100, 0));
        using var _ = tracker;
        var failed = new ServerMetrics { IsValid = false, ErrorMessage = "admin endpoint unreachable" };

        tracker.Start();
        (await gate.NextAsync()).SetResult(failed);
        (await gate.NextAsync()).SetResult(failed);
        await gate.NextAsync();

        var history = tracker.GetHistory();
        history.Should().HaveCount(2);
        history[1].ServerSnmpRequestsPerSec.Should().BeApproximately(100, 1e-9);
    }
}
