using System;
using System.Diagnostics;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using RavenBench.Core;
using RavenBench.Core.Metrics;
using RavenBench.Core.Metrics.Snmp;
using RavenBench.Core.Transport;
using RavenBench.Core.Workload;
using Xunit;

namespace RavenBench.Tests;

public sealed class LoadGeneratorCoordinatedOmissionTests
{
    [Fact]
    public async Task ClosedLoop_Records_One_Sample_Per_Issued_Request_At_Its_Service_Time()
    {
        const int requests = 20;
        var transport = new VariableLatencyTransport(latencyMs: 0);
        var workload = new CountedWorkload { Remaining = 50 };
        var generator = new ClosedLoopLoadGenerator(transport, workload, concurrency: 1, new Random(17));

        // A near-zero warmup floor followed by slow requests is what synthetic backfill would inflate.
        await generator.ExecuteWarmupAsync(TimeSpan.FromMinutes(1), CancellationToken.None);
        transport.LatencyMs = 10;
        workload.Remaining = requests;

        var (recorder, metrics) = await generator.ExecuteMeasurementAsync(TimeSpan.FromMinutes(1), CancellationToken.None);
        var snapshot = recorder.Snapshot();

        metrics.OperationsCompleted.Should().Be(requests);
        snapshot.TotalCount.Should().Be(requests);
        snapshot.GetPercentile(50).Should().BeGreaterOrEqualTo(10_000);
    }

    // One worker serving at 20 ms against 200 arrivals/s holds a quarter of the rate, so the queue grows for the whole step.
    private const int ServiceMs = 20;
    private const double TargetRps = 200;
    private static readonly TimeSpan SaturatedStep = TimeSpan.FromMilliseconds(500);

    private static Task<(LatencyRecorder, LoadGeneratorMetrics)> RunSaturatedRateStep() =>
        new RateLoadGenerator(new VariableLatencyTransport(ServiceMs), new SingleOperationWorkload(), TargetRps, maxConcurrency: 1, new Random(42))
            .ExecuteMeasurementAsync(SaturatedStep, CancellationToken.None);

    [Fact]
    public async Task RateGenerator_IncludesQueueWaitUnderSaturation()
    {
        var (recorder, metrics) = await RunSaturatedRateStep();

        metrics.OperationsCompleted.Should().BeGreaterThan(0);
        // An arrival late in the step waits about step × (1 − capacity / rate) behind the queue; half of it leaves room for timer slack.
        var capacityRps = 1000.0 / ServiceMs;
        var expectedQueueWaitMs = SaturatedStep.TotalMilliseconds * (1 - capacityRps / TargetRps);
        recorder.Snapshot().MaxMicros.Should().BeGreaterThan((long)((ServiceMs + expectedQueueWaitMs / 2) * 1000),
            "latency is measured from each arrival's due time, so the queue wait adds to the service time");
    }

    [Fact]
    public async Task RateGenerator_Counts_Every_Due_Arrival_In_The_Latency()
    {
        var (recorder, metrics) = await RunSaturatedRateStep();

        metrics.ScheduledOperations.Should().BeGreaterThan(metrics.OperationsCompleted, "the step stops with arrivals never issued");
        recorder.Snapshot().TotalCount.Should().Be(metrics.ScheduledOperations);
    }

    // Ends the step once its remaining operations are drawn.
    private sealed class CountedWorkload : IWorkload
    {
        public int Remaining;
        public bool IsExhausted => Remaining <= 0;
        public OperationBase NextOperation(Random rng)
        {
            Remaining--;
            return new ReadOperation { Id = "users/1" };
        }
    }

    private sealed class SingleOperationWorkload : IWorkload
    {
        public OperationBase NextOperation(Random rng) => new ReadOperation { Id = "users/1" };
    }

    private sealed class VariableLatencyTransport : IYcsbTransport
    {
        private int _latencyMs;

        public int LatencyMs
        {
            get => _latencyMs;
            set => _latencyMs = Math.Max(0, value);
        }

        public VariableLatencyTransport(int latencyMs)
        {
            _latencyMs = Math.Max(0, latencyMs);
        }

        public string ProductName => "Stub";

        public bool ReportsWireBytes => true;

        public async Task<TransportResult> ExecuteAsync(OperationBase op, CancellationToken ct)
        {
            if (_latencyMs > 0)
            {
                // Timers tick in whole milliseconds and may fire early; the stub serves for at least its latency on the recorder's clock.
                var due = Stopwatch.GetTimestamp() + Stopwatch.Frequency * _latencyMs / 1000;
                await Task.Delay(_latencyMs, ct);
                while (Stopwatch.GetTimestamp() < due)
                    await Task.Yield();
            }

            return new TransportResult(64, 32);
        }

        public Task PutAsync<T>(string id, T document) => Task.CompletedTask;
        public Task EnsureDatabaseExistsAsync(string databaseName) => Task.CompletedTask;
        public Task<long> GetDocumentCountAsync(string idPrefix) => Task.FromResult(0L);
        public Task<string> GetServerVersionAsync() => Task.FromResult("test");

        public void Dispose()
        {
        }
    }
}
