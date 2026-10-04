using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using RavenBench.Core;
using RavenBench.Core.Metrics;
using RavenBench.Core.Reporting;
using RavenBench.Core.Transport;
using RavenBench.Core.Workload;
using Xunit;

namespace RavenBench.Tests;

public class ClosedLoopRampTests
{
    [Fact]
    public async Task Ramp_Collects_Steps_And_Keeps_Invariants()
    {
        var opts = new RunOptions
        {
            Url = "http://localhost:10101",
            Database = "ycsb",
            Distribution = KeyDistributionKind.Uniform,
            Transport = TransportKind.Raw,
            Compression = CompressionMode.Identity,
            Warmup = TimeSpan.FromMilliseconds(50),
            Duration = TimeSpan.FromMilliseconds(100),
            Step = new StepPlan(2, 8, 2),
            Profile = WorkloadProfile.Mixed
        };

        using var transport = new TestTransport(baseLatencyMs: 1);
        var workload = new MixedProfileWorkload(WorkloadMix.FromWeights(0, 100, 0), new UniformDistribution(), 1024, seed: 42);
        using var serverTracker = new ServerMetricsTracker(transport, opts);
        var executor = new BenchmarkExecutor(opts, transport, workload, new ProcessCpuTracker(), serverTracker);
        var rng = new Random(42);

        var steps = new List<StepResult>();
        for (int concurrency = 2; concurrency <= 8; concurrency *= 2)
        {
            var generator = new ClosedLoopLoadGenerator(transport, workload, concurrency, rng);
            var (_, step) = await executor.ExecuteStepAsync(generator, steps.Count, concurrency, CancellationToken.None);
            steps.Add(step);
        }

        steps.Count.Should().BeGreaterOrEqualTo(2);
        steps.All(s => s.Throughput >= 0).Should().BeTrue();
        steps.All(s => s.ErrorRate >= 0 && s.ErrorRate <= 1).Should().BeTrue();
        steps.All(s => s.NetworkUtilization >= 0 && s.NetworkUtilization <= 1).Should().BeTrue();
    }

    [Fact]
    public async Task Bulk_Load_Throughput_Counts_Documents_Not_Batches()
    {
        const int documents = 1000;
        const int batchSize = 100;

        using var transport = new TestTransport(baseLatencyMs: 0);
        var workload = new BulkWriteWorkload(docSizeBytes: 1024, batchSize, seed: 42, targetCount: documents);
        var generator = new ClosedLoopLoadGenerator(transport, workload, concurrency: 4, new Random(42));

        var (_, metrics) = await generator.ExecuteMeasurementAsync(TimeSpan.FromSeconds(30), CancellationToken.None);

        metrics.OperationsCompleted.Should().Be(documents / batchSize);
        (metrics.Throughput * metrics.Duration.TotalSeconds).Should().BeApproximately(documents, 1);
    }
    [Fact]
    public async Task Ramp_Reports_Tail_Percentiles_In_Order()
    {
        var opts = new RunOptions
        {
            Url = "http://localhost:10101",
            Database = "ycsb",
            Seed = 1,
            Warmup = TimeSpan.FromMilliseconds(100),
            Duration = TimeSpan.FromMilliseconds(500),
            Shape = LoadShape.Closed,
            Step = new StepPlan(2, 2, 2)
        };
        using var transport = new RareOutlierTransport(every: 20_000);
        var workload = new MixedProfileWorkload(WorkloadMix.FromWeights(100, 0, 0), new UniformDistribution(), 64, seed: 42);
        var executor = new BenchmarkExecutor(opts, transport, workload, new ProcessCpuTracker(), serverTracker: null);

        var step = (await BenchmarkRunner.RunRampAsync(opts, transport, executor, workload, startupCalibration: null, new Random(1))).Steps[^1];

        step.Raw.P99.Should().BeLessOrEqualTo(step.Raw.P999);
        step.Raw.P999.Should().BeLessOrEqualTo(step.P9999);
    }

    // Answers at once except for one slow call in every `every`, rarer than one in 10,000.
    private sealed class RareOutlierTransport(int every) : IYcsbTransport
    {
        private long _calls;
        public string ProductName => "fake";
        public bool ReportsWireBytes => false;
        public string RecordedEndpoint => "stub";

        public async Task<TransportResult> ExecuteAsync(OperationBase op, CancellationToken ct)
        {
            if (Interlocked.Increment(ref _calls) % every == 0)
                await Task.Delay(50, CancellationToken.None);
            return new TransportResult(0, 0);
        }

        public Task PutAsync<T>(string id, T document) => Task.CompletedTask;
        public Task EnsureDatabaseExistsAsync(string databaseName) => Task.CompletedTask;
        public Task<long> GetDocumentCountAsync(string idPrefix) => Task.FromResult(0L);
        public Task<string> GetServerVersionAsync() => Task.FromResult("0");
        public void Dispose() { }
    }
}
