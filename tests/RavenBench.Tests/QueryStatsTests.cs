using System;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using RavenBench.Core;
using RavenBench.Core.Metrics;
using RavenBench.Core.Workload;
using Xunit;

namespace RavenBench.Tests;

public class QueryStatsTests
{
    [Fact]
    public void Aggregates_Index_Usage_Result_Counts_And_Staleness()
    {
        var stats = new QueryStats();
        stats.Record("Idx/A", resultCount: 10, isStale: false);
        stats.Record("Idx/A", resultCount: 0, isStale: true);
        stats.Record("Idx/B", resultCount: 5, isStale: false);

        var snap = stats.Snapshot();

        snap.QueryOperations.Should().Be(3);
        snap.IndexUsage["Idx/A"].Should().Be(2);
        snap.IndexUsage["Idx/B"].Should().Be(1);
        snap.TotalResults.Should().Be(15);
        snap.MinResultCount.Should().Be(0);
        snap.MaxResultCount.Should().Be(10);
        snap.AvgResultCount.Should().Be(5.0);
        snap.StaleQueries.Should().Be(1);
    }

    [Fact]
    public void Non_Query_Operations_Produce_Empty_Snapshot()
    {
        var stats = new QueryStats();
        stats.Record(indexName: null, resultCount: null, isStale: null);

        var snap = stats.Snapshot();

        snap.HasQueries.Should().BeFalse();
        snap.AvgResultCount.Should().BeNull();
        snap.MinResultCount.Should().BeNull();
    }

    [Fact]
    public async Task Production_Step_Captures_Query_Metadata()
    {
        var opts = new RunOptions
        {
            Url = "http://localhost:10101",
            Database = "so",
            Distribution = KeyDistributionKind.Uniform,
            Transport = TransportKind.Raw,
            Compression = CompressionMode.Identity,
            Warmup = TimeSpan.Zero,
            Duration = TimeSpan.FromMilliseconds(100),
            Profile = WorkloadProfile.Mixed
        };

        // An index that returns zero rows is the failure mode result-count guards against.
        using var transport = new TestTransport(baseLatencyMs: 1, indexName: "Auto/Questions", resultCount: 0, isStale: true);
        var workload = new MixedProfileWorkload(WorkloadMix.FromWeights(100, 0, 0), new UniformDistribution(), 1024);
        var executor = new BenchmarkExecutor(opts, transport, workload, new ProcessCpuTracker());
        var generator = new ClosedLoopLoadGenerator(transport, workload, concurrency: 4, new Random(42));

        var (_, step) = await executor.ExecuteStepAsync(generator, 0, 4, CancellationToken.None);

        step.QueryOperations.Should().BeGreaterThan(0);
        step.IndexUsage!.Should().ContainKey("Auto/Questions");
        step.MaxResultCount.Should().Be(0);
        step.AvgResultCount.Should().Be(0.0);
        step.StaleQueryCount.Should().Be(step.QueryOperations);
    }
}
