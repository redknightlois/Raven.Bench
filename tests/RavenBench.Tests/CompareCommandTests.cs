using System;
using System.Collections.Generic;
using System.Linq;
using RavenBench.Core;
using RavenBench.Core.Reporting;
using RavenBench.Reporter.Commands;
using RavenBench.Reporter.Models;
using Xunit;

namespace RavenBench.Tests;

public class CompareCommandTests
{
    [Fact]
    public void ComparisonModelBuilder_Build_WithTwoSummaries_Succeeds()
    {
        var summaries = CreateCompatibleSummaries(2);
        var labels = new List<string> { "Run A", "Run B" };

        var model = ComparisonModelBuilder.Build(summaries, labels);

        // The model should have a baseline and a list of contenders
        Assert.Equal("Run A", model.Baseline.Label);
        Assert.Single(model.Contenders);
        Assert.Equal("Run B", model.Contenders[0].Label);

        // Should have latency contrasts between the runs
        Assert.NotEmpty(model.LatencyContrasts);

        // Should have aligned concurrency snapshots
        Assert.NotEmpty(model.AlignedSteps);
    }

    [Fact]
    public void ComparisonModelBuilder_Build_WithThreeSummaries_Succeeds()
    {
        var summaries = CreateCompatibleSummaries(3);
        var labels = new List<string> { "Run A", "Run B", "Run C" };

        var model = ComparisonModelBuilder.Build(summaries, labels);

        Assert.Equal("Run A", model.Baseline.Label);
        Assert.Equal(2, model.Contenders.Count);
    }

    [Fact]
    public void ComparisonModelBuilder_Build_IncompatibleSummaries_Throws()
    {
        // Different workload profiles should be incompatible
        var summaries = new List<BenchmarkSummary>
        {
            CreateSummary(WorkloadProfile.Reads),
            CreateSummary(WorkloadProfile.Writes)
        };
        var labels = new List<string> { "Reads", "Writes" };

        Assert.Throws<InvalidOperationException>(() => ComparisonModelBuilder.Build(summaries, labels));
    }

    [Fact]
    public void ComparisonModelBuilder_Build_DifferentTransports_ShouldSucceed()
    {
        // INVARIANT: Different transports (client vs raw) should be comparable
        // as long as the workload profile and dataset are the same.
        var summary1 = CreateSummaryWithTransport(WorkloadProfile.Writes, "raw");
        var summary2 = CreateSummaryWithTransport(WorkloadProfile.Writes, "client");
        var summaries = new List<BenchmarkSummary> { summary1, summary2 };
        var labels = new List<string> { "Raw", "Client" };

        // This should NOT throw - comparing different transports is a primary use case
        var model = ComparisonModelBuilder.Build(summaries, labels);

        Assert.Equal("Raw", model.Baseline.Label);
        Assert.Single(model.Contenders);
        Assert.Equal("Client", model.Contenders[0].Label);
    }

    [Fact]
    public void ComparisonModelBuilder_Build_DifferentHttpVersions_ShouldSucceed()
    {
        // INVARIANT: Different HTTP versions (HTTP/1 vs HTTP/2/3) should be comparable
        // as long as the workload profile and dataset are the same.
        var summary1 = CreateSummaryWithHttpVersion(WorkloadProfile.Reads, "1.1");
        var summary2 = CreateSummaryWithHttpVersion(WorkloadProfile.Reads, "2.0");
        var summaries = new List<BenchmarkSummary> { summary1, summary2 };
        var labels = new List<string> { "HTTP/1.1", "HTTP/2.0" };

        // This should NOT throw - comparing different HTTP versions is a primary use case
        var model = ComparisonModelBuilder.Build(summaries, labels);

        Assert.Equal("HTTP/1.1", model.Baseline.Label);
        Assert.Single(model.Contenders);
        Assert.Equal("HTTP/2.0", model.Contenders[0].Label);
    }

    [Fact]
    public void ComparisonModelBuilder_Build_LabelCountMismatch_Throws()
    {
        var summaries = CreateCompatibleSummaries(2);
        var labels = new List<string> { "Only one label" };

        Assert.Throws<ArgumentException>(() => ComparisonModelBuilder.Build(summaries, labels));
    }

    [Fact]
    public void ComparisonModelBuilder_Build_LessThanTwoSummaries_Throws()
    {
        var summaries = CreateCompatibleSummaries(1);
        var labels = new List<string> { "Single" };

        Assert.Throws<ArgumentException>(() => ComparisonModelBuilder.Build(summaries, labels));
    }

    [Fact]
    public void ComparisonModelBuilder_Build_MoreThanThreeSummaries_Succeeds()
    {
        // The comparison builder now supports 2+ summaries (no upper limit)
        var summaries = CreateCompatibleSummaries(4);
        var labels = new List<string> { "A", "B", "C", "D" };

        var model = ComparisonModelBuilder.Build(summaries, labels);

        Assert.NotNull(model);
        Assert.Equal("A", model.Baseline.Label);
        Assert.Equal(3, model.Contenders.Count);
    }

    [Fact]
    public void ComparisonModelBuilder_Build_CalculatesQualityScoreCorrectly()
    {
        // Quality score = throughput / p999
        // Step 1: concurrency=8, throughput=1000, p999=10 => score = 100
        // Step 2: concurrency=16, throughput=1500, p999=20 => score = 75
        // Best should be step 1 (concurrency 8 with higher score)
        var summary1 = CreateSummaryWithSteps(new[]
        {
            CreateStepResult(8, 1000, p99: 8, p999: 10),
            CreateStepResult(16, 1500, p99: 18, p999: 20)
        });
        var summary2 = CreateSummaryWithSteps(new[]
        {
            CreateStepResult(8, 1100, p99: 10, p999: 12),
            CreateStepResult(16, 1600, p99: 15, p999: 17)
        });
        var summaries = new List<BenchmarkSummary> { summary1, summary2 };
        var labels = new List<string> { "Test1", "Test2" };

        var model = ComparisonModelBuilder.Build(summaries, labels);

        // Quality score = throughput / p999, so best step should be concurrency 8 with score 100
        Assert.Equal(8, model.Baseline.BestStep?.Concurrency);
        Assert.Equal(100, model.Baseline.BestQualityScore);
    }

    [Fact]
    public void ComparisonModelBuilder_Build_HandlesMissingConcurrencyLevels()
    {
        var summary1 = CreateSummaryWithSteps(new[]
        {
            CreateStepResult(8, 1000, p99: 10, p999: 10),
            CreateStepResult(16, 1500, p99: 15, p999: 15)
        });
        var summary2 = CreateSummaryWithSteps(new[]
        {
            CreateStepResult(8, 1100, p99: 12, p999: 12),
            CreateStepResult(32, 1600, p99: 18, p999: 18)
        });
        var summaries = new List<BenchmarkSummary> { summary1, summary2 };
        var labels = new List<string> { "Run A", "Run B" };

        var model = ComparisonModelBuilder.Build(summaries, labels);

        // Should have snapshots for concurrencies 8, 16, 32
        Assert.Equal(3, model.AlignedSteps.Count);

        var concurrency8 = model.AlignedSteps.First(s => s.Concurrency == 8);
        // Index 0 is baseline (Run A), index 1 is contender (Run B)
        Assert.Equal(1000, concurrency8.RunMetrics[0]?.Throughput);
        Assert.Equal(1100, concurrency8.RunMetrics[1]?.Throughput);

        var concurrency16 = model.AlignedSteps.First(s => s.Concurrency == 16);
        Assert.Equal(1500, concurrency16.RunMetrics[0]?.Throughput);
        Assert.Null(concurrency16.RunMetrics[1]); // Run B doesn't have concurrency 16

        var concurrency32 = model.AlignedSteps.First(s => s.Concurrency == 32);
        Assert.Null(concurrency32.RunMetrics[0]); // Run A doesn't have concurrency 32
        Assert.Equal(1600, concurrency32.RunMetrics[1]?.Throughput);
    }

    [Fact]
    public void ComparisonModelBuilder_Build_CalculatesDeltasCorrectly()
    {
        var summary1 = CreateSummaryWithSteps(new[]
        {
            CreateStepResult(16, 1000, p99: 10, p999: 10, errorRate: 0.01, serverCpu: 50, serverMemoryMB: 1000)
        });
        var summary2 = CreateSummaryWithSteps(new[]
        {
            CreateStepResult(16, 1200, p99: 8, p999: 8, errorRate: 0.005, serverCpu: 45, serverMemoryMB: 950)
        });
        var summaries = new List<BenchmarkSummary> { summary1, summary2 };
        var labels = new List<string> { "Baseline", "Improved" };

        var model = ComparisonModelBuilder.Build(summaries, labels);

        // Find throughput contrast (best vs best)
        var throughputContrast = model.ThroughputContrasts
            .FirstOrDefault(c => c.Context == "Best vs Best");
        Assert.NotNull(throughputContrast);

        // Throughput: 1000 -> 1200 = +200 absolute, +20% relative
        Assert.Equal(1000, throughputContrast.BaselineValue);
        Assert.Equal(1200, throughputContrast.ContenderValue);
        Assert.Equal(200, throughputContrast.AbsoluteDelta);
        Assert.Equal(20, throughputContrast.PercentageDelta);

        // Find latency contrast (best vs best)
        var latencyContrast = model.LatencyContrasts
            .FirstOrDefault(c => c.Context == "Best vs Best");
        Assert.NotNull(latencyContrast);

        // P99: 10 -> 8 = -2 absolute, -20% relative (lower latency is better)
        Assert.Equal(10, latencyContrast.BaselineValue);
        Assert.Equal(8, latencyContrast.ContenderValue);
        Assert.Equal(-2, latencyContrast.AbsoluteDelta);
        Assert.Equal(-20, latencyContrast.PercentageDelta);

        // Find error rate contrast
        var errorRateContrast = model.ErrorRateContrasts
            .FirstOrDefault(c => c.Context == "Best vs Best");
        Assert.NotNull(errorRateContrast);

        // Error rate: 1% -> 0.5% = -0.5 absolute, -50% relative
        // Note: ErrorRate in StepResult is a fraction (0.01), but contrast shows percentage (1.0)
        Assert.Equal(1.0, errorRateContrast.BaselineValue);
        Assert.Equal(0.5, errorRateContrast.ContenderValue);
        Assert.Equal(-0.5, errorRateContrast.AbsoluteDelta);
        Assert.Equal(-50, errorRateContrast.PercentageDelta);
    }

    [Fact]
    public void ComparisonModelBuilder_Build_GeneratesKeyTakeaways()
    {
        var summary1 = CreateSummaryWithSteps(new[]
        {
            CreateStepResult(16, 1000, p99: 10, p999: 10, errorRate: 0.01)
        });
        var summary2 = CreateSummaryWithSteps(new[]
        {
            CreateStepResult(16, 1200, p99: 8, p999: 8, errorRate: 0.005)
        });
        var summaries = new List<BenchmarkSummary> { summary1, summary2 };
        var labels = new List<string> { "Baseline", "Improved" };

        var model = ComparisonModelBuilder.Build(summaries, labels);

        Assert.NotEmpty(model.KeyTakeaways);
        // Should contain throughput improvement
        Assert.Contains(model.KeyTakeaways, t => t.Contains("throughput improvement"));
        // Should contain latency improvement
        Assert.Contains(model.KeyTakeaways, t => t.Contains("latency improvement"));
    }

    [Fact]
    public void ComparisonModelBuilder_Build_HandlesZeroP999Gracefully()
    {
        // Test case: if P999 is zero or invalid, that step should be skipped in quality score calculation
        // This tests the robustness of the quality score calculation (throughput / p999)
        var summary1 = CreateSummaryWithSteps(new[]
        {
            CreateStepResult(8, 1000, p99: 0, p999: 0), // Invalid step - should be skipped
            CreateStepResult(16, 1200, p99: 10, p999: 12) // Valid step
        });
        var summary2 = CreateSummaryWithSteps(new[]
        {
            CreateStepResult(8, 1100, p99: 8, p999: 10),
            CreateStepResult(16, 1300, p99: 11, p999: 13)
        });
        var summaries = new List<BenchmarkSummary> { summary1, summary2 };
        var labels = new List<string> { "Run A", "Run B" };

        var model = ComparisonModelBuilder.Build(summaries, labels);

        // For Run A: The best step should be concurrency 16 (the valid one), not concurrency 8
        Assert.NotNull(model.Baseline.BestStep);
        Assert.Equal(16, model.Baseline.BestStep.Concurrency);
    }

    [Fact]
    public void ComparisonModelBuilder_Build_HandlesAllStepsWithZeroP999()
    {
        // Edge case: all steps have zero P999 - what happens?
        // In this case, both BestStep and SecondBestStep should be null
        var summary1 = CreateSummaryWithSteps(new[]
        {
            CreateStepResult(8, 1000, p99: 0, p999: 0),
            CreateStepResult(16, 1200, p99: 0, p999: 0)
        });
        var summary2 = CreateSummaryWithSteps(new[]
        {
            CreateStepResult(8, 1100, p99: 0, p999: 0),
            CreateStepResult(16, 1300, p99: 0, p999: 0)
        });
        var summaries = new List<BenchmarkSummary> { summary1, summary2 };
        var labels = new List<string> { "Run A", "Run B" };

        var model = ComparisonModelBuilder.Build(summaries, labels);

        // When all steps have invalid P999, best step should be null
        Assert.Null(model.Baseline.BestStep);
        Assert.Null(model.Baseline.SecondBestStep);

        // Latency contrasts should be empty or handle null steps gracefully
        // The current implementation only creates contrasts when both best steps exist
        Assert.Empty(model.LatencyContrasts);
    }

    [Fact]
    public void ComparisonModelBuilder_Build_HandlesDivisionByZeroInPercentageDelta()
    {
        // Edge case: baseline value is zero, percentage delta calculation should handle division by zero
        // For example, if baseline error rate is 0 and contender error rate is 0.01
        var summary1 = CreateSummaryWithSteps(new[]
        {
            CreateStepResult(16, 1000, p99: 10, p999: 10, errorRate: 0.0) // Zero error rate
        });
        var summary2 = CreateSummaryWithSteps(new[]
        {
            CreateStepResult(16, 1200, p99: 8, p999: 8, errorRate: 0.01) // Non-zero error rate
        });
        var summaries = new List<BenchmarkSummary> { summary1, summary2 };
        var labels = new List<string> { "Baseline", "Contender" };

        var model = ComparisonModelBuilder.Build(summaries, labels);

        // Find error rate contrast
        var errorRateContrast = model.ErrorRateContrasts
            .FirstOrDefault(c => c.Context == "Best vs Best");
        Assert.NotNull(errorRateContrast);

        // Baseline error rate is 0%, contender is 1%
        Assert.Equal(0.0, errorRateContrast.BaselineValue);
        Assert.Equal(1.0, errorRateContrast.ContenderValue);
        Assert.Equal(1.0, errorRateContrast.AbsoluteDelta);

        // Percentage delta should be 0 when baseline is 0 (to avoid division by zero)
        // This is the defensive behavior - an alternative would be to use infinity or a special sentinel
        Assert.Equal(0, errorRateContrast.PercentageDelta);
    }

    private static List<BenchmarkSummary> CreateCompatibleSummaries(int count)
    {
        var summaries = new List<BenchmarkSummary>();
        for (int i = 0; i < count; i++)
        {
            summaries.Add(CreateSummary(WorkloadProfile.Reads));
        }
        return summaries;
    }

    private static BenchmarkSummary CreateSummary(WorkloadProfile profile)
    {
        return new BenchmarkSummary
        {
            Options = new RunOptions
            {
                Url = "http://localhost:8080",
                Database = "test",
                Profile = profile,
                Dataset = "test",
                Transport = "raw",
                QueryProfile = QueryProfile.Equality
            },
            EffectiveHttpVersion = "1.1",
            Steps = new List<StepResult>
            {
                CreateStepResult(16, 1000 + Random.Shared.Next(100), p99: 10 + Random.Shared.Next(5), p999: 10 + Random.Shared.Next(5))
            },
            Verdict = "Passed",
            ClientCompression = "identity"
        };
    }

    private static BenchmarkSummary CreateSummaryWithTransport(WorkloadProfile profile, string transport)
    {
        return new BenchmarkSummary
        {
            Options = new RunOptions
            {
                Url = "http://localhost:8080",
                Database = "test",
                Profile = profile,
                Dataset = "test",
                Transport = transport,
                QueryProfile = QueryProfile.Equality
            },
            EffectiveHttpVersion = "1.1",
            Steps = new List<StepResult>
            {
                CreateStepResult(16, 1000, p99: 10, p999: 10)
            },
            Verdict = "Passed",
            ClientCompression = "identity"
        };
    }

    private static BenchmarkSummary CreateSummaryWithHttpVersion(WorkloadProfile profile, string httpVersion)
    {
        return new BenchmarkSummary
        {
            Options = new RunOptions
            {
                Url = "http://localhost:8080",
                Database = "test",
                Profile = profile,
                Dataset = "test",
                Transport = "client",
                QueryProfile = QueryProfile.Equality
            },
            EffectiveHttpVersion = httpVersion,
            Steps = new List<StepResult>
            {
                CreateStepResult(16, 1000, p99: 10, p999: 10)
            },
            Verdict = "Passed",
            ClientCompression = "identity"
        };
    }

    private static BenchmarkSummary CreateSummaryWithSteps(StepResult[] steps)
    {
        return new BenchmarkSummary
        {
            Options = new RunOptions
            {
                Url = "http://localhost:8080",
                Database = "test",
                Profile = WorkloadProfile.Reads,
                Dataset = "test",
                Transport = "raw",
                QueryProfile = QueryProfile.Equality
            },
            EffectiveHttpVersion = "1.1",
            Steps = steps.ToList(),
            Verdict = "Passed",
            ClientCompression = "identity"
        };
    }

    private static StepResult CreateStepResult(
        int concurrency,
        double throughput,
        double p99,
        double p999,
        double errorRate = 0.01,
        double serverCpu = 50,
        long serverMemoryMB = 1000)
    {
        return new StepResult
        {
            Concurrency = concurrency,
            Throughput = throughput,
            // P50, P75, P90, P95, P99, P999
            Raw = new Percentiles(1, 1, 1, 1, p99, p999),
            ErrorRate = errorRate,
            ServerCpu = serverCpu,
            ServerMemoryMB = serverMemoryMB
        };
    }
}
