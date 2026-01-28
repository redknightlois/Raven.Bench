using System;
using System.Linq;
using System.Threading.Tasks;
using FluentAssertions;
using RavenBench.Core.Metrics;
using RavenBench.Core.Reporting;
using RavenBench.Core.Transport;
using RavenBench.Core;
using RavenBench.Core.Workload;
using Xunit;

namespace RavenBench.Tests;

public class IntegrationTests
{
    [Fact]
    public void BenchmarkRun_Structure_Validation()
    {
        // INVARIANT: BenchmarkRun should have proper structure and validation
        // INVARIANT: All metrics should be within reasonable bounds
        
        var result = IntegrationTestHelper.CreateSampleBenchmarkRun(3);
        
        // Validate overall structure
        result.Should().NotBeNull();
        result.Steps.Should().NotBeEmpty();
        result.Steps.Should().HaveCount(3);
        
        // Validate concurrency progression
        var concurrencies = result.Steps.Select(s => s.Concurrency).ToArray();
        concurrencies.Should().BeInAscendingOrder();
        
        // Validate all steps have reasonable metrics
        result.Steps.Should().AllSatisfy(step =>
        {
            step.Throughput.Should().BeGreaterThan(0);
            step.ErrorRate.Should().BeInRange(0, 1);
            step.Raw.P50.Should().BeGreaterThan(0);
            step.Raw.P95.Should().BeGreaterOrEqualTo(step.Raw.P50);
            step.Raw.P99.Should().BeGreaterOrEqualTo(step.Raw.P95);
            step.Normalized.P50.Should().BeGreaterThan(0);
            step.Normalized.P95.Should().BeGreaterOrEqualTo(step.Normalized.P50);
            step.Normalized.P99.Should().BeGreaterOrEqualTo(step.Normalized.P95);

            // Validate raw tail latency metrics
            step.P9999.Should().BeGreaterThan(0, "raw p99.99 should capture extreme tail latency");
            step.PMax.Should().BeGreaterThan(0, "raw pMax should capture worst-case latency");
            step.P9999.Should().BeGreaterOrEqualTo(step.Raw.P999, "p99.99 should be >= p99.9");
            step.PMax.Should().BeGreaterOrEqualTo(step.P9999, "pMax should be >= p99.99");

            // Validate normalized tail latency metrics
            step.NormalizedP9999.Should().BeGreaterThan(0, "normalized p99.99 should capture baseline-adjusted tail latency");
            step.NormalizedPMax.Should().BeGreaterThan(0, "normalized pMax should capture baseline-adjusted worst-case latency");
            step.NormalizedP9999.Should().BeGreaterOrEqualTo(step.Normalized.P999, "normalized p99.99 should be >= normalized p99.9");
            step.NormalizedPMax.Should().BeGreaterOrEqualTo(step.NormalizedP9999, "normalized pMax should be >= normalized p99.99");

            // Validate coordinated omission correction counts
            step.SampleCount.Should().BeGreaterThan(0, "SampleCount should track actual operations");
            step.CorrectedCount.Should().BeGreaterOrEqualTo(step.SampleCount,
                "CorrectedCount should be >= SampleCount when coordinated omission corrections are applied");

            step.ClientCpu.Should().BeGreaterOrEqualTo(0);
            step.NetworkUtilization.Should().BeGreaterOrEqualTo(0);
            step.BytesOut.Should().BeGreaterThan(0);
            step.BytesIn.Should().BeGreaterThan(0);
        });
        
        // Validate transport metadata
        result.ClientCompression.Should().NotBeNullOrEmpty();
        result.EffectiveHttpVersion.Should().NotBeNullOrEmpty();
        result.MaxNetworkUtilization.Should().BeGreaterOrEqualTo(0);
    }
    
    [Fact]
    public async Task Transport_Server_Metrics_Integration()
    {
        // INVARIANT: Transport should provide server metrics correctly
        // INVARIANT: Server metrics should have valid values
        
        using var transport = new TestTransport();
        
        var metrics = await transport.GetServerMetricsAsync();
        
        metrics.Should().NotBeNull();
        metrics.CpuUsagePercent.Should().BeGreaterOrEqualTo(0);
        metrics.MemoryUsageMB.Should().BeGreaterThan(0);
        metrics.ActiveConnections.Should().BeGreaterOrEqualTo(0);
        metrics.RequestsPerSecond.Should().BeGreaterOrEqualTo(0);
        metrics.Timestamp.Should().BeAfter(DateTime.MinValue);
    }
    
    [Fact]
    public void PayloadGeneration_Integration_With_Workload()
    {
        // INVARIANT: PayloadGenerator should integrate correctly with workload system
        // INVARIANT: Generated payloads should match size expectations

        var distribution = new UniformDistribution();
        var workload = new MixedProfileWorkload(
            WorkloadMix.FromWeights(0, 100, 0),
            distribution,
            2048 // 2KB documents
        );

        var rng = new Random(42);

        // Generate several operations to test consistency
        for (int i = 0; i < 10; i++)
        {
            var op = workload.NextOperation(rng);

            op.Should().NotBeNull();
            op.Should().BeOfType<InsertOperation<string>>(); // Should be write since 100% writes

            if (op is InsertOperation<string> insertOp)
            {
                var payloadString = insertOp.Payload;
                payloadString.Should().NotBeNull();

                // Payload should be valid JSON
                var document = System.Text.Json.JsonDocument.Parse(payloadString);
                document.RootElement.ValueKind.Should().Be(System.Text.Json.JsonValueKind.Object);

                // Should be reasonably sized
                var payloadBytes = System.Text.Encoding.UTF8.GetByteCount(payloadString);
                payloadBytes.Should().BeGreaterThan(100);
                payloadBytes.Should().BeLessThan(5000); // Within reasonable bounds for 2KB target
            }
        }
    }

    [Fact]
    public void Query_Profile_Workload_Building_Integration()
    {
        // INVARIANT: BenchmarkRunner should correctly build workloads based on query profiles
        // INVARIANT: Different query profiles should produce different workload types

        var usersMetadata = new UsersWorkloadMetadata
        {
            SampleNames = new[] { "Alice", "Bob" },
            SampleCount = 2,
            TotalUserCount = 100,
            ReputationBuckets = new[]
            {
                new ReputationBucket { MinReputation = 1, MaxReputation = 100, EstimatedDocCount = 50 },
                new ReputationBucket { MinReputation = 100, MaxReputation = 1000, EstimatedDocCount = 50 }
            },
            MinReputation = 1,
            MaxReputation = 1000,
            ComputedAt = DateTime.UtcNow
        };

        var stackOverflowMetadata = new StackOverflowWorkloadMetadata
        {
            QuestionIds = new[] { 1, 2, 3 },
            UserIds = new[] { 1, 2 },
            QuestionCount = 3,
            UserCount = 2,
            TitlePrefixes = new[] { "How", "What" },
            SearchTermsRare = new[] { "algorithm" },
            SearchTermsCommon = new[] { "error", "help" },
            ComputedAt = DateTime.UtcNow
        };

        // Test Users equality workload
        var usersEqualityOpts = new RunOptions { Url = "http://localhost:8080", Database = "test", Profile = WorkloadProfile.QueryUsersByName, QueryProfile = QueryProfile.Equality };
        var usersEqualityWorkload = BenchmarkRunner.BuildWorkload(usersEqualityOpts, stackOverflowMetadata, usersMetadata, null);
        usersEqualityWorkload.Should().BeOfType<UsersByNameQueryWorkload>();

        // Test Users range workload
        var usersRangeOpts = new RunOptions { Url = "http://localhost:8080", Database = "test", Profile = WorkloadProfile.QueryUsersByName, QueryProfile = QueryProfile.Range };
        var usersRangeWorkload = BenchmarkRunner.BuildWorkload(usersRangeOpts, stackOverflowMetadata, usersMetadata, null);
        usersRangeWorkload.Should().BeOfType<UsersRangeQueryWorkload>();

        // Test StackOverflow equality workload (query by id)
        var soEqualityOpts = new RunOptions { Url = "http://localhost:8080", Database = "test", Profile = WorkloadProfile.StackOverflowQueries, QueryProfile = QueryProfile.Equality };
        var soEqualityWorkload = BenchmarkRunner.BuildWorkload(soEqualityOpts, stackOverflowMetadata, usersMetadata, null);
        soEqualityWorkload.Should().BeOfType<StackOverflowQueryWorkload>();

        // Test StackOverflow text prefix workload
        var soPrefixOpts = new RunOptions { Url = "http://localhost:8080", Database = "test", Profile = WorkloadProfile.StackOverflowQueries, QueryProfile = QueryProfile.TextPrefix };
        var soPrefixWorkload = BenchmarkRunner.BuildWorkload(soPrefixOpts, stackOverflowMetadata, usersMetadata, null);
        soPrefixWorkload.Should().BeOfType<QuestionsByTitlePrefixWorkload>();

        // Test StackOverflow text search workload
        var soSearchOpts = new RunOptions { Url = "http://localhost:8080", Database = "test", Profile = WorkloadProfile.StackOverflowQueries, QueryProfile = QueryProfile.TextSearch };
        var soSearchWorkload = BenchmarkRunner.BuildWorkload(soSearchOpts, stackOverflowMetadata, usersMetadata, null);
        soSearchWorkload.Should().BeOfType<QuestionsByTitleSearchWorkload>();
    }

    [Fact]
    public void Query_Profile_Operations_Generation_Integration()
    {
        // INVARIANT: Workloads should generate appropriate operations for their query profiles
        // INVARIANT: Operations should have correct query text and parameters

        var usersMetadata = new UsersWorkloadMetadata
        {
            SampleNames = new[] { "Alice", "Bob" },
            SampleCount = 2,
            TotalUserCount = 100,
            ReputationBuckets = new[]
            {
                new ReputationBucket { MinReputation = 10, MaxReputation = 100, EstimatedDocCount = 50 }
            },
            MinReputation = 10,
            MaxReputation = 100,
            ComputedAt = DateTime.UtcNow
        };

        var stackOverflowMetadata = new StackOverflowWorkloadMetadata
        {
            QuestionIds = new[] { 1, 2, 3 },
            UserIds = new[] { 1, 2 },
            QuestionCount = 3,
            UserCount = 2,
            TitlePrefixes = new[] { "How", "What" },
            SearchTermsRare = new[] { "algorithm" },
            SearchTermsCommon = new[] { "error", "help" },
            ComputedAt = DateTime.UtcNow
        };

        var rng = new Random(42);

        // Test Users equality query
        var usersEqualityWorkload = new UsersByNameQueryWorkload(usersMetadata);
        var eqOp = usersEqualityWorkload.NextOperation(rng);
        eqOp.Should().BeOfType<QueryOperation>();
        var eqQueryOp = (QueryOperation)eqOp;
        eqQueryOp.QueryText.Should().Be("from Users where DisplayName = $name");
        eqQueryOp.Parameters.Should().ContainKey("name");

        // Test Users range query
        var usersRangeWorkload = new UsersRangeQueryWorkload(usersMetadata);
        var rangeOp = usersRangeWorkload.NextOperation(rng);
        rangeOp.Should().BeOfType<QueryOperation>();
        var rangeQueryOp = (QueryOperation)rangeOp;
        rangeQueryOp.QueryText.Should().Be("from Users where Reputation between $min and $max");
        rangeQueryOp.Parameters.Should().ContainKey("min");
        rangeQueryOp.Parameters.Should().ContainKey("max");

        // Test StackOverflow text prefix query
        var prefixWorkload = new QuestionsByTitlePrefixWorkload(stackOverflowMetadata);
        var prefixOp = prefixWorkload.NextOperation(rng);
        prefixOp.Should().BeOfType<QueryOperation>();
        var prefixQueryOp = (QueryOperation)prefixOp;
        prefixQueryOp.QueryText.Should().Be("from questions where startsWith(Title, $prefix)");
        prefixQueryOp.Parameters.Should().ContainKey("prefix");

        // Test StackOverflow text search query
        var searchWorkload = new QuestionsByTitleSearchWorkload(stackOverflowMetadata);
        var searchOp = searchWorkload.NextOperation(rng);
        searchOp.Should().BeOfType<QueryOperation>();
        var searchQueryOp = (QueryOperation)searchOp;
        searchQueryOp.QueryText.Should().Be("from questions where search(Title, $term)");
        searchQueryOp.Parameters.Should().ContainKey("term");
    }

    [Fact]
    public void BuildWorkload_Throws_On_Invalid_Query_Profile_Combinations()
    {
        // INVARIANT: BuildWorkload should throw NotSupportedException for unsupported query profile combinations
        // INVARIANT: Error messages should clearly indicate supported profiles for each workload type

        // StackOverflow queries do not support range queries
        var soRangeOpts = new RunOptions { Url = "http://localhost:8080", Database = "test", Profile = WorkloadProfile.StackOverflowQueries, QueryProfile = QueryProfile.Range };
        var act1 = () => BenchmarkRunner.BuildWorkload(soRangeOpts, null, null, null);
        act1.Should().Throw<NotSupportedException>()
            .WithMessage("Query profile 'Range' is not supported for StackOverflow queries. Supported profiles: equality, text-prefix, text-search, text-search-rare, text-search-common, text-search-mixed");

        // StackOverflow queries do not support text-prefix for users profile
        var usersPrefixOpts = new RunOptions { Url = "http://localhost:8080", Database = "test", Profile = WorkloadProfile.QueryUsersByName, QueryProfile = QueryProfile.TextPrefix };
        var act2 = () => BenchmarkRunner.BuildWorkload(usersPrefixOpts, null, null, null);
        act2.Should().Throw<NotSupportedException>()
            .WithMessage("Query profile 'TextPrefix' is not supported for Users queries. Supported profiles: equality, range");

        // StackOverflow queries do not support text-search for users profile
        var usersSearchOpts = new RunOptions { Url = "http://localhost:8080", Database = "test", Profile = WorkloadProfile.QueryUsersByName, QueryProfile = QueryProfile.TextSearch };
        var act3 = () => BenchmarkRunner.BuildWorkload(usersSearchOpts, null, null, null);
        act3.Should().Throw<NotSupportedException>()
            .WithMessage("Query profile 'TextSearch' is not supported for Users queries. Supported profiles: equality, range");

        // StackOverflow queries support text-prefix
        var soTextPrefixOpts = new RunOptions { Url = "http://localhost:8080", Database = "test", Profile = WorkloadProfile.StackOverflowQueries, QueryProfile = QueryProfile.TextPrefix };
        var validMetadata = new StackOverflowWorkloadMetadata
        {
            TitlePrefixes = new[] { "How", "What" },
            SearchTermsRare = new[] { "algorithm" },
            SearchTermsCommon = new[] { "error" },
            QuestionIds = new[] { 1, 2 },
            UserIds = new[] { 1 },
            QuestionCount = 2,
            UserCount = 1,
            ComputedAt = DateTime.UtcNow
        };
        var workload = BenchmarkRunner.BuildWorkload(soTextPrefixOpts, validMetadata, null, null);
        workload.Should().BeOfType<QuestionsByTitlePrefixWorkload>();
    }
    
    private static RunOptions CreateBasicOptions() => new()
    {
        Url = "http://localhost:8080",
        Database = "test", 
        Writes = 100,
        Distribution = "uniform",
        Compression = "identity",
        DocumentSizeBytes = 1024,
        Warmup = TimeSpan.FromMilliseconds(25),
        Duration = TimeSpan.FromMilliseconds(50),
        Step = new StepPlan(2, 2, 1),
        Profile = WorkloadProfile.Mixed
    };
}

// Simple integration test that just validates basic workflow components
internal static class IntegrationTestHelper
{
    public static BenchmarkRun CreateSampleBenchmarkRun(int stepCount = 2)
    {
        var steps = new System.Collections.Generic.List<StepResult>();
        
        for (int i = 0; i < stepCount; i++)
        {
            var concurrency = (i + 1) * 2; // 2, 4, 6, etc.
            steps.Add(new StepResult
            {
                Concurrency = concurrency,
                Throughput = 100.0 + i * 50, // Increasing throughput
                ErrorRate = 0.01, // 1% error rate
                BytesOut = 1000 + i * 100,
                BytesIn = 800 + i * 80,
                SampleCount = 1000 + i * 500, // Actual operations observed
                Raw = new Percentiles(10.0 + i, 12.5 + i, 15.0 + i, 20.0 + i, 30.0 + i, 40.0 + i),
                Normalized = new Percentiles(9.0 + i, 11.5 + i, 14.0 + i, 18.0 + i, 28.0 + i, 38.0 + i),
                P9999 = 50.0 + i * 5, // p99.99 raw tail latency
                PMax = 60.0 + i * 10, // Maximum raw latency
                NormalizedP9999 = 48.0 + i * 5, // Normalized p99.99 (baseline-adjusted)
                NormalizedPMax = 58.0 + i * 10, // Normalized pMax (baseline-adjusted)
                CorrectedCount = 1050 + i * 520, // TotalCount including CO corrections (slightly higher than SampleCount)
                ClientCpu = 0.25 + i * 0.1,
                NetworkUtilization = 0.1 + i * 0.05
            });
        }
        
        return new BenchmarkRun
        {
            Steps = steps,
            MaxNetworkUtilization = steps.Max(s => s.NetworkUtilization),
            ClientCompression = "identity",
            EffectiveHttpVersion = "1.1"
        };
    }
}

// Note: StubTransport is defined in ServerMetricsTrackerTests.cs and reused here
