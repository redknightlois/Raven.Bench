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
    public void PayloadGeneration_Integration_With_Workload()
    {
        // INVARIANT: PayloadGenerator should integrate correctly with workload system
        // INVARIANT: Generated payloads should match size expectations

        var distribution = new UniformDistribution();
        var workload = new MixedProfileWorkload(
            WorkloadMix.FromWeights(0, 100, 0),
            distribution,
            2048, // 2KB documents
            seed: 42
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

        var usersMetadata = new StackOverflowUsersWorkloadMetadata
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
            ComputedAt = DateTime.UtcNow,
            DisplayNameIndexName = "Users/ByDisplayName-corax",
            ReputationIndexName = "Users/ByReputation-corax"
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
            ComputedAt = DateTime.UtcNow,
            TitleIndexName = "Questions/ByTitle-corax",
            TitleSearchIndexName = "Questions/ByTitleSearch-corax"
        };

        // Test Users VoronEquality workload
        var usersVoronEqualityOpts = new RunOptions { Url = "http://localhost:8080", Database = "test", Profile = WorkloadProfile.QueryUsersByName, QueryProfile = QueryProfile.VoronEquality };
        var usersVoronEqualityWorkload = WorkloadFactory.BuildWorkload(usersVoronEqualityOpts, stackOverflowMetadata, usersMetadata, null);
        usersVoronEqualityWorkload.Should().BeOfType<StackOverflowUsersByNameQueryWorkload>();

        // Test Users IndexEquality workload
        var usersIndexEqualityOpts = new RunOptions { Url = "http://localhost:8080", Database = "test", Profile = WorkloadProfile.QueryUsersByName, QueryProfile = QueryProfile.IndexEquality };
        var usersIndexEqualityWorkload = WorkloadFactory.BuildWorkload(usersIndexEqualityOpts, stackOverflowMetadata, usersMetadata, null);
        usersIndexEqualityWorkload.Should().BeOfType<StackOverflowUsersByNameQueryWorkload>();

        // Test Users range workload
        var usersRangeOpts = new RunOptions { Url = "http://localhost:8080", Database = "test", Profile = WorkloadProfile.QueryUsersByName, QueryProfile = QueryProfile.Range };
        var usersRangeWorkload = WorkloadFactory.BuildWorkload(usersRangeOpts, stackOverflowMetadata, usersMetadata, null);
        usersRangeWorkload.Should().BeOfType<StackOverflowUsersRangeQueryWorkload>();

        // Test StackOverflow VoronEquality workload (direct Voron lookup via id())
        var soVoronEqualityOpts = new RunOptions { Url = "http://localhost:8080", Database = "test", Profile = WorkloadProfile.StackOverflowTextSearch, QueryProfile = QueryProfile.VoronEquality };
        var soVoronEqualityWorkload = WorkloadFactory.BuildWorkload(soVoronEqualityOpts, stackOverflowMetadata, usersMetadata, null);
        soVoronEqualityWorkload.Should().BeOfType<StackOverflowQueryWorkload>();

        // Test StackOverflow IndexEquality workload (index-based lookup)
        var soIndexEqualityOpts = new RunOptions { Url = "http://localhost:8080", Database = "test", Profile = WorkloadProfile.StackOverflowTextSearch, QueryProfile = QueryProfile.IndexEquality };
        var soIndexEqualityWorkload = WorkloadFactory.BuildWorkload(soIndexEqualityOpts, stackOverflowMetadata, usersMetadata, null);
        soIndexEqualityWorkload.Should().BeOfType<StackOverflowQueryWorkload>();

        // Test StackOverflow text prefix workload
        var soPrefixOpts = new RunOptions { Url = "http://localhost:8080", Database = "test", Profile = WorkloadProfile.StackOverflowTextSearch, QueryProfile = QueryProfile.TextPrefix };
        var soPrefixWorkload = WorkloadFactory.BuildWorkload(soPrefixOpts, stackOverflowMetadata, usersMetadata, null);
        soPrefixWorkload.Should().BeOfType<QuestionsByTitlePrefixWorkload>();

        // Test StackOverflow text search workload
        var soSearchOpts = new RunOptions { Url = "http://localhost:8080", Database = "test", Profile = WorkloadProfile.StackOverflowTextSearch, QueryProfile = QueryProfile.TextSearch };
        var soSearchWorkload = WorkloadFactory.BuildWorkload(soSearchOpts, stackOverflowMetadata, usersMetadata, null);
        soSearchWorkload.Should().BeOfType<QuestionsByTitleSearchWorkload>();
    }

    [Fact]
    public void Query_Profile_Operations_Generation_Integration()
    {
        // INVARIANT: Workloads should generate appropriate operations for their query profiles
        // INVARIANT: Operations should have correct query text and parameters

        var usersMetadata = new StackOverflowUsersWorkloadMetadata
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
            ComputedAt = DateTime.UtcNow,
            DisplayNameIndexName = "Users/ByDisplayName-corax",
            ReputationIndexName = "Users/ByReputation-corax"
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
            ComputedAt = DateTime.UtcNow,
            TitleIndexName = "Questions/ByTitle-corax",
            TitleSearchIndexName = "Questions/ByTitleSearch-corax"
        };

        var rng = new Random(42);

        // Test Users equality query
        var usersEqualityWorkload = new StackOverflowUsersByNameQueryWorkload(usersMetadata);
        var eqOp = usersEqualityWorkload.NextOperation(rng);
        eqOp.Should().BeOfType<QueryOperation>();
        var eqQueryOp = (QueryOperation)eqOp;
        eqQueryOp.QueryText.Should().Be("from index 'Users/ByDisplayName-corax' where DisplayName = $name");
        eqQueryOp.Parameters.Should().ContainKey("name");

        // Test Users range query
        var usersRangeWorkload = new StackOverflowUsersRangeQueryWorkload(usersMetadata);
        var rangeOp = usersRangeWorkload.NextOperation(rng);
        rangeOp.Should().BeOfType<QueryOperation>();
        var rangeQueryOp = (QueryOperation)rangeOp;
        rangeQueryOp.QueryText.Should().Be("from index 'Users/ByReputation-corax' where Reputation between $min and $max");
        rangeQueryOp.Parameters.Should().ContainKey("min");
        rangeQueryOp.Parameters.Should().ContainKey("max");

        // Test StackOverflow text prefix query
        var prefixWorkload = new QuestionsByTitlePrefixWorkload(stackOverflowMetadata);
        var prefixOp = prefixWorkload.NextOperation(rng);
        prefixOp.Should().BeOfType<QueryOperation>();
        var prefixQueryOp = (QueryOperation)prefixOp;
        prefixQueryOp.QueryText.Should().Be("from index 'Questions/ByTitle-corax' where startsWith(Title, $prefix) limit 16");
        prefixQueryOp.Parameters.Should().ContainKey("prefix");

        // Test StackOverflow text search query
        var searchWorkload = new QuestionsByTitleSearchWorkload(stackOverflowMetadata);
        var searchOp = searchWorkload.NextOperation(rng);
        searchOp.Should().BeOfType<QueryOperation>();
        var searchQueryOp = (QueryOperation)searchOp;
        searchQueryOp.QueryText.Should().Be("from index 'Questions/ByTitleSearch-corax' where search(Title, $term) limit 16");
        searchQueryOp.Parameters.Should().ContainKey("term");
    }

    [Fact]
    public void BuildWorkload_Throws_On_Invalid_Query_Profile_Combinations()
    {
        // INVARIANT: BuildWorkload should throw NotSupportedException for unsupported query profile combinations
        // INVARIANT: Error messages should clearly indicate supported profiles for each workload type

        // StackOverflow queries do not support range queries
        var soRangeOpts = new RunOptions { Url = "http://localhost:8080", Database = "test", Profile = WorkloadProfile.StackOverflowTextSearch, QueryProfile = QueryProfile.Range };
        var act1 = () => WorkloadFactory.BuildWorkload(soRangeOpts, null, null, null);
        act1.Should().Throw<NotSupportedException>()
            .WithMessage("Query profile 'Range' is not supported for StackOverflow queries. Supported profiles: voron-equality, index-equality, text-prefix, text-search, text-search-rare, text-search-common, text-search-mixed, suggestions, more-like-this, group-by, stream");

        // StackOverflow queries do not support text-prefix for users profile
        var usersPrefixOpts = new RunOptions { Url = "http://localhost:8080", Database = "test", Profile = WorkloadProfile.QueryUsersByName, QueryProfile = QueryProfile.TextPrefix };
        var act2 = () => WorkloadFactory.BuildWorkload(usersPrefixOpts, null, null, null);
        act2.Should().Throw<NotSupportedException>()
            .WithMessage("Query profile 'TextPrefix' is not supported for Users queries. Supported profiles: voron-equality, index-equality, range, spatial");

        // StackOverflow queries do not support text-search for users profile
        var usersSearchOpts = new RunOptions { Url = "http://localhost:8080", Database = "test", Profile = WorkloadProfile.QueryUsersByName, QueryProfile = QueryProfile.TextSearch };
        var act3 = () => WorkloadFactory.BuildWorkload(usersSearchOpts, null, null, null);
        act3.Should().Throw<NotSupportedException>()
            .WithMessage("Query profile 'TextSearch' is not supported for Users queries. Supported profiles: voron-equality, index-equality, range, spatial");

        // StackOverflow queries support text-prefix
        var soTextPrefixOpts = new RunOptions { Url = "http://localhost:8080", Database = "test", Profile = WorkloadProfile.StackOverflowTextSearch, QueryProfile = QueryProfile.TextPrefix };
        var validMetadata = new StackOverflowWorkloadMetadata
        {
            TitlePrefixes = new[] { "How", "What" },
            SearchTermsRare = new[] { "algorithm" },
            SearchTermsCommon = new[] { "error" },
            QuestionIds = new[] { 1, 2 },
            UserIds = new[] { 1 },
            QuestionCount = 2,
            UserCount = 1,
            ComputedAt = DateTime.UtcNow,
            TitleIndexName = "Questions/ByTitle-corax",
            TitleSearchIndexName = "Questions/ByTitleSearch-corax"
        };
        var workload = WorkloadFactory.BuildWorkload(soTextPrefixOpts, validMetadata, null, null);
        workload.Should().BeOfType<QuestionsByTitlePrefixWorkload>();
    }
    
    private static RunOptions CreateBasicOptions() => new()
    {
        Url = "http://localhost:8080",
        Database = "test", 
        Distribution = KeyDistributionKind.Uniform,
        Compression = CompressionMode.Identity,
        DocumentSizeBytes = 1024,
        Warmup = TimeSpan.FromMilliseconds(25),
        Duration = TimeSpan.FromMilliseconds(50),
        Step = new StepPlan(2, 2, 1),
        Profile = WorkloadProfile.Mixed
    };
}

