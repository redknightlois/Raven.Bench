using System;
using System.Collections.Generic;
using FluentAssertions;
using RavenBench;
using RavenBench.Core;
using RavenBench.Core.Workload;
using Xunit;

namespace RavenBench.Tests.Workload;

public class QuestionsByTitlePrefixWorkloadTests
{
    [Fact]
    public void Throws_On_Empty_Title_Prefixes()
    {
        var emptyMetadata = new StackOverflowWorkloadMetadata
        {
            QuestionIds = new[] { 1, 2, 3 },
            UserIds = new[] { 1, 2 },
            QuestionCount = 3,
            UserCount = 2,
            TitlePrefixes = Array.Empty<string>(),
            ComputedAt = DateTime.UtcNow
        };

        Action act = () => new QuestionsByTitlePrefixWorkload(emptyMetadata);

        act.Should().Throw<ArgumentException>()
            .WithMessage("Metadata must contain sampled title prefixes");
    }

    [Fact]
    public void Generates_Prefix_Query_Operations_With_Parameters()
    {
        var metadata = new StackOverflowWorkloadMetadata
        {
            QuestionIds = new[] { 1, 2, 3 },
            UserIds = new[] { 1, 2 },
            QuestionCount = 3,
            UserCount = 2,
            TitlePrefixes = new[] { "How", "What", "Why" },
            ComputedAt = DateTime.UtcNow,
            TitleIndexName = "Questions/ByTitle-corax"
        };

        var workload = new QuestionsByTitlePrefixWorkload(metadata);
        var rng = new Random(42);
        var op = workload.NextOperation(rng);

        op.Should().BeOfType<QueryOperation>();
        var queryOp = (QueryOperation)op;

        queryOp.QueryText.Should().Be("from questions where startsWith(Title, $prefix)");
        queryOp.Parameters.Should().ContainKey("prefix");
        queryOp.Parameters["prefix"].Should().BeOneOf("How", "What", "Why");
        queryOp.ExpectedIndex.Should().Be("Questions/ByTitle-corax");
    }

    [Fact]
    public void Samples_All_Prefixes_Uniformly()
    {
        var prefixes = new[] { "How", "What", "Why", "Can", "Is" };
        var metadata = new StackOverflowWorkloadMetadata
        {
            QuestionIds = new[] { 1, 2, 3 },
            UserIds = new[] { 1, 2 },
            QuestionCount = 3,
            UserCount = 2,
            TitlePrefixes = prefixes,
            ComputedAt = DateTime.UtcNow,
            TitleIndexName = "Questions/ByTitle-corax"
        };

        var workload = new QuestionsByTitlePrefixWorkload(metadata);
        var rng = new Random(42);
        var prefixesSeen = new HashSet<string>();

        // Generate enough operations to see all prefixes
        for (int i = 0; i < 100; i++)
        {
            var op = (QueryOperation)workload.NextOperation(rng);
            prefixesSeen.Add(op.Parameters["prefix"]!.ToString()!);
        }

        // With 100 operations and 5 prefixes, we should see all of them
        prefixesSeen.Should().BeEquivalentTo(prefixes);
    }
}

public class QuestionsByTitleSearchWorkloadTests
{
    [Fact]
    public void Throws_On_Empty_Search_Terms()
    {
        var emptyMetadata = new StackOverflowWorkloadMetadata
        {
            QuestionIds = new[] { 1, 2, 3 },
            UserIds = new[] { 1, 2 },
            QuestionCount = 3,
            UserCount = 2,
            SearchTermsRare = Array.Empty<string>(),
            SearchTermsCommon = Array.Empty<string>(),
            ComputedAt = DateTime.UtcNow
        };

        Action act = () => new QuestionsByTitleSearchWorkload(emptyMetadata);

        act.Should().Throw<ArgumentException>()
            .WithMessage("Metadata must contain both rare and common search terms");
    }

    [Fact]
    public void Generates_Search_Query_Operations_With_Parameters()
    {
        var metadata = new StackOverflowWorkloadMetadata
        {
            QuestionIds = new[] { 1, 2, 3 },
            UserIds = new[] { 1, 2 },
            QuestionCount = 3,
            UserCount = 2,
            SearchTermsRare = new[] { "algorithm", "optimization" },
            SearchTermsCommon = new[] { "error", "problem", "help" },
            ComputedAt = DateTime.UtcNow,
            TitleSearchIndexName = "Questions/ByTitleSearch-corax"
        };

        var workload = new QuestionsByTitleSearchWorkload(metadata);
        var rng = new Random(42);
        var op = workload.NextOperation(rng);

        op.Should().BeOfType<QueryOperation>();
        var queryOp = (QueryOperation)op;

        queryOp.QueryText.Should().Be("from questions where search(Title, $term)");
        queryOp.Parameters.Should().ContainKey("term");
        queryOp.Parameters["term"].Should().BeOneOf("algorithm", "optimization", "error", "problem", "help");
        queryOp.ExpectedIndex.Should().Be("Questions/ByTitleSearch-corax");
    }

    [Fact]
    public void Prefers_Rare_Terms_At_Specified_Probability()
    {
        var metadata = new StackOverflowWorkloadMetadata
        {
            QuestionIds = new[] { 1, 2, 3 },
            UserIds = new[] { 1, 2 },
            QuestionCount = 3,
            UserCount = 2,
            SearchTermsRare = new[] { "rare1", "rare2" },
            SearchTermsCommon = new[] { "common1", "common2", "common3" },
            ComputedAt = DateTime.UtcNow,
            TitleSearchIndexName = "Questions/ByTitleSearch-corax"
        };

        var workload = new QuestionsByTitleSearchWorkload(metadata);
        var rng = new Random(42);
        var rareCount = 0;
        var commonCount = 0;
        const int totalOps = 1000;

        for (int i = 0; i < totalOps; i++)
        {
            var op = (QueryOperation)workload.NextOperation(rng);
            var term = op.Parameters["term"]!.ToString()!;

            if (term.StartsWith("rare"))
                rareCount++;
            else if (term.StartsWith("common"))
                commonCount++;
        }

        // Should have both rare and common terms
        rareCount.Should().BeGreaterThan(0);
        commonCount.Should().BeGreaterThan(0);

        // Rare terms should be around 30% (RareTermProbability = 0.3)
        var rareRatio = (double)rareCount / totalOps;
        rareRatio.Should().BeInRange(0.25, 0.35); // Allow some variance
    }

    [Fact]
    public void Throws_On_Missing_Rare_Terms()
    {
        var metadata = new StackOverflowWorkloadMetadata
        {
            QuestionIds = new[] { 1, 2, 3 },
            UserIds = new[] { 1, 2 },
            QuestionCount = 3,
            UserCount = 2,
            SearchTermsRare = Array.Empty<string>(),
            SearchTermsCommon = new[] { "common1", "common2" },
            ComputedAt = DateTime.UtcNow
        };

        Action act = () => new QuestionsByTitleSearchWorkload(metadata);

        act.Should().Throw<ArgumentException>()
            .WithMessage("Metadata must contain both rare and common search terms");
    }

    [Fact]
    public void Throws_On_Missing_Common_Terms()
    {
        var metadata = new StackOverflowWorkloadMetadata
        {
            QuestionIds = new[] { 1, 2, 3 },
            UserIds = new[] { 1, 2 },
            QuestionCount = 3,
            UserCount = 2,
            SearchTermsRare = new[] { "rare1", "rare2" },
            SearchTermsCommon = Array.Empty<string>(),
            ComputedAt = DateTime.UtcNow
        };

        Action act = () => new QuestionsByTitleSearchWorkload(metadata);

        act.Should().Throw<ArgumentException>()
            .WithMessage("Metadata must contain both rare and common search terms");
    }

    [Fact]
    public void StackOverflowQueries_Fail_On_Incomplete_Metadata()
    {
        // Metadata must be complete; no fallbacks for missing fields
        var incompleteMetadata = new StackOverflowWorkloadMetadata
        {
            QuestionIds = new[] { 1, 2, 3 },
            UserIds = new[] { 5, 6 },
            QuestionCount = 3,
            UserCount = 2,
            TitlePrefixes = Array.Empty<string>(),
            SearchTermsRare = Array.Empty<string>(),
            SearchTermsCommon = Array.Empty<string>(),
            ComputedAt = DateTime.UtcNow
        };

        var opts = new RunOptions
        {
            Url = "http://localhost:8080",
            Database = "test",
            Profile = WorkloadProfile.StackOverflowTextSearch,
            QueryProfile = QueryProfile.TextPrefix
        };

        Action act = () => BenchmarkRunner.BuildWorkload(opts, incompleteMetadata, usersMetadata: null, vectorMetadata: null);

        act.Should().Throw<ArgumentException>("workloads require complete metadata without fallbacks");
    }

    [Fact]
    public void Throws_On_Empty_Rare_Or_Common_Search_Terms()
    {
        var emptyRareMetadata = new StackOverflowWorkloadMetadata
        {
            QuestionIds = new[] { 1, 2, 3 },
            UserIds = new[] { 1, 2 },
            QuestionCount = 3,
            UserCount = 2,
            SearchTermsRare = Array.Empty<string>(),
            SearchTermsCommon = new[] { "error", "problem" },
            ComputedAt = DateTime.UtcNow
        };

        Action act = () => new QuestionsByTitleSearchWorkload(emptyRareMetadata);

        act.Should().Throw<ArgumentException>()
            .WithMessage("Metadata must contain both rare and common search terms");
    }

    [Fact]
    public void Generates_TextSearch_Operations_With_Rare_Only()
    {
        var metadata = new StackOverflowWorkloadMetadata
        {
            QuestionIds = new[] { 1, 2, 3 },
            UserIds = new[] { 1, 2 },
            QuestionCount = 3,
            UserCount = 2,
            SearchTermsRare = new[] { "rare1", "rare2" },
            SearchTermsCommon = new[] { "common1", "common2" },
            ComputedAt = DateTime.UtcNow,
            TitleSearchIndexName = "Questions/ByTitleSearch-corax"
        };

        var workload = new QuestionsByTitleSearchWorkload(metadata, 1.0); // 100% rare
        var rng = new Random(42);
        var op = workload.NextOperation(rng);

        op.Should().BeOfType<QueryOperation>();
        var queryOp = (QueryOperation)op;

        queryOp.QueryText.Should().Be("from questions where search(Title, $term)");
        queryOp.Parameters.Should().ContainKey("term");
        queryOp.Parameters["term"].Should().BeOneOf("rare1", "rare2");
        queryOp.ExpectedIndex.Should().Be("Questions/ByTitleSearch-corax");
    }

    [Fact]
    public void Generates_TextSearch_Operations_With_Common_Only()
    {
        var metadata = new StackOverflowWorkloadMetadata
        {
            QuestionIds = new[] { 1, 2, 3 },
            UserIds = new[] { 1, 2 },
            QuestionCount = 3,
            UserCount = 2,
            SearchTermsRare = new[] { "rare1", "rare2" },
            SearchTermsCommon = new[] { "common1", "common2" },
            ComputedAt = DateTime.UtcNow,
            TitleSearchIndexName = "Questions/ByTitleSearch-corax"
        };

        var workload = new QuestionsByTitleSearchWorkload(metadata, 0.0); // 100% common
        var rng = new Random(42);
        var op = workload.NextOperation(rng);

        op.Should().BeOfType<QueryOperation>();
        var queryOp = (QueryOperation)op;

        queryOp.QueryText.Should().Be("from questions where search(Title, $term)");
        queryOp.Parameters.Should().ContainKey("term");
        queryOp.Parameters["term"].Should().BeOneOf("common1", "common2");
        queryOp.ExpectedIndex.Should().Be("Questions/ByTitleSearch-corax");
    }

    [Fact]
    public void Generates_TextSearch_Operations_With_Mixed_50_50()
    {
        var metadata = new StackOverflowWorkloadMetadata
        {
            QuestionIds = new[] { 1, 2, 3 },
            UserIds = new[] { 1, 2 },
            QuestionCount = 3,
            UserCount = 2,
            SearchTermsRare = new[] { "rare1" },
            SearchTermsCommon = new[] { "common1" },
            ComputedAt = DateTime.UtcNow,
            TitleSearchIndexName = "Questions/ByTitleSearch-corax"
        };

        var workload = new QuestionsByTitleSearchWorkload(metadata, 0.5); // 50% rare, 50% common
        var rng = new Random(42);
        var rareCount = 0;
        var commonCount = 0;
        const int totalOps = 1000;

        for (int i = 0; i < totalOps; i++)
        {
            var op = (QueryOperation)workload.NextOperation(rng);
            var term = op.Parameters["term"]!.ToString()!;

            if (term.StartsWith("rare"))
                rareCount++;
            else if (term.StartsWith("common"))
                commonCount++;
        }

        // Should have both rare and common terms
        rareCount.Should().BeGreaterThan(0);
        commonCount.Should().BeGreaterThan(0);

        // Should be around 50% each (allow some variance)
        var rareRatio = (double)rareCount / totalOps;
        rareRatio.Should().BeInRange(0.4, 0.6);
    }

    [Fact]
    public void Throws_On_Negative_RareTermProbability()
    {
        var metadata = new StackOverflowWorkloadMetadata
        {
            QuestionIds = new[] { 1, 2, 3 },
            UserIds = new[] { 1, 2 },
            QuestionCount = 3,
            UserCount = 2,
            SearchTermsRare = new[] { "rare1" },
            SearchTermsCommon = new[] { "common1" },
            ComputedAt = DateTime.UtcNow,
            TitleSearchIndexName = "Questions/ByTitleSearch-corax"
        };

        Action act = () => new QuestionsByTitleSearchWorkload(metadata, -0.1);

        act.Should().Throw<ArgumentOutOfRangeException>()
            .WithMessage("*rareTermProbability*");
    }

    [Fact]
    public void Throws_On_RareTermProbability_Greater_Than_One()
    {
        var metadata = new StackOverflowWorkloadMetadata
        {
            QuestionIds = new[] { 1, 2, 3 },
            UserIds = new[] { 1, 2 },
            QuestionCount = 3,
            UserCount = 2,
            SearchTermsRare = new[] { "rare1" },
            SearchTermsCommon = new[] { "common1" },
            ComputedAt = DateTime.UtcNow,
            TitleSearchIndexName = "Questions/ByTitleSearch-corax"
        };

        Action act = () => new QuestionsByTitleSearchWorkload(metadata, 1.1);

        act.Should().Throw<ArgumentOutOfRangeException>()
            .WithMessage("*rareTermProbability*");
    }
}
