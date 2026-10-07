using System;
using System.Collections.Generic;
using FluentAssertions;
using RavenBench.Core;
using RavenBench.Core.Reporting;
using RavenBench.Core.Workload;
using Xunit;

namespace RavenBench.Tests;

[Trait("Category", "Unit")]
public class RunCompatibilityWorkloadTests
{
    private static BenchmarkSummary Summary(RunOptions options) => new()
    {
        Options = options,
        EffectiveHttpVersion = "1.1",
        Steps = new List<StepResult>(),
        Verdict = "Passed",
        ClientCompression = "identity"
    };

    private static RunOptions Base() => new() { Url = "http://localhost:8080", Database = "test" };

    private static readonly Dictionary<string, RunOptions> OneOptionDiffers = new()
    {
        ["document size"] = Base() with { DocumentSizeBytes = 4096 },
        ["attachment operation"] = Base() with { AttachmentOp = AttachmentOperationKind.Get },
        ["bulk batch size"] = Base() with { BulkBatchSize = 7 },
        ["search engine"] = Base() with { SearchEngine = IndexingEngine.Lucene },
        ["preload"] = Base() with { Preload = 500 },
        ["vector top-k"] = Base() with { VectorTopK = 3 },
        ["vector quantization"] = Base() with { VectorQuantization = VectorQuantization.Int8 },
    };

    [Theory]
    [InlineData("document size")]
    [InlineData("attachment operation")]
    [InlineData("bulk batch size")]
    [InlineData("search engine")]
    [InlineData("preload")]
    [InlineData("vector top-k")]
    [InlineData("vector quantization")]
    public void Runs_Differing_Only_In_A_Workload_Option_Are_Not_Comparable_And_The_Message_Names_It(string field)
    {
        var a = Summary(Base());
        var b = Summary(OneOptionDiffers[field]);

        RunCompatibilityChecker.AreComparable(a, b).Should().BeFalse();
        var act = () => RunCompatibilityChecker.EnsureComparable(a, b);
        act.Should().Throw<InvalidOperationException>().WithMessage($"*different {field}:*");
    }

    [Fact]
    public void Runs_With_Equal_Options_Stay_Comparable_When_Only_Transport_Differs()
    {
        var a = Summary(Base());
        var b = Summary(Base() with { Transport = TransportKind.Client });

        RunCompatibilityChecker.AreComparable(a, b).Should().BeTrue();
    }
}
