using System.Collections.Generic;
using FluentAssertions;
using RavenBench.Core.Workload;
using Xunit;

namespace RavenBench.Tests.Workload;

public class SelectSearchTermsTests
{
    [Fact]
    public void Rare_Slice_Is_NonEmpty_When_Least_Frequent_Words_Occur_Once()
    {
        // Diverse corpus: the rarest words each appear exactly once (the real
        // StackOverflow case). Rare terms must still be populated.
        var wordCounts = new Dictionary<string, int>
        {
            ["the"] = 500, ["error"] = 300, ["how"] = 200, ["fix"] = 150, ["null"] = 80,
            ["segfault"] = 1, ["mutex"] = 1, ["heisenbug"] = 1, ["bitmask"] = 1, ["coroutine"] = 1,
        };

        var (rare, common) = StackOverflowWorkloadHelper.SelectSearchTerms(wordCounts);

        rare.Should().NotBeEmpty();
        common.Should().NotBeEmpty();
    }
}
