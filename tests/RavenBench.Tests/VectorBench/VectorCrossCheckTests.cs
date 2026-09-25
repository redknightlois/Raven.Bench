using System.Collections.Generic;
using FluentAssertions;
using RavenBench.Core.Reporting;
using Xunit;

namespace RavenBench.Tests.VectorBench;

public class VectorCrossCheckTests
{
    private const string HnswDefault = "CREATE INDEX bench_v_idx ON public.bench USING hnsw (v vector_cosine_ops)";

    [Fact]
    public void PublishedHnswWithoutOptions_IsTheDefaultBuild() =>
        VectorBuildState.DefaultBuild("hnsw", new Dictionary<string, int>(), HnswDefault).Should().Be(VectorBuildState.AtDefaultBuild);

    [Fact]
    public void PublishedIvfflat_IsNotTheDefaultBuild()
    {
        var state = VectorBuildState.DefaultBuild("ivfflat", new Dictionary<string, int> { ["lists"] = 1000 }, HnswDefault);
        state.Should().StartWith("not the pgvector default build").And.Contain("lists=1000").And.Contain("USING hnsw");
    }

    [Fact]
    public void PublishedHnswWithOptions_IsNotTheDefaultBuild() =>
        VectorBuildState.DefaultBuild("hnsw", new Dictionary<string, int> { ["m"] = 16 }, HnswDefault).Should().StartWith("not ");

    [Fact]
    public void DefaultBuildWithOptions_IsNotTheDefaultBuild() =>
        VectorBuildState.DefaultBuild("hnsw", new Dictionary<string, int>(), HnswDefault + " WITH (m='32')").Should().StartWith("not ");
}
