using System;
using RavenBench.Cli;
using Xunit;

namespace RavenBench.Tests.Cli;

public class RecallKsParsingTests
{
    [Fact]
    public void Recall_Ks_Not_Specified_Returns_Null()
    {
        var opts = CreateOptions(vectorRecallKs: null, vectorTopK: 10);
        Assert.Null(opts.VectorRecallKs);
    }

    [Fact]
    public void Recall_Ks_Empty_Returns_Null()
    {
        var opts = CreateOptions(vectorRecallKs: "", vectorTopK: 10);
        Assert.Null(opts.VectorRecallKs);
    }

    [Fact]
    public void Recall_Ks_Single_Value()
    {
        var opts = CreateOptions(vectorRecallKs: "5", vectorTopK: 10);
        Assert.NotNull(opts.VectorRecallKs);
        Assert.Single(opts.VectorRecallKs);
        Assert.Equal(5, opts.VectorRecallKs[0]);
    }

    [Fact]
    public void Recall_Ks_Multiple_Values_Sorted()
    {
        var opts = CreateOptions(vectorRecallKs: "10,1,5", vectorTopK: 10);
        Assert.NotNull(opts.VectorRecallKs);
        Assert.Equal(3, opts.VectorRecallKs.Length);
        Assert.Equal(1, opts.VectorRecallKs[0]);
        Assert.Equal(5, opts.VectorRecallKs[1]);
        Assert.Equal(10, opts.VectorRecallKs[2]);
    }

    [Fact]
    public void Recall_Ks_Exceeding_TopK_Throws()
    {
        Assert.Throws<ArgumentException>(() =>
            CreateOptions(vectorRecallKs: "1,5,20", vectorTopK: 10));
    }

    [Fact]
    public void Recall_Ks_Zero_Throws()
    {
        Assert.Throws<ArgumentException>(() =>
            CreateOptions(vectorRecallKs: "0,5", vectorTopK: 10));
    }

    [Fact]
    public void Recall_Ks_Negative_Throws()
    {
        Assert.Throws<ArgumentException>(() =>
            CreateOptions(vectorRecallKs: "-1,5", vectorTopK: 10));
    }

    private static Core.RunOptions CreateOptions(string? vectorRecallKs, int vectorTopK)
    {
        var settings = new ClosedSettings
        {
            Url = "http://localhost:8080",
            Database = "test",
            Profile = "vector-search",
            Dataset = "clinicalwords100d",
            VectorTopK = vectorTopK,
            VectorRecallKs = vectorRecallKs
        };
        return settings.ToRunOptions();
    }
}
