using System;
using RavenBench.Cli;
using RavenBench.Core.Workload;
using Xunit;

namespace RavenBench.Tests.Cli;

public class VectorQuantizationParsingTests
{
    [Theory]
    [InlineData("none", VectorQuantization.None)]
    [InlineData("int8", VectorQuantization.Int8)]
    [InlineData("int4", VectorQuantization.Int4)]
    [InlineData("int3", VectorQuantization.Int3)]
    [InlineData("int2", VectorQuantization.Int2)]
    [InlineData("binary", VectorQuantization.Binary)]
    public void ParsesAllQuantizationTypes(string input, VectorQuantization expected)
    {
        var settings = new ClosedSettings
        {
            Url = "http://localhost:8080",
            Database = "test",
            Profile = "vector-search",
            Dataset = "clinicalwords100d",
            VectorQuantization = input
        };

        var opts = settings.ToRunOptions();

        Assert.Equal(expected, opts.VectorQuantization);
    }

    [Theory]
    [InlineData("INT8")]
    [InlineData("Int4")]
    [InlineData("BINARY")]
    [InlineData(" int3 ")]
    public void ParsesQuantizationCaseInsensitive(string input)
    {
        var settings = new ClosedSettings
        {
            Url = "http://localhost:8080",
            Database = "test",
            Profile = "vector-search",
            Dataset = "clinicalwords100d",
            VectorQuantization = input
        };

        // Should not throw
        var opts = settings.ToRunOptions();
        Assert.NotEqual(VectorQuantization.None, opts.VectorQuantization);
    }

    [Fact]
    public void InvalidQuantizationThrows()
    {
        var settings = new ClosedSettings
        {
            Url = "http://localhost:8080",
            Database = "test",
            Profile = "vector-search",
            Dataset = "clinicalwords100d",
            VectorQuantization = "float16"
        };

        Assert.Throws<ArgumentException>(() => settings.ToRunOptions());
    }
}
