using System;
using System.Linq;
using FluentAssertions;
using RavenBench.Cli;
using RavenBench.Core;
using RavenBench.Core.Reporting;
using RavenBench.Core.Transport;
using RavenBench.Core.Ycsb;
using RavenBench.Ycsb;
using Xunit;

namespace RavenBench.Tests.Ycsb;

/// <summary>The cross-check verdicts and the rows it compares.</summary>
public class YcsbCrossCheckTests
{
    [Fact]
    public void A_Difference_Inside_The_Repetition_Spread_Is_Within_Noise()
    {
        var comparison = YcsbCrossCheck.Compare("C|closed|uniform|-", Side("raw", 100, 110, 104), Side("client", 106, 98, 101));

        comparison.WithinNoise.Should().BeTrue();
        comparison.Difference.Should().Be(101 - 104);
        comparison.Noise.Should().Be(10);
        comparison.NoiseRule.Should().Be(YcsbCrossCheck.NoiseRule);
        comparison.Reference.Results.Should().HaveCount(3);
    }

    [Fact]
    public void A_Difference_Beyond_Either_Spread_Is_Outside_Noise()
    {
        var comparison = YcsbCrossCheck.Compare("C|closed|uniform|-", Side("raw", 100, 102, 101), Side("client", 80, 81));

        comparison.WithinNoise.Should().BeFalse();
        comparison.Difference.Should().Be(80.5 - 101);
    }

    [Fact]
    public void One_Repetition_Measures_No_Noise_And_Gives_No_Verdict()
    {
        var compare = () => YcsbCrossCheck.Compare("C|closed|uniform|-", Side("raw", 100), Side("client", 100, 101));
        compare.Should().Throw<YcsbCrossCheckException>().WithMessage("*raw*");
    }

    [Fact]
    public void The_Cross_Check_Compares_Only_Closed_Loop_Workload_C_Rows()
    {
        YcsbCrossCheckCommand.IsComparedRow(Identity(YcsbRunKind.WorkloadC, LoadShape.Closed)).Should().BeTrue();
        YcsbCrossCheckCommand.IsComparedRow(Identity(YcsbRunKind.WorkloadC, LoadShape.Rate)).Should().BeFalse();
        YcsbCrossCheckCommand.IsComparedRow(Identity(YcsbRunKind.Load, LoadShape.Closed)).Should().BeFalse();
        YcsbCrossCheckCommand.IsComparedRow(Identity(YcsbRunKind.WorkloadA, LoadShape.Closed)).Should().BeFalse();
    }

    [Fact]
    public void A_Side_With_No_Valid_Repetition_Is_Never_Within_Noise()
    {
        YcsbRunResult Repetition(int repetition, params StepResult[] steps) => new(
            Identity(YcsbRunKind.WorkloadC, LoadShape.Closed) with { Repetition = repetition },
            new BenchmarkSummary { Options = new RunOptions { Url = "u", Database = "d" }, Steps = steps.ToList(), Verdict = "v", ClientCompression = "identity", EffectiveHttpVersion = "1.1" });
        var results = new[] { Repetition(1), Repetition(2, new StepResult { Throughput = 0, InvalidReason = "client-bound" }) };

        var reference = YcsbCrossCheckCommand.Rows(TransportKind.Raw, results).Single();
        var candidate = YcsbCrossCheckCommand.Rows(TransportKind.Client, results).Single();

        reference.Value.Values.Should().BeEmpty();
        var compare = () => YcsbCrossCheck.Compare(reference.Key, reference.Value, candidate.Value);
        compare.Should().Throw<YcsbCrossCheckException>().WithMessage("*0 valid repetition(s)*");
    }

    private static YcsbRunIdentity Identity(YcsbRunKind kind, LoadShape shape) => new()
    {
        Kind = kind, Shape = shape, Distribution = "uniform", Repetition = 1, Rate = shape == LoadShape.Rate ? 100 : null
    };

    private static YcsbCrossCheckSide Side(string mode, params double[] values) =>
        new(mode, Array.ConvertAll(values, v => $"{mode}-{v}"), values);
}
