using System.Collections.Generic;
using FluentAssertions;
using RavenBench.Analysis;
using RavenBench.Core.Reporting;
using Xunit;

namespace RavenBench.Tests;

public class KneeFinderTests
{
    [Fact]
    public void Detects_Knee_When_Quality_Degrades()
    {
        // Quality = Throughput / P999
        // C=16: 6763 / 142.8 = 47.4 (best quality)
        // C=64: 8157 / 224.5 = 36.3
        // C=8:  9084 / 1188.9 = 7.6  <- Quality degraded significantly
        var steps = new List<StepResult>
        {
            new() { Concurrency = 16, Throughput = 6763, Raw = new(40, 60, 80, 94.7, 142.8, 142.8), Normalized = new(35, 55, 75, 89.7, 137.8, 137.8) },
            new() { Concurrency = 64, Throughput = 8157, Raw = new(80, 100, 140, 158, 224.5, 224.5), Normalized = new(75, 95, 135, 153, 219.5, 219.5) },
            new() { Concurrency = 8, Throughput = 9084, Raw = new(500, 600, 700, 800, 1188.9, 1188.9), Normalized = new(495, 595, 695, 795, 1183.9, 1183.9) },
        };

        var knee = KneeFinder.FindKnee(steps, maxErr: 0.005)!;
        knee.Concurrency.Should().Be(64); // Quality degrades after C=64
        knee.Reason.Should().Contain("Quality");
    }

    [Fact]
    public void Detects_Knee_When_Quality_Drops()
    {
        // Quality = Throughput / P999
        // C=8:  1000 / 65 = 15.4
        // C=16: 2000 / 125 = 16.0 (improved)
        // C=32: 1800 / 155 = 11.6 (degraded from 16.0)
        var steps = new List<StepResult>
        {
            new() { Concurrency = 8, Throughput = 1000, Raw = new(50, 52, 54, 56, 60, 65), Normalized = new(45, 47, 49, 51, 55, 60) },
            new() { Concurrency = 16, Throughput = 2000, Raw = new(110, 112, 114, 116, 120, 125), Normalized = new(105, 107, 109, 111, 115, 120) },
            new() { Concurrency = 32, Throughput = 1800, Raw = new(140, 142, 144, 146, 150, 155), Normalized = new(135, 137, 139, 141, 145, 150) },
        };

        var knee = KneeFinder.FindKnee(steps, maxErr: 0.005)!;
        knee.Concurrency.Should().Be(16); // Quality peaks at C=16
        knee.Reason.Should().Contain("Quality");
    }

    [Fact]
    public void A_Step_Above_The_Error_Limit_Is_Never_The_Knee()
    {
        StepResult Failing(int concurrency) => new() { Concurrency = concurrency, Throughput = 100, ErrorRate = 0.5, Raw = new(10, 20, 30, 40, 50, 50) };

        KneeFinder.FindKnee(new List<StepResult> { Failing(1), Failing(2) }, maxErr: 0.01).Should().BeNull();
        KneeFinder.FindKnee(new List<StepResult> { Failing(1) }, maxErr: 0.01).Should().BeNull();
    }

    [Fact]
    public void A_Client_Bound_Step_Is_Never_The_Knee_And_The_Verdict_Says_Client_Limited()
    {
        StepResult Step(int concurrency, string? invalid) => new() { Concurrency = concurrency, Throughput = concurrency * 100, InvalidReason = invalid, Raw = new(10, 20, 30, 40, 50, 50) };
        var opts = new RavenBench.Core.RunOptions { Url = "http://localhost:1", Database = "db" };

        var knee = KneeFinder.FindKnee(new List<StepResult> { Step(1, null), Step(2, null), Step(4, "client CPU saturated"), Step(8, null) }, maxErr: 0.01)!;
        knee.Concurrency.Should().Be(2);
        knee.KneeDegraded.Should().BeTrue();
        ResultAnalyzer.BuildVerdict(knee, opts).Should().StartWith("client-limited").And.Contain("client CPU saturated");

        KneeFinder.FindKnee(new List<StepResult> { Step(1, "client CPU saturated"), Step(2, null) }, maxErr: 0.01).Should().BeNull();
    }

    [Fact]
    public void A_Ramp_Ending_On_A_Degraded_Step_Is_Not_A_Clean_Knee()
    {
        StepResult Step(int concurrency, double throughput, double p50, double errors) => new() { Concurrency = concurrency, Throughput = throughput, ErrorRate = errors, Raw = new(p50, p50, p50, p50, p50, p50) };

        KneeFinder.FindKnee(new List<StepResult> { Step(1, 100, 10, 0), Step(2, 200, 10, 0) }, maxErr: 0.01)!.KneeDegraded.Should().BeFalse();

        // The dip at C=2 is deferred because C=4 recovers, and the ramp ends in the danger zone.
        var slow = KneeFinder.FindKnee(new List<StepResult> { Step(1, 100, 10, 0), Step(2, 200, 150, 0), Step(4, 1440, 120, 0) }, maxErr: 0.01)!;
        slow.Concurrency.Should().Be(4);
        slow.KneeDegraded.Should().BeTrue();

        KneeFinder.FindKnee(new List<StepResult> { Step(1, 100, 10, 0), Step(2, 200, 10, 0.001) }, maxErr: 0.01)!.KneeDegraded.Should().BeTrue();
    }
}
