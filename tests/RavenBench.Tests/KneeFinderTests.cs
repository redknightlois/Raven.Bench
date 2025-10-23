using System.Collections.Generic;
using FluentAssertions;
using RavenBench.Analysis;
using RavenBench.Core.Reporting;
using Xunit;

namespace RavenBench.Tests;

public class KneeFinderTests
{
    [Fact]
    public void Detects_Knee_When_Throughput_Flattens_And_p95_Jumps()
    {
        var steps = new List<StepResult>
        {
            new() { Concurrency = 8, Throughput = 1000, Raw = new(20, 22, 24, 26, 30, 35), Normalized = new(18, 20, 22, 24, 28, 33) },
            new() { Concurrency = 16, Throughput = 1900, Raw = new(80, 82, 84, 86, 90, 95), Normalized = new(75, 77, 79, 81, 85, 90) },
            new() { Concurrency = 32, Throughput = 2000, Raw = new(120, 122, 124, 126, 140, 145), Normalized = new(115, 117, 119, 121, 135, 140) },
            new() { Concurrency = 64, Throughput = 2050, Raw = new(150, 152, 154, 156, 170, 175), Normalized = new(145, 147, 149, 151, 165, 170) },
        };

        var knee = KneeFinder.FindKnee(steps, dThr: 0.05, dP95: 0.20, maxErr: 0.005)!;
        knee.Concurrency.Should().Be(32);
        knee.Reason.Should().Contain("Δthr");
    }

    [Fact]
    public void Detects_Knee_When_Throughput_Drops_And_p95_Rises()
    {
        var steps = new List<StepResult>
        {
            new() { Concurrency = 8, Throughput = 1000, Raw = new(50, 52, 54, 56, 60, 65), Normalized = new(45, 47, 49, 51, 55, 60) },
            new() { Concurrency = 16, Throughput = 2000, Raw = new(110, 112, 114, 116, 120, 125), Normalized = new(105, 107, 109, 111, 115, 120) },
            new() { Concurrency = 32, Throughput = 1800, Raw = new(140, 142, 144, 146, 150, 155), Normalized = new(135, 137, 139, 141, 145, 150) },
        };

        var knee = KneeFinder.FindKnee(steps, dThr: 0.05, dP95: 0.20, maxErr: 0.005)!;
        knee.Concurrency.Should().Be(16);
        knee.Reason.Should().Contain("Δthr");
    }
}

