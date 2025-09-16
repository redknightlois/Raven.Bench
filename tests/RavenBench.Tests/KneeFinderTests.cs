using System.Collections.Generic;
using FluentAssertions;
using RavenBench.Analysis;
using RavenBench.Reporting;
using Xunit;

namespace RavenBench.Tests;

public class KneeFinderTests
{
    [Fact]
    public void Detects_Knee_When_Throughput_Flattens_And_p95_Jumps()
    {
        var steps = new List<StepResult>
        {
            new() { Concurrency = 8, Throughput = 1000, Raw = new(20, 20, 5, 30), Normalized = new(18, 18, 4, 28) },
            new() { Concurrency = 16, Throughput = 1900, Raw = new(80, 80, 6, 90), Normalized = new(75, 75, 5, 85) },
            new() { Concurrency = 32, Throughput = 2000, Raw = new(120, 120, 10, 140), Normalized = new(115, 115, 8, 135) },
            new() { Concurrency = 64, Throughput = 2050, Raw = new(150, 150, 25, 170), Normalized = new(145, 145, 22, 165) },
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
            new() { Concurrency = 8, Throughput = 1000, Raw = new(50, 50, 5, 60), Normalized = new(45, 45, 4, 55) },
            new() { Concurrency = 16, Throughput = 2000, Raw = new(110, 110, 6, 120), Normalized = new(105, 105, 5, 115) },
            new() { Concurrency = 32, Throughput = 1800, Raw = new(140, 140, 12, 150), Normalized = new(135, 135, 10, 145) },
        };

        var knee = KneeFinder.FindKnee(steps, dThr: 0.05, dP95: 0.20, maxErr: 0.005)!;
        knee.Concurrency.Should().Be(16);
        knee.Reason.Should().Contain("Δthr");
    }
}

