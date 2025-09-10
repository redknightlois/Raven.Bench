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
            new() { Concurrency = 8, Throughput = 1000, P95Ms = 5 },
            new() { Concurrency = 16, Throughput = 1900, P95Ms = 6 },
            new() { Concurrency = 32, Throughput = 2000, P95Ms = 10 },
            new() { Concurrency = 64, Throughput = 2050, P95Ms = 25 },
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
            new() { Concurrency = 8, Throughput = 1000, P95Ms = 5 },
            new() { Concurrency = 16, Throughput = 2000, P95Ms = 6 },
            new() { Concurrency = 32, Throughput = 1800, P95Ms = 12 },
        };

        var knee = KneeFinder.FindKnee(steps, dThr: 0.05, dP95: 0.20, maxErr: 0.005)!;
        knee.Concurrency.Should().Be(16);
        knee.Reason.Should().Contain("Δthr");
    }
}

