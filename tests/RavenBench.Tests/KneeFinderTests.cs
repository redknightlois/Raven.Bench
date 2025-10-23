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

        var knee = KneeFinder.FindKnee(steps, dThr: 0.05, dP95: 0.20, maxErr: 0.005)!;
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

        var knee = KneeFinder.FindKnee(steps, dThr: 0.05, dP95: 0.20, maxErr: 0.005)!;
        knee.Concurrency.Should().Be(16); // Quality peaks at C=16
        knee.Reason.Should().Contain("Quality");
    }
}

