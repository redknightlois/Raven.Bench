using System;
using FluentAssertions;
using RavenBench.Workload;
using Xunit;

namespace RavenBench.Tests;

public class UniformDistributionTests
{
    [Fact]
    public void Produces_Values_In_Range()
    {
        var rng = new Random(123);
        var u = new UniformDistribution();
        for (int i = 0; i < 1000; i++)
        {
            var k = u.NextKey(rng, 100);
            k.Should().BeGreaterOrEqualTo(1);
            k.Should().BeLessOrEqualTo(100);
        }
    }
}

