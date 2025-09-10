using FluentAssertions;
using RavenBench.Workload;
using Xunit;

namespace RavenBench.Tests;

public class WorkloadMixWeightsTests
{
    [Fact]
    public void Normalizes_Weights_To_100()
    {
        var mix = WorkloadMix.FromWeights(3, 1, 0);
        (mix.ReadPercent + mix.WritePercent + mix.UpdatePercent).Should().Be(100);
        mix.ReadPercent.Should().BeGreaterThan(mix.WritePercent);
        mix.UpdatePercent.Should().Be(0);
    }

    [Fact]
    public void Accepts_Percents_Directly()
    {
        var mix = WorkloadMix.FromWeights(75, 25, 0);
        (mix.ReadPercent, mix.WritePercent, mix.UpdatePercent).Should().Be((75, 25, 0));
    }
}

