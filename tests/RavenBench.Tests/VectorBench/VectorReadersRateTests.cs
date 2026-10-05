using System.Text.Json;
using FluentAssertions;
using RavenBench.VectorBench;
using Xunit;

namespace RavenBench.Tests.VectorBench;

[Trait("Category", "Unit")]
public class VectorReadersRateTests
{
    [Theory]
    [InlineData(2.0)]
    [InlineData(10.0)]
    [InlineData(1234.56)]
    public void The_Readers_Fixed_Rate_Is_The_Shared_Fraction_Below_The_Closed_Loop_And_Is_Recorded(double closed)
    {
        var info = VectorRunner.ReadersInfo(readers: 8, closed);

        info.FixedRate.Should().BeGreaterThan(0).And.BeLessThan(closed);
        info.FixedRate.Should().Be(FixedRate.For(closed), "the vector and aggregate runners share one fraction");
        info.FixedRateFraction.Should().Be(FixedRate.Fraction);
        JsonSerializer.Serialize(info).Should().Contain("\"FixedRateFraction\"");
    }
}
