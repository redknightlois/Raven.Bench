using System;
using System.Linq;
using FluentAssertions;
using RavenBench.Core;
using RavenBench.Core.Workload;
using RavenBench.Core.Ycsb;
using Xunit;

namespace RavenBench.Tests.Ycsb;

/// <summary>
/// Pins C, A, B and insert-stream as fixed points of one weighted blend: each preset resolves to
/// the mix the run sequence holds for it. The mix values are not restated here.
/// </summary>
public class YcsbBlendFixedPointsTests
{
    private const int DocumentSize = 1024;
    private const int Seed = 42;
    private const int Sample = 4000;
    private const int Keyspace = 1000;

    public static TheoryData<YcsbRunKind> BlendedRuns => new()
    {
        YcsbRunKind.WorkloadC, YcsbRunKind.WorkloadA, YcsbRunKind.WorkloadB, YcsbRunKind.InsertStream
    };

    [Theory]
    [MemberData(nameof(BlendedRuns))]
    public void Each_Preset_Is_The_One_Blend_At_The_Mix_Its_Run_Carries(YcsbRunKind kind)
    {
        var mix = YcsbRunKinds.MixFor(kind);
        var workload = new MixedProfileWorkload(mix, new UniformDistribution(), DocumentSize, Seed, initialKeyspace: Keyspace);
        var rng = new Random(1);

        var ops = Enumerable.Range(0, Sample).Select(_ => workload.NextOperation(rng)).ToList();

        var tolerance = Sample / 10;
        ops.Count(op => op is ReadOperation).Should().BeCloseTo(mix.ReadPercent * Sample / 100, (uint)tolerance);
        ops.Count(op => op is UpdateFieldOperation).Should().BeCloseTo(mix.UpdatePercent * Sample / 100, (uint)tolerance);
        ops.Count(op => op is InsertOperation<string>).Should().BeCloseTo(mix.WritePercent * Sample / 100, (uint)tolerance);
    }

    [Fact]
    public void The_Insert_Stream_Preset_Is_The_Blend_With_Every_Weight_On_Insert()
    {
        var mix = YcsbRunKinds.MixFor(YcsbRunKind.InsertStream);

        mix.WritePercent.Should().Be(100);
        mix.ReadPercent.Should().Be(0);
        mix.UpdatePercent.Should().Be(0);
    }

    [Fact]
    public void The_Load_Run_Carries_No_Weighted_Mix()
    {
        // Load fills the keyspace through the bulk path; it is not a point of the blend.
        var act = () => YcsbRunKinds.MixFor(YcsbRunKind.Load);

        act.Should().Throw<ArgumentOutOfRangeException>();
    }
}
