using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FluentAssertions;
using RavenBench.Core.Reporting;
using RavenBench.Core.Workload;
using RavenBench.Dataset.Vectors;
using RavenBench.VectorBench;
using Xunit;

namespace RavenBench.Tests.VectorBench;

public class VectorRunMathTests
{
    private static VectorEffortPoint Point(string label, int value, double recall) => new(label, "hnsw.ef_search", value, recall, 0, 0, 0, 0);

    [Fact]
    public void A_Curve_That_Reaches_The_Threshold_Only_At_High_Names_High()
    {
        var curve = new[] { Point("low", 10, 0.80), Point("default", 40, 0.93), Point("high", 400, 0.99) };
        VectorRunMath.SelectLowest(curve, 0.95)!.Label.Should().Be("high");
    }

    [Fact]
    public void A_Curve_That_Never_Reaches_The_Threshold_Names_None()
    {
        var curve = new[] { Point("low", 10, 0.80), Point("default", 40, 0.93), Point("high", 400, 0.949) };
        VectorRunMath.SelectLowest(curve, 0.95).Should().BeNull();
    }

    [Fact]
    public void The_Selection_Follows_Effort_Order_Not_Scenario_Order()
    {
        var curve = new[] { Point("high", 400, 0.99), Point("low", 10, 0.80), Point("default", 40, 0.96) };
        VectorRunMath.SelectLowest(curve, 0.95)!.Value.Should().Be(40);
    }

    [Fact]
    public void The_Split_Is_Seeded_Disjoint_And_Labels_The_Selectivity()
    {
        const long baseCount = 10_000;
        var a = new VectorSplit(baseCount, seed: 7, insertCount: 500, selectivity: 0.05);
        var b = new VectorSplit(baseCount, seed: 7, insertCount: 500, selectivity: 0.05);
        var c = new VectorSplit(baseCount, seed: 8, insertCount: 500, selectivity: 0.05);

        var sliceA = Enumerable.Range(0, (int)baseCount).Where(p => a.IsInsertSlice(p)).ToList();
        sliceA.Should().HaveCount(500);
        sliceA.Should().Equal(Enumerable.Range(0, (int)baseCount).Where(p => b.IsInsertSlice(p)));
        sliceA.Should().NotEqual(Enumerable.Range(0, (int)baseCount).Where(p => c.IsInsertSlice(p)));

        a.LoadedCount.Should().Be(baseCount - 500);
        a.LabelledCount.Should().Be((long)Math.Round((baseCount - 500) * 0.05));
        Enumerable.Range(0, (int)a.LoadedCount).Count(o => a.LabelOf(o) == VectorSplit.LabelIn).Should().Be((int)a.LabelledCount);
        Enumerable.Range(0, (int)a.LoadedCount).Select(o => a.LabelOf(o)).Should().Equal(Enumerable.Range(0, (int)b.LoadedCount).Select(o => b.LabelOf(o)));
    }

    [Fact]
    public async Task The_Filtered_Truth_Holds_Only_Labelled_Ids_In_Exact_Order()
    {
        var random = new Random(3);
        var vectors = Enumerable.Range(0, 400).Select(i => new BaseVector(i.ToString(), Enumerable.Range(0, 8).Select(_ => (float)random.NextDouble() - 0.5f).ToArray())).ToList();
        var split = new VectorSplit(vectors.Count, seed: 1, insertCount: 20, selectivity: 0.25);
        var labelled = new List<BaseVector>();
        long position = 0, loaded = 0;
        foreach (var v in vectors)
        {
            if (split.IsInsertSlice(position++))
                continue;
            if (split.LabelOf(loaded++) == VectorSplit.LabelIn)
                labelled.Add(v);
        }
        var query = new[] { vectors[5].Vector.Select(x => -x).ToArray() };

        var truth = await BruteForceTruth.ComputeAsync(query, labelled.ToAsyncEnumerable(), VectorMetric.L2, 10);

        var labelledIds = labelled.Select(v => v.Id).ToHashSet();
        truth[0].Should().OnlyContain(id => labelledIds.Contains(id));
        var expected = labelled.OrderBy(v => v.Vector.Zip(query[0], (x, y) => (x - y) * (x - y)).Sum()).Take(10).Select(v => v.Id);
        truth[0].Should().Equal(expected);
    }

    [Fact]
    public void An_Insert_Before_A_Query_Can_Be_In_Its_Truth_And_One_After_Cannot()
    {
        var query = new[] { 1f, 0f };
        var quiet = new List<BaseVector> { new("a", [0.5f, 0.5f]), new("b", [0f, 1f]) };
        var slice = new List<BaseVector> { new("s0", [1f, 0.01f]), new("s1", [1f, 0.001f]) };

        var truth = VectorRunMath.TruthWithInserts(query, quiet, slice, [0, 1, 2], VectorMetric.Cosine, 2);

        truth[0].Should().Equal("a", "b");
        truth[1].Should().Equal("s0", "a");
        truth[2].Should().Equal("s1", "s0");
    }

    [Fact]
    public void The_Quiet_Truth_Drops_The_Slice_And_Fails_By_Query_When_Too_Shallow()
    {
        var truth = new[] { new[] { "1", "2", "3", "4" } };
        VectorRunMath.WithoutSlice(truth, new HashSet<string> { "2" }, 3)[0].Should().Equal("1", "3", "4");

        var act = () => VectorRunMath.WithoutSlice(truth, new HashSet<string> { "2", "3" }, 3);
        act.Should().Throw<InsufficientTruthDepthException>().WithMessage("*query 0*TruthDepth*");
    }

    [Fact]
    public async Task A_Capped_Set_Loads_Its_First_Vectors_And_Scores_Against_Them_Alone()
    {
        var data = Directory.CreateTempSubdirectory("vector-cap-");
        try
        {
            var inner = new TinyHeldOutSet(data.FullName);
            var capped = new CappedVectorDataset(inner, 500);
            var files = await PinnedFiles.EnsureAsync(capped, data.FullName);
            var selection = new QuerySelection(5, 10);

            var loaded = await capped.ReadBaseAsync(files, selection).ToListAsync();
            loaded.Select(v => v.Id).Should().Equal(await inner.ReadBaseAsync(files, selection).Take(500).Select(v => v.Id).ToListAsync());
            (await capped.BaseCountAsync(files, selection)).Should().Be(500);

            var queries = await capped.GetQueriesAsync(files, selection, 10);
            var expected = await BruteForceTruth.ComputeAsync(queries.Queries, loaded.ToAsyncEnumerable(), capped.Metric, 10);
            queries.Neighbors.Should().BeEquivalentTo(expected, o => o.WithStrictOrdering());
            queries.Neighbors.Should().NotBeEquivalentTo((await inner.GetQueriesAsync(files, selection, 10)).Neighbors, "the whole set holds nearer vectors past the cap");
        }
        finally
        {
            data.Delete(recursive: true);
        }
    }

    [Fact]
    public void A_Short_Result_Counts_Its_Missing_Rows_As_Misses()
    {
        VectorRunMath.Recall(["1", "2"], ["1", "2", "3", "4"], 4).Should().Be(0.5);
        VectorRunMath.Recall(["4", "3", "2", "1"], ["1", "2", "3", "4"], 4).Should().Be(1.0);
    }

    [Fact]
    public void ResourceCheck_RefusesByName_WhenMemoryOrDiskIsShort()
    {
        const long needed = 1000L * 100 * sizeof(float) * VectorResourceCheck.FootprintFactor;
        VectorResourceCheck.Require(1000, 100, needed, needed);
        FluentActions.Invoking(() => VectorResourceCheck.Require(1000, 100, needed - 1, needed)).Should().Throw<VectorResourceException>().WithMessage("Available memory*");
        FluentActions.Invoking(() => VectorResourceCheck.Require(1000, 100, needed, needed - 1)).Should().Throw<VectorResourceException>().WithMessage("Free disk*");
    }
}
