using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using FluentAssertions;
using RavenBench.Aggregate;
using RavenBench.Core.Aggregate;
using RavenBench.Core.Transport;
using RavenBench.Core.Workload;
using Xunit;

namespace RavenBench.Tests.Aggregate;

public class AggregateUnitTests
{
    private static AggregateDataSpec Spec(int seed = 7, long count = 2_000, int categories = 100, int regions = 1_000, GroupDistribution? distribution = null) =>
        new(seed, count, 256, categories, regions, distribution ?? GroupDistribution.Parse(GroupDistribution.Uniform, null));

    private static GroupDistribution Zipf(double exponent = 0.99) => GroupDistribution.Parse(GroupDistribution.Zipfian, exponent);

    [Fact]
    public void Generator_Is_Deterministic_And_Values_Stay_Inside_Their_Cardinality()
    {
        var spec = Spec(distribution: Zipf());
        var first = new AggregateDataSet(spec).Generate().ToList();
        var second = new AggregateDataSet(spec).Generate().ToList();

        second.Should().Equal(first);
        first.Should().HaveCount(2_000);
        var categories = Enumerable.Range(1, 100).Select(r => AggregateDataSet.CategoryKey(r, 100)).ToHashSet();
        var regions = Enumerable.Range(1, 1_000).Select(r => AggregateDataSet.RegionKey(r, 1_000)).ToHashSet();
        first.Should().OnlyContain(d => categories.Contains(d.Category) && regions.Contains(d.Region) && d.Amount >= 1 && d.Amount <= AggregateDataSet.MaxAmount);
        first.Select(d => d.Id).Should().OnlyHaveUniqueItems();
    }

    [Fact]
    public void Skewed_Distribution_Concentrates_Documents_On_Fewer_Groups_Than_Uniform()
    {
        var uniform = new AggregateDataSet(Spec()).Generate().ToList();
        var skewed = new AggregateDataSet(Spec(distribution: Zipf())).Generate().ToList();

        static int Hottest(IEnumerable<string> keys) => keys.GroupBy(k => k).Max(g => g.Count());
        Hottest(skewed.Select(d => d.Category)).Should().BeGreaterThan(Hottest(uniform.Select(d => d.Category)));
        skewed.Select(d => d.Region).Distinct().Count().Should().BeLessThan(uniform.Select(d => d.Region).Distinct().Count());
    }

    [Fact]
    public void Checksum_Follows_Every_Input_Of_The_Emitted_Set()
    {
        var baseline = new AggregateDataSet(Spec()).Summarize();

        new AggregateDataSet(Spec()).Summarize().Should().Be(baseline);
        baseline.Count.Should().Be(2_000);
        new AggregateDataSet(Spec(seed: 8)).Summarize().Checksum.Should().NotBe(baseline.Checksum);
        new AggregateDataSet(Spec(count: 1_999)).Summarize().Checksum.Should().NotBe(baseline.Checksum);
        new AggregateDataSet(Spec(categories: 99)).Summarize().Checksum.Should().NotBe(baseline.Checksum);
        new AggregateDataSet(Spec(regions: 999)).Summarize().Checksum.Should().NotBe(baseline.Checksum);
        var zipf = new AggregateDataSet(Spec(distribution: Zipf())).Summarize();
        zipf.Checksum.Should().NotBe(baseline.Checksum);
        zipf.Distribution.Should().Be("zipfian(0.99)");
        new AggregateDataSet(Spec(distribution: Zipf(0.5))).Summarize().Checksum.Should().NotBe(zipf.Checksum);
    }

    [Theory]
    [InlineData("""{"documentSize":256,"categoryCardinality":100,"regionCardinality":10,"distribution":"uniform"}""", "documentCount")]
    [InlineData("""{"documentCount":10,"documentSize":256,"regionCardinality":10,"distribution":"uniform"}""", "categoryCardinality")]
    [InlineData("""{"documentCount":10,"documentSize":256,"categoryCardinality":100,"distribution":"uniform"}""", "regionCardinality")]
    [InlineData("""{"documentCount":10,"documentSize":256,"categoryCardinality":0,"regionCardinality":10,"distribution":"uniform"}""", "categoryCardinality")]
    [InlineData("""{"documentCount":10,"documentSize":256,"categoryCardinality":100,"regionCardinality":-1,"distribution":"uniform"}""", "regionCardinality")]
    [InlineData("""{"documentCount":10,"documentSize":256,"categoryCardinality":100,"regionCardinality":10,"distribution":"pareto"}""", "distribution")]
    [InlineData("""{"documentCount":10,"documentSize":256,"categoryCardinality":100,"regionCardinality":10}""", "distribution")]
    [InlineData("""{"documentCount":10,"documentSize":256,"categoryCardinality":100,"regionCardinality":10,"distribution":"zipfian"}""", "distributionExponent")]
    [InlineData("""{"documentCount":"ten","documentSize":256,"categoryCardinality":100,"regionCardinality":10,"distribution":"uniform"}""", "documentCount")]
    public void Scenario_Validation_Names_The_Parameter(string json, string parameter)
    {
        using var doc = JsonDocument.Parse(json);
        var act = () => AggregateDataSpec.FromJson(doc.RootElement, 1);
        act.Should().Throw<AggregateScenarioException>().Which.Parameter.Should().Be(parameter);
    }

    [Fact]
    public void Scenario_Reads_A_Complete_Zipfian_Set()
    {
        using var doc = JsonDocument.Parse("""{"documentCount":10,"documentSize":1024,"categoryCardinality":100,"regionCardinality":10000,"distribution":"zipfian","distributionExponent":0.9}""");
        var spec = AggregateDataSpec.FromJson(doc.RootElement, 3);
        spec.Distribution.Should().Be(new GroupDistribution(GroupDistribution.Zipfian, 0.9));
        spec.RegionCardinality.Should().Be(10_000);
    }

    [Fact]
    public void Ordering_Compares_Values_As_Numbers_And_Breaks_Ties_By_Ordinal_Key()
    {
        var groups = new[] { new AggregateGroup("a", 9), new AggregateGroup("x", 10), new AggregateGroup("b", 9), new AggregateGroup("B", 9) };

        AggregateOrdering.Top(groups, 10).Should().Equal(
            new AggregateGroup("x", 10), new AggregateGroup("B", 9), new AggregateGroup("a", 9), new AggregateGroup("b", 9));
    }

    [Fact]
    public void Top_N_Cuts_A_Tie_By_Key_And_Returns_Every_Group_When_N_Exceeds_The_Group_Count()
    {
        var groups = new[] { new AggregateGroup("c", 5), new AggregateGroup("b", 5), new AggregateGroup("a", 7) };

        AggregateOrdering.Top(groups, 2).Should().Equal(new AggregateGroup("a", 7), new AggregateGroup("b", 5));
        AggregateOrdering.Top(groups, 50).Should().HaveCount(3);
    }

    [Fact]
    public void Compute_Applies_Equality_And_Half_Open_Range_Filters()
    {
        var docs = new[] { Doc("1", "c1", "r1", 3), Doc("2", "c2", "r1", 4), Doc("3", "c3", "r2", 5) };
        var sum = new GroupedAggregateOperation { GroupBy = "region", Kind = AggregateKind.Sum, SumField = "amount", TopN = 10 };

        AggregateOrdering.Compute(sum, docs).Should().Equal(new AggregateGroup("r1", 7), new AggregateGroup("r2", 5));
        AggregateOrdering.Compute(new GroupedAggregateOperation { GroupBy = "region", Kind = AggregateKind.Sum, SumField = "amount", TopN = 10, Filter = new EqualityFilter("category", "c2") }, docs)
            .Should().Equal(new AggregateGroup("r1", 4));
        AggregateOrdering.Compute(new GroupedAggregateOperation { GroupBy = "category", Kind = AggregateKind.Count, TopN = 10, Filter = new RangeFilter("category", "c2", "c3") }, docs)
            .Should().Equal(new AggregateGroup("c2", 1));
        AggregateOrdering.Compute(new GroupedAggregateOperation { GroupBy = "region", Kind = AggregateKind.Count, TopN = 10, Filter = new EqualityFilter("category", "none") }, docs)
            .Should().BeEmpty();
    }

    [Fact]
    public void Operation_Validation_Rejects_An_Invalid_Shape()
    {
        new Action(() => new GroupedAggregateOperation { GroupBy = "region", Kind = AggregateKind.Count, TopN = 0 }.Validate()).Should().Throw<ArgumentOutOfRangeException>();
        new Action(() => new GroupedAggregateOperation { GroupBy = "region", Kind = AggregateKind.Sum, TopN = 1 }.Validate()).Should().Throw<ArgumentException>();
        new Action(() => new GroupedAggregateOperation { GroupBy = "region", Kind = AggregateKind.Count, SumField = "amount", TopN = 1 }.Validate()).Should().Throw<ArgumentException>();
        new Action(() => AggregateShapes.Create("median-by-region", 1)).Should().Throw<ArgumentException>();
    }

    [Fact]
    public void Index_Resource_Names_Hold_The_Product_Folder_Once_Per_Shape()
    {
        var assembly = typeof(AggregateShapes).Assembly;
        var names = assembly.GetManifestResourceNames().Where(n => n.Contains(".Aggregate.Indexes.", StringComparison.Ordinal)).ToList();
        var expected = new[] { "ravendb", "mongodb" }
            .SelectMany(p => AggregateShapes.All.Select(s => $"{assembly.GetName().Name}.Aggregate.Indexes.{p}.{s}.json"));

        names.Should().OnlyHaveUniqueItems().And.BeSubsetOf(expected);
        names.Should().Contain(AggregateShapes.All.Select(s => $"{assembly.GetName().Name}.Aggregate.Indexes.ravendb.{s}.json"));
    }

    [Fact]
    public void Every_Shape_Has_One_RavenDb_Index_File_And_Every_MongoDb_Index_File_Belongs_To_A_Shape()
    {
        var ravenNames = AggregateShapes.RavenDbIndexes().Select(i => i.GetProperty("Name").GetString()).ToList();
        var shapeNames = AggregateShapes.All.Select(s => AggregateShapes.Create(s, 1, "c1").IndexName).ToList();

        ravenNames.Should().BeEquivalentTo(shapeNames);
        AggregateShapes.MongoIndexes().Should().HaveCount(AggregateShapes.All.Count(s => AggregateShapes.MongoIndexFor(s) is not null));
    }

    [Fact]
    public void A_MongoDb_Index_Exists_Only_When_Its_Shape_Pipeline_Starts_On_The_Index_Leading_Key()
    {
        // INVARIANT: the planner uses an index for an aggregate only through a leading $match or $sort
        // on the index's leading key; a pipeline that starts with $group scans the collection.
        foreach (var shape in AggregateShapes.All)
        {
            if (AggregateShapes.MongoIndexFor(shape) is not { } index)
                continue;
            var leadingKey = index.GetProperty("key").EnumerateObject().First().Name;
            var first = MongoYcsbTransport.AggregatePipeline(AggregateShapes.Create(shape, 1, "c1"))[0];

            first.Names.First().Should().BeOneOf(new[] { "$match", "$sort" }, $"the {shape} index leads on {leadingKey}");
            first[0].AsBsonDocument.Names.First().Should().Be(leadingKey, $"the {shape} pipeline must start on its index's leading key");
        }
    }

    [Fact]
    public void Both_Products_Store_The_Emitted_Field_Values_Unchanged()
    {
        var d = new AggregateDataSet(Spec(count: 1)).Generate().Single();
        using var raven = JsonDocument.Parse(RawHttpTransport.ToRavenJson(d));
        var mongo = MongoYcsbTransport.ToStoredAggregate(d);

        foreach (var field in new[] { "category", "region", "timestamp", "payload" })
            raven.RootElement.GetProperty(field).GetString().Should().Be(mongo[field].AsString);
        raven.RootElement.GetProperty("amount").GetInt64().Should().Be(d.Amount).And.Be(mongo["amount"].AsInt64);
    }

    [Fact]
    public void Compare_Detects_A_Changed_Value_A_Changed_Order_And_A_Missing_Group()
    {
        var expected = new[] { new AggregateGroup("a", 3), new AggregateGroup("b", 2) };

        AggregateParityCheck.Compare("mongodb", "sum-by-region", expected, expected.ToArray()).Should().BeNull();
        AggregateParityCheck.Compare("mongodb", "sum-by-region", expected, [new("a", 3), new("b", 1)])
            .Should().Be("mongodb sum-by-region position 1: expected (b, 2), actual (b, 1)");
        AggregateParityCheck.Compare("mongodb", "sum-by-region", expected, [new("b", 2), new("a", 3)])
            .Should().Be("mongodb sum-by-region position 0: expected (a, 3), actual (b, 2)");
        AggregateParityCheck.Compare("mongodb", "sum-by-region", expected, [new("a", 3)])
            .Should().Be("mongodb sum-by-region position 1: expected (b, 2), actual (none)");
    }

    [Fact]
    public void A_Pair_That_Compared_No_Group_Is_Not_An_Agreement()
    {
        new AggregateParityPair("count-by-category", "mongodb", 0, null, null).Agreed.Should().BeFalse();
        new AggregateParityPair("count-by-category", "mongodb", 1, null, null).Agreed.Should().BeTrue();
    }

    internal static AggregateDocument Doc(string id, string category, string region, long amount, string timestamp = "2025-01-01T00:00:00Z") =>
        new("aggregates/" + id, category, region, amount, timestamp, "");
}
