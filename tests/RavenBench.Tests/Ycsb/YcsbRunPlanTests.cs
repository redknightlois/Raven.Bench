using System.Collections.Generic;
using System.Linq;
using FluentAssertions;
using RavenBench.Core;
using RavenBench.Core.Reporting;
using RavenBench.Core.Ycsb;
using Xunit;

namespace RavenBench.Tests.Ycsb;

/// <summary>
/// The set one invocation produces, the identity each run of it carries, the per-run seed and the
/// median rule. These are relationships to the scenario file, not counted literals.
/// </summary>
public class YcsbRunPlanTests
{
    private static YcsbScenario Scenario(double[]? rates = null, string[]? distributions = null, int? repetitions = null) => new()
    {
        Seed = 7,
        Target = "ravendb",
        DocumentCount = 100,
        DocumentSize = "1KB",
        Concurrency = "4..8x2",
        Distribution = "uniform",
        Warmup = "1s",
        Duration = "2s",
        Rates = rates,
        Distributions = distributions,
        Repetitions = repetitions
    };

    private static int ExpectedCount(YcsbScenario scenario) =>
        1 + scenario.ResolvedRepetitions * (scenario.ResolvedDistributions.Count + 3 + scenario.ResolvedRates.Count);

    [Fact]
    public void A_Scenario_Naming_Nothing_Extra_Produces_The_Base_Sequence()
    {
        var plan = YcsbRunPlan.Build(Scenario());

        plan.Should().HaveCount(ExpectedCount(Scenario()));
        plan.Select(r => r.Kind).Should().Equal(
            YcsbRunKind.Load, YcsbRunKind.WorkloadC, YcsbRunKind.WorkloadA, YcsbRunKind.WorkloadB, YcsbRunKind.InsertStream);
        plan.Should().OnlyContain(r => r.Shape == LoadShape.Closed);
        plan.Should().OnlyContain(r => r.Rate == null);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    public void The_Produced_Set_Follows_Every_Key_The_File_Names(int repetitions)
    {
        var scenario = Scenario(rates: new[] { 500.0, 1500.0 }, distributions: new[] { "uniform", "zipfian" }, repetitions: repetitions);
        var plan = YcsbRunPlan.Build(scenario);

        plan.Should().HaveCount(ExpectedCount(scenario));

        // A named distribution adds C rows alone; a named rate adds rate-shaped C rows alone.
        plan.Count(r => r.Kind == YcsbRunKind.WorkloadC && r.Shape == LoadShape.Closed)
            .Should().Be(repetitions * scenario.ResolvedDistributions.Count);
        plan.Count(r => r.Shape == LoadShape.Rate).Should().Be(repetitions * scenario.ResolvedRates.Count);
        plan.Where(r => r.Shape == LoadShape.Rate).Select(r => r.Rate).Distinct()
            .Should().BeEquivalentTo(scenario.ResolvedRates);
        plan.Should().OnlyContain(r => r.Shape == LoadShape.Closed || r.Kind == YcsbRunKind.WorkloadC);
    }

    [Fact]
    public void The_Load_Run_Is_Not_Multiplied_By_A_Shape_A_Distribution_Or_A_Repetition()
    {
        var plan = YcsbRunPlan.Build(Scenario(rates: new[] { 100.0 }, distributions: new[] { "uniform", "latest" }, repetitions: 3));

        plan.Count(r => r.Kind == YcsbRunKind.Load).Should().Be(1, "the keyspace is loaded once per invocation");
    }

    [Fact]
    public void Workload_C_Runs_Under_Every_Distribution_The_Scenario_Names()
    {
        var scenario = Scenario(distributions: new[] { "uniform", "zipfian", "latest" });
        var plan = YcsbRunPlan.Build(scenario);

        plan.Where(r => r.Kind == YcsbRunKind.WorkloadC && r.Shape == LoadShape.Closed)
            .Select(r => r.Distribution)
            .Should().BeEquivalentTo(scenario.ResolvedDistributions);
        plan.Where(r => r.Kind != YcsbRunKind.WorkloadC).Select(r => r.Distribution)
            .Should().AllBe(scenario.ResolvedDistributions[0], "every other run uses the first named distribution");
    }

    [Fact]
    public void No_Two_Runs_Of_One_Invocation_Share_An_Identity_Or_A_Result_Name()
    {
        var plan = YcsbRunPlan.Build(Scenario(rates: new[] { 100.0, 200.0 }, distributions: new[] { "uniform", "zipfian" }, repetitions: 3));

        plan.Select(r => (r.Kind, r.Shape, r.Distribution, r.Rate, r.Repetition)).Should().OnlyHaveUniqueItems();
        plan.Select(r => r.ResultName).Should().OnlyHaveUniqueItems();
    }

    [Fact]
    public void A_Row_Is_Every_Repetition_That_Shares_The_Rest_Of_The_Identity()
    {
        var plan = YcsbRunPlan.Build(Scenario(rates: new[] { 100.0 }, distributions: new[] { "uniform", "zipfian" }, repetitions: 3));

        foreach (var row in plan.Where(r => r.Kind != YcsbRunKind.Load).GroupBy(r => r.RowKey))
        {
            row.Select(r => r.Repetition).Should().BeEquivalentTo(new[] { 1, 2, 3 });
            row.Select(r => (r.Kind, r.Shape, r.Distribution, r.Rate)).Distinct().Should().HaveCount(1);
        }
    }

    [Fact]
    public void The_Single_Rate_Key_Still_Names_One_Rate()
    {
        var scenario = Scenario() with { Rate = 250 };

        scenario.ResolvedRates.Should().Equal(250.0);
        YcsbRunPlan.Build(scenario).Count(r => r.Shape == LoadShape.Rate).Should().Be(1);
    }

    [Fact]
    public void A_Runs_Seed_Is_A_Pure_Function_Of_The_Scenario_Seed_And_Its_Identity()
    {
        var plan = YcsbRunPlan.Build(Scenario(distributions: new[] { "uniform", "zipfian" }, repetitions: 3));

        var seeds = plan.Select(r => SeedMixer.Derive(7, r.ResultName)).ToList();

        seeds.Should().OnlyHaveUniqueItems("two repetitions of one row draw two streams");
        seeds.Should().Equal(plan.Select(r => SeedMixer.Derive(7, r.ResultName)), "two invocations repeat the streams");
    }

    [Fact]
    public void The_Run_Seed_Does_Not_Alias_Across_Scenario_Seeds_And_Repetitions()
    {
        for (int seed = 1; seed < 40; seed++)
        {
            for (int repetition = 1; repetition < 10; repetition++)
            {
                SeedMixer.Derive(seed + 1, $"C-closed-uniform-rep{repetition}")
                    .Should().NotBe(SeedMixer.Derive(seed, $"C-closed-uniform-rep{repetition + 1}"),
                        "an additive derivation aliases");
            }
        }
    }

    private static IReadOnlyList<StepResult> StepsWith(double throughput, string? invalidReason = null) =>
        new[] { new StepResult { Concurrency = 4, Throughput = throughput, InvalidReason = invalidReason } };

    private static IReadOnlyList<(string Row, IReadOnlyList<StepResult> Steps)> SelectMedians(
        params (string Row, IReadOnlyList<StepResult> Steps)[] results) =>
        YcsbMedianSelector.SelectMedians(results, r => r.Row, r => r.Steps);

    [Fact]
    public void The_Median_Follows_The_Statistic_And_Not_The_Repetition_Order()
    {
        var medians = SelectMedians(("row", StepsWith(300)), ("row", StepsWith(200)), ("row", StepsWith(100)));

        medians.Should().ContainSingle();
        YcsbMedianSelector.Statistic(medians[0].Steps).Should().Be(200, "the median of the statistic, not the middle position");
    }

    [Fact]
    public void Exactly_One_Repetition_Of_Each_Row_Is_The_Median_And_Rows_Are_Never_Compared()
    {
        var medians = SelectMedians(
            ("C", StepsWith(100)), ("C", StepsWith(300)), ("C", StepsWith(200)),
            ("A", StepsWith(10)), ("A", StepsWith(30)), ("A", StepsWith(20)));

        medians.Select(m => m.Row).Should().BeEquivalentTo(new[] { "C", "A" });
        medians.Select(m => YcsbMedianSelector.Statistic(m.Steps)).Should().BeEquivalentTo(new[] { 200.0, 20.0 });
    }

    [Fact]
    public void A_Single_Repetition_Is_Its_Own_Row_Median()
    {
        SelectMedians(("row", StepsWith(42))).Should().ContainSingle();
    }

    [Fact]
    public void An_Even_Count_Marks_The_Lower_Of_The_Two_Middle_Results()
    {
        var medians = SelectMedians(
            ("row", StepsWith(100)), ("row", StepsWith(200)), ("row", StepsWith(300)), ("row", StepsWith(400)));

        YcsbMedianSelector.Statistic(medians[0].Steps).Should().Be(200);
    }

    [Fact]
    public void An_Invalid_Repetition_Is_Left_Out_Of_The_Selection()
    {
        var medians = SelectMedians(
            ("row", StepsWith(100)),
            ("row", StepsWith(500, invalidReason: "the load host was saturated")));

        YcsbMedianSelector.Statistic(medians[0].Steps).Should().Be(100,
            "a client-bound repetition must not be published as the row");
    }

    [Fact]
    public void A_Row_Whose_Every_Repetition_Is_Invalid_Carries_No_Median()
    {
        SelectMedians(
            ("row", StepsWith(100, invalidReason: "the load host was saturated")),
            ("row", StepsWith(200, invalidReason: "the load host was saturated")))
            .Should().BeEmpty();
    }
}
