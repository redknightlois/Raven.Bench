using FluentAssertions;
using RavenBench.Cli;
using RavenBench.Core.Ycsb;
using RavenBench.Ycsb;
using Xunit;

namespace RavenBench.Tests.Ycsb;

public class YcsbScenarioResolverTests
{
    // Deliberately not 42/"1KB"/"20s"/"60s"/"uniform" (the settings defaults), so an
    // accidental fall-through to the settings default is visible as a wrong resolved value.
    private static readonly YcsbScenario Scenario = new()
    {
        Seed = 7,
        Target = "ravendb",
        DocumentCount = 500,
        DocumentSize = "2KB",
        Concurrency = "4..4",
        Distribution = "zipfian",
        Warmup = "5s",
        Duration = "15s"
    };

    // Seed/DocSize/Warmup/Duration/Distribution/Step left at their BaseRunSettings defaults
    // unless overridden below, which is exactly the ambiguous case resolution must handle.
    private static YcsbSettings DefaultSettings(int? seed = null, string? docSize = null, string? step = null) => new()
    {
        Url = "http://localhost:8081",
        Database = "ycsb",
        Scenario = "scenario.json",
        Seed = seed ?? 42,
        DocSize = docSize ?? "1KB",
        Step = step
    };

    [Fact]
    public void Scenario_Value_Wins_When_Nothing_Was_Given_On_The_Command_Line()
    {
        var args = new[] { "ycsb", "--url", "http://localhost:8081", "--database", "ycsb", "--scenario", "scenario.json" };
        var resolved = YcsbScenarioResolver.Resolve(Scenario, DefaultSettings(), args);

        resolved.Seed.Should().Be(Scenario.Seed);
        resolved.DocumentSize.Should().Be(Scenario.DocumentSize);
        resolved.Warmup.Should().Be(Scenario.Warmup);
        resolved.Duration.Should().Be(Scenario.Duration);
        resolved.Distribution.Should().Be(Scenario.Distribution);
    }

    [Fact]
    public void Explicit_Command_Line_Value_Wins_Over_The_Scenario()
    {
        var settings = DefaultSettings(seed: 99); // an explicit value distinct from the scenario's seed (7)
        var args = new[] { "ycsb", "--seed", "99", "--url", "http://localhost:8081", "--database", "ycsb", "--scenario", "scenario.json" };

        var resolved = YcsbScenarioResolver.Resolve(Scenario, settings, args);

        resolved.Seed.Should().Be(settings.Seed).And.NotBe(Scenario.Seed);
    }

    [Fact]
    public void Explicit_Command_Line_Value_Wins_Even_When_It_Equals_The_Settings_Default()
    {
        // The settings default for --seed is 42, which is also a value a user could legitimately
        // pass explicitly. Resolution must not treat "given" and "equal to default" as the same.
        var settings = DefaultSettings(seed: 42);
        var args = new[] { "ycsb", "--seed", "42", "--url", "http://localhost:8081", "--database", "ycsb", "--scenario", "scenario.json" };

        var resolved = YcsbScenarioResolver.Resolve(Scenario, settings, args);

        resolved.Seed.Should().Be(42).And.NotBe(Scenario.Seed);
    }

    [Fact]
    public void Resolving_The_Same_Inputs_Twice_Gives_The_Same_Result()
    {
        var args = new[] { "ycsb", "--doc-size", "4KB", "--url", "http://localhost:8081", "--database", "ycsb", "--scenario", "scenario.json" };
        var settings = DefaultSettings(docSize: "4KB");

        var first = YcsbScenarioResolver.Resolve(Scenario, settings, args);
        var second = YcsbScenarioResolver.Resolve(Scenario, settings, args);

        second.Should().Be(first);
    }

    [Fact]
    public void Command_Line_Step_Overrides_Scenario_Concurrency()
    {
        var settings = DefaultSettings(step: "16..16");
        var args = new[] { "ycsb", "--step", "16..16", "--url", "http://localhost:8081", "--database", "ycsb", "--scenario", "scenario.json" };

        var resolved = YcsbScenarioResolver.Resolve(Scenario, settings, args);

        resolved.Concurrency.Should().Be("16..16").And.NotBe(Scenario.Concurrency);
    }

    [Fact]
    public void The_File_Names_The_Set_When_The_Command_Line_Does_Not()
    {
        var scenario = Scenario with { Rates = new[] { 700.0 }, Distributions = new[] { "uniform", "latest" }, Repetitions = 4 };
        var args = new[] { "ycsb", "--url", "http://localhost:8081", "--database", "ycsb", "--scenario", "scenario.json" };

        var resolved = YcsbScenarioResolver.Resolve(scenario, DefaultSettings(), args);

        resolved.ResolvedRates.Should().Equal(scenario.ResolvedRates);
        resolved.ResolvedDistributions.Should().Equal(scenario.ResolvedDistributions);
        resolved.ResolvedRepetitions.Should().Be(scenario.ResolvedRepetitions);
    }

    [Fact]
    public void Command_Line_Set_Keys_Beat_The_File()
    {
        var scenario = Scenario with { Rates = new[] { 700.0 }, Distributions = new[] { "uniform" }, Repetitions = 4 };
        var settings = new YcsbSettings
        {
            Url = "http://localhost:8081",
            Database = "ycsb",
            Scenario = "scenario.json",
            Rates = "100,250",
            Distributions = "zipfian,latest",
            Repetitions = 2
        };
        var args = new[] { "ycsb", "--rates", "100,250", "--distributions", "zipfian,latest", "--repetitions", "2" };

        var resolved = YcsbScenarioResolver.Resolve(scenario, settings, args);

        resolved.ResolvedRates.Should().Equal(100.0, 250.0).And.NotEqual(scenario.ResolvedRates);
        resolved.ResolvedDistributions.Should().Equal("zipfian", "latest");
        resolved.ResolvedRepetitions.Should().Be(2).And.NotBe(scenario.ResolvedRepetitions);
    }

    [Fact]
    public void An_Explicit_Distribution_Replaces_The_Files_Distributions()
    {
        var scenario = Scenario with { Distributions = new[] { "uniform", "latest" } };
        var settings = new YcsbSettings { Url = "http://localhost:8081", Database = "ycsb", Scenario = "scenario.json", Distribution = "zipfian" };

        var resolved = YcsbScenarioResolver.Resolve(scenario, settings, new[] { "ycsb", "--distribution", "zipfian" });

        resolved.ResolvedDistributions.Should().Equal("zipfian");
        resolved.Distributions.Should().BeNull("a distribution override replaces both spellings the file may carry");
        YcsbRunPlan.Build(resolved).Should().OnlyContain(run => run.Distribution == "zipfian");
    }

    [Fact]
    public void An_Empty_Rates_Override_Means_No_Fixed_Rate_Run()
    {
        var scenario = Scenario with { Rate = 700 };
        var settings = new YcsbSettings { Url = "http://localhost:8081", Database = "ycsb", Scenario = "scenario.json", Rates = "" };

        var resolved = YcsbScenarioResolver.Resolve(scenario, settings, new[] { "ycsb", "--rates", "" });

        resolved.ResolvedRates.Should().BeEmpty();
        resolved.Rate.Should().BeNull("a rates override replaces both spellings the file may carry");
    }

    [Fact]
    public void A_Resolved_Value_Outside_Its_Domain_Fails_Naming_The_Key()
    {
        var settings = new YcsbSettings { Url = "http://localhost:8081", Database = "ycsb", Scenario = "scenario.json", Repetitions = 0 };

        var act = () => YcsbScenarioResolver.Resolve(Scenario, settings, new[] { "ycsb", "--repetitions", "0" });

        act.Should().Throw<YcsbScenarioException>().WithMessage("*Repetitions*");
    }
}
