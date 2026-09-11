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
}
