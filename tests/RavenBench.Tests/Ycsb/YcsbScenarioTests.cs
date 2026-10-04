using System;
using System.IO;
using FluentAssertions;
using RavenBench.Core.Diagnostics;
using RavenBench.Core.Ycsb;
using Xunit;

namespace RavenBench.Tests.Ycsb;

public class YcsbScenarioTests
{
    private const string ValidJson = """
        {
          "Seed": 7,
          "Target": "ravendb",
          "DocumentCount": 1000,
          "DocumentSize": "2KB",
          "Concurrency": "8..32x2",
          "Distribution": "zipfian",
          "Warmup": "5s",
          "Duration": "15s"
        }
        """;

    [Fact]
    public void Load_Reads_Every_Field_From_The_File()
    {
        var path = WriteTemp(ValidJson);
        var scenario = YcsbScenario.Load(path);

        scenario.Seed.Should().Be(7);
        scenario.DocumentCount.Should().Be(1000);
        scenario.DocumentSize.Should().Be("2KB");
        scenario.Concurrency.Should().Be("8..32x2");
        scenario.Distribution.Should().Be("zipfian");
        scenario.Warmup.Should().Be("5s");
        scenario.Duration.Should().Be("15s");
        scenario.Rate.Should().BeNull();
    }

    [Fact]
    public void Load_Missing_File_Throws_Naming_The_Path()
    {
        var path = Path.Combine(Path.GetTempPath(), Guid.NewGuid() + ".json");
        var act = () => YcsbScenario.Load(path);
        act.Should().Throw<YcsbScenarioException>().WithMessage($"*{path}*");
    }

    [Fact]
    public void Load_Missing_Required_Key_Names_The_Key()
    {
        var path = WriteTemp("""{ "Seed": 1 }""");
        var act = () => YcsbScenario.Load(path);
        act.Should().Throw<YcsbScenarioException>().WithMessage("*DocumentCount*");
    }

    [Fact]
    public void Load_Unknown_Key_Names_The_Key()
    {
        var path = WriteTemp("""
            {
              "Seed": 1,
              "Target": "ravendb",
              "DocumentCount": 10,
              "DocumentSize": "1KB",
              "Concurrency": "8..8",
              "Distribution": "uniform",
              "Warmup": "1s",
              "Duration": "1s",
              "TotallyUnknown": 1
            }
            """);
        var act = () => YcsbScenario.Load(path);
        act.Should().Throw<YcsbScenarioException>().WithMessage("*TotallyUnknown*");
    }

    [Fact]
    public void Load_Non_Json_File_Throws()
    {
        var path = WriteTemp("not json at all {{{");
        var act = () => YcsbScenario.Load(path);
        act.Should().Throw<YcsbScenarioException>();
    }

    [Fact]
    public void Rate_Is_Optional_And_Round_Trips_When_Present()
    {
        var path = WriteTemp("""
            {
              "Seed": 1,
              "Target": "ravendb",
              "DocumentCount": 10,
              "DocumentSize": "1KB",
              "Concurrency": "8..8",
              "Rate": 500,
              "Distribution": "uniform",
              "Warmup": "1s",
              "Duration": "1s"
            }
            """);
        YcsbScenario.Load(path).Rate.Should().Be(500);
    }

    private static string SetKeys(string keys) => $$"""
        {
          "Seed": 1,
          "Target": "ravendb",
          "DocumentCount": 10,
          "DocumentSize": "1KB",
          "Concurrency": "8..8",
          "Distribution": "uniform",
          "Warmup": "1s",
          "Duration": "1s",
          {{keys}}
        }
        """;

    [Fact]
    public void The_Set_Keys_Are_Read_From_The_File()
    {
        var scenario = YcsbScenario.Load(WriteTemp(SetKeys("""
            "Rates": [ 500, 1500 ],
            "Distributions": [ "uniform", "zipfian" ],
            "Repetitions": 3
            """)));

        scenario.ResolvedRates.Should().Equal(500.0, 1500.0);
        scenario.ResolvedDistributions.Should().Equal("uniform", "zipfian");
        scenario.ResolvedRepetitions.Should().Be(3);
    }

    [Fact]
    public void The_Set_Keys_Are_Optional_And_Fall_Back_To_The_Single_Value_Keys()
    {
        var scenario = YcsbScenario.Load(WriteTemp(SetKeys(""" "Rate": null """)));

        scenario.ResolvedRates.Should().BeEmpty("an absent rate list means no fixed-rate run");
        scenario.ResolvedDistributions.Should().Equal(scenario.Distribution);
        scenario.ResolvedRepetitions.Should().Be(1);
    }

    [Theory]
    [InlineData(""" "Rates": [ 500, 0 ] """, "Rates")]
    [InlineData(""" "Rates": [ -1 ] """, "Rates")]
    [InlineData(""" "Distributions": [ "gaussian" ] """, "Distributions")]
    [InlineData(""" "Distributions": [] """, "Distributions")]
    [InlineData(""" "Repetitions": 0 """, "Repetitions")]
    [InlineData(""" "Repetitions": -3 """, "Repetitions")]
    [InlineData(""" "Rate": 100, "Rates": [ 200 ] """, "Rates")]
    public void A_Value_Outside_Its_Domain_Fails_Naming_The_Key(string keys, string expectedKey)
    {
        var act = () => YcsbScenario.Load(WriteTemp(SetKeys(keys)));

        act.Should().Throw<YcsbScenarioException>().WithMessage($"*{expectedKey}*");
    }

    [Fact]
    public void An_Unknown_Key_Beside_The_Set_Keys_Is_Still_Rejected()
    {
        var act = () => YcsbScenario.Load(WriteTemp(SetKeys(""" "Repetitions": 2, "Reps": 3 """)));

        act.Should().Throw<YcsbScenarioException>().WithMessage("*Reps*");
    }

    [Fact]
    public void The_Checked_In_Scenario_Names_A_Set_And_Loads()
    {
        var path = Path.Combine(RepositoryRootLocator.Find(), "benchmarks", "ycsb", "scenario.json");
        var scenario = YcsbScenario.Load(path);

        YcsbRunPlan.Build(scenario).Should().HaveCount(
            1 + scenario.ResolvedRepetitions * (scenario.ResolvedDistributions.Count + 3 + scenario.ResolvedRates.Count));
    }

    private static string WriteTemp(string content)
    {
        var path = Path.Combine(Path.GetTempPath(), Guid.NewGuid() + ".json");
        File.WriteAllText(path, content);
        return path;
    }

    private static YcsbScenario Valid => new()
    {
        Seed = 7, Target = "ravendb", DocumentCount = 1000, DocumentSize = "2KB", Concurrency = "8..32x2",
        Distribution = "zipfian", Warmup = "5s", Duration = "15s"
    };

    [Theory]
    [InlineData("7")]
    [InlineData("bogus")]
    public void Validate_Rejects_A_Distribution_The_Runner_Cannot_Run_In_Either_Key(string distribution)
    {
        var single = () => (Valid with { Distribution = distribution }).Validate();
        var list = () => (Valid with { Distributions = new[] { "uniform", distribution } }).Validate();

        single.Should().Throw<YcsbScenarioException>().WithMessage("*'Distribution'*");
        list.Should().Throw<YcsbScenarioException>().WithMessage("*'Distributions'*");
        RavenBench.Core.Workload.KeyDistributions.TryParse(distribution, out _).Should().BeFalse();
    }
}
