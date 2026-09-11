using System;
using System.IO;
using FluentAssertions;
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

    private static string WriteTemp(string content)
    {
        var path = Path.Combine(Path.GetTempPath(), Guid.NewGuid() + ".json");
        File.WriteAllText(path, content);
        return path;
    }
}
