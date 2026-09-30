using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json.Nodes;
using FluentAssertions;
using RavenBench.Cli;
using RavenBench.Core.Diagnostics;
using RavenBench.Core.Transport;
using RavenBench.Core.Vector;
using RavenBench.Core.Workload;
using RavenBench.VectorBench;
using Xunit;

namespace RavenBench.Tests.VectorBench;

public class VectorScenarioTests
{
    private static string ShippedScenario => Path.Combine(RepositoryRootLocator.Find(), "benchmarks", "vector", "scenario.json");

    [Fact]
    public void The_Shipped_Scenario_Loads_With_Every_Parameter()
    {
        var scenario = VectorScenario.Load(ShippedScenario);
        scenario.EffortsFor("ravendb").Knob.Should().Be(SearchEffort.RavenDbKnob);
        scenario.EffortsFor("pgvector").Knob.Should().Be(SearchEffort.PgVectorKnob);
        scenario.FilterSelectivity.Should().Be(0.05);
        scenario.InsertRate.Should().Be(500);
        scenario.Readers.Should().Be(1000);
        scenario.CrossCheck.PublishedRecall.Should().BeInRange(0.5, 1.0);
        scenario.CrossCheck.Source.Should().StartWith("https://ann-benchmarks.com/");
    }

    [Fact]
    public void A_Null_Published_Recall_Fails_By_Name()
    {
        var json = JsonNode.Parse(File.ReadAllText(ShippedScenario))!.AsObject();
        json["CrossCheck"]!["PublishedRecall"] = null;
        var path = Path.GetTempFileName();
        File.WriteAllText(path, json.ToJsonString());
        try
        {
            var act = () => VectorScenario.Load(path);
            act.Should().Throw<System.Exception>().WithMessage("*PublishedRecall*");
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void A_Missing_Required_Parameter_Fails_By_Name()
    {
        var json = JsonNode.Parse(File.ReadAllText(ShippedScenario))!.AsObject();
        json.Remove("InsertRate");
        var path = Path.GetTempFileName();
        File.WriteAllText(path, json.ToJsonString());
        try
        {
            var act = () => VectorScenario.Load(path);
            act.Should().Throw<VectorScenarioException>().WithMessage("*InsertRate*");
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void An_Explicit_Override_Is_Recorded_Even_When_It_Equals_The_File()
    {
        var file = VectorScenario.Load(ShippedScenario);

        var (_, withSeed) = VectorScenarioResolver.Resolve(file, new VectorSettings { Seed = file.Seed }, ["vector", "--seed", file.Seed.ToString()]);
        withSeed.Should().Contain("--seed", file.Seed.ToString());

        var (capped, withCap) = VectorScenarioResolver.Resolve(file, new VectorSettings { VectorCountCap = 5000 }, ["vector", "--vector-count-cap", "5000"]);
        withCap.Should().Contain("--vector-count-cap", "5000");
        capped.VectorCountCap.Should().Be(5000);

        var (resolved, none) = VectorScenarioResolver.Resolve(file, new VectorSettings { Seed = file.Seed + 1 }, ["vector"]);
        none.Should().BeEmpty();
        resolved.Should().Be(file);
    }

    [Fact]
    public void The_Cross_Check_Figure_Is_In_The_Pinned_Evidence()
    {
        var check = VectorScenario.Load(ShippedScenario).CrossCheck;
        var path = Path.Combine(Path.GetDirectoryName(ShippedScenario)!, check.Evidence);

        var bytes = File.ReadAllBytes(path);
        System.Convert.ToHexStringLower(System.Security.Cryptography.SHA256.HashData(bytes)).Should().Be(check.EvidenceSha256);
        var point = System.Array.Find(File.ReadAllLines(path), l => l.Contains(check.PublishedSettings.Split(',')[0] + "," + check.PublishedSettings.Split(',')[1]));
        point.Should().NotBeNull();
        double.Parse(point!.Split(' ')[2], System.Globalization.CultureInfo.InvariantCulture).Should().BeApproximately(check.PublishedRecall, 1e-5);
        point.Should().StartWith("{ x:").And.Contain("PGVector(lists=");
        check.PublishedSettings.Should().Contain($"lists={check.PublishedBuildOptions["lists"]}, probes={check.PublishedSearchValue}", "the harness rebuilds the published index");
    }

    [Fact]
    public void The_Shipped_Scenario_Names_Each_Elasticsearch_Kind_By_Its_Own_Knob()
    {
        var scenario = VectorScenario.Load(ShippedScenario);
        foreach (var kind in ElasticsearchIndexKind.All)
        {
            using var target = VectorRunner.BuildTarget("elasticsearch", "http://localhost:1", "unused", VectorMetric.Cosine, 8, 1, scenario with { ElasticsearchIndexKind = kind.Name });
            scenario.EffortsFor(target.EffortFamily).Knob.Should().Be(kind.Knob).And.Be(target.Effort(1).Knob);
        }
        scenario.EffortsFor("elasticsearch-hnsw").Knob.Should().Be(ElasticsearchIndexKind.CandidatesKnob);
        scenario.EffortsFor("elasticsearch-bbq_disk").Knob.Should().Be(ElasticsearchIndexKind.VisitKnob);
    }

    [Fact]
    public void The_Index_Kind_Override_Is_Recorded()
    {
        var file = VectorScenario.Load(ShippedScenario);
        var (scenario, overrides) = VectorScenarioResolver.Resolve(file, new VectorSettings { ElasticsearchIndexKind = "bbq_disk" },
            ["vector", "--elasticsearch-index-kind", "bbq_disk"]);
        scenario.ElasticsearchIndexKind.Should().Be("bbq_disk");
        overrides.Should().Contain("--elasticsearch-index-kind", "bbq_disk");
    }

    [Fact]
    public void An_Unknown_Index_Kind_Fails_By_Key()
    {
        var file = VectorScenario.Load(ShippedScenario);
        var act = () => VectorScenarioResolver.Resolve(file, new VectorSettings { ElasticsearchIndexKind = "int8_hnsw" }, ["vector", "--elasticsearch-index-kind", "int8_hnsw"]);
        act.Should().Throw<VectorScenarioException>().WithMessage("*ElasticsearchIndexKind*int8_hnsw*");
    }

    [Fact]
    public void An_Override_Outside_Its_Domain_Fails_By_Key()
    {
        var file = VectorScenario.Load(ShippedScenario);
        var act = () => VectorScenarioResolver.Resolve(file, new VectorSettings { FilterSelectivity = 1.5 }, ["vector", "--filter-selectivity=1.5"]);
        act.Should().Throw<VectorScenarioException>().WithMessage("*FilterSelectivity*");
    }

    [Fact]
    public void A_Target_That_Cannot_Serve_The_Metric_Is_Refused_By_Name_Before_Load()
    {
        var act = () => VectorRunner.BuildTarget("ravendb", "http://localhost:1", "unused", VectorMetric.L2, 8, 1, VectorScenario.Load(ShippedScenario));
        act.Should().Throw<UnsupportedVectorMetricException>().Where(e => e.Metric == VectorMetric.L2).WithMessage("*L2*");
    }

    [Fact]
    public void An_Unknown_Target_Or_Set_Fails_By_Name()
    {
        FluentActions.Invoking(() => VectorRunner.BuildTarget("elastic", "x", "y", VectorMetric.Cosine, 8, 1, VectorScenario.Load(ShippedScenario)))
            .Should().Throw<VectorScenarioException>().WithMessage("*elastic*");
        FluentActions.Invoking(() => VectorRunner.ResolveSet("nope"))
            .Should().Throw<VectorScenarioException>().WithMessage("*nope*");
    }

    [Fact]
    public void An_Insert_Slice_Covering_The_Base_Is_Refused_With_Every_Key_Option_And_Needed_Value()
    {
        var scenario = VectorScenario.Load(ShippedScenario) with { InsertRate = 500, Warmup = "10s", Duration = "30s" };

        var act = () => scenario.RequireInsertSliceBelow(20000, TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(30));

        var message = act.Should().Throw<InsertSliceTooLargeException>().Which.Message;
        foreach (var name in new[] { "'InsertRate'", "--insert-rate", "'Warmup'", "--warmup", "'Duration'", "--duration", "'VectorCountCap'", "--vector-count-cap" })
            message.Should().Contain(name);
        // (20000 - 1) / 40 s, (20000 - 1) / 500 per s, 20000 + 1.
        message.Should().Contain("at most 499.975;").And.Contain("at most 39.998s").And.Contain("at least 20001");
    }

    [Fact]
    public void The_Readme_Small_Set_Fits_The_Shipped_Scenario()
    {
        var scenario = VectorScenario.Load(ShippedScenario) with { InsertRate = 100 };
        var readme = File.ReadAllText(Path.Combine(RepositoryRootLocator.Find(), "benchmarks", "vector", "README.md"));
        readme.Should().Contain("--vector-count-cap 20000 --insert-rate 100");

        scenario.RequireInsertSliceBelow(20000, CliParsing.ParseDuration(scenario.Warmup), CliParsing.ParseDuration(scenario.Duration));
    }
}
