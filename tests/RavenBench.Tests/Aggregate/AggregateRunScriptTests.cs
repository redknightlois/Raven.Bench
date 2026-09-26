using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using RavenBench.Tests.Infrastructure;
using Xunit;
using static RavenBench.Tests.VectorBench.VectorRunScriptTests;

namespace RavenBench.Tests.Aggregate;

/// <summary>Drives the aggregate folder's entry script and holds it to the vector script's endpoint and Docker behaviour.</summary>
[Collection(LiveServers.Name)]
public class AggregateRunScriptTests
{
    private static string Script => Path.Combine(Folder("aggregate"), "run.sh");

    [Fact]
    public void The_Folder_Has_The_Script_The_Compose_File_The_Scenario_And_The_Readme()
    {
        foreach (var file in new[] { "run.sh", "docker-compose.yml", "README.md", "scenario.json" })
            File.Exists(Path.Combine(Folder("aggregate"), file)).Should().BeTrue(file);
    }

    [Fact]
    public void The_Compose_File_Pins_A_Mongodb_8_0_Release_And_Ravendb_7()
    {
        var compose = File.ReadAllText(Path.Combine(Folder("aggregate"), "docker-compose.yml"));
        compose.Should().MatchRegex(@"image: mongo:8\.0\.\d+\s", "a release tag, not the floating 8.0");
        compose.Should().MatchRegex(@"image: ravendb/ravendb:7\.\d+\.\d+\s");
        compose.Should().NotContain("latest").And.NotContain("8080").And.NotContain("8081");
    }

    [Fact]
    public void The_Readme_Documents_The_Command_The_Rows_Freshness_And_Adding_A_Shape()
    {
        var readme = File.ReadAllText(Path.Combine(Folder("aggregate"), "README.md"));
        readme.Should().Contain("./benchmarks/aggregate/run.sh --target");
        foreach (var text in new[] { "Docker is not installed", "the daemon is not reachable", "The database host is not the client", "node_exporter", "Freshness", "Adding a query shape" })
            readme.Should().Contain(text);
        foreach (var run in new[] { "`build`", "`count-by-category`", "`sum-by-region`", "`filtered-group`", "`under-write`" })
            readme.Should().Contain(run);
    }

    [RequiresBashFact]
    public void An_Unknown_Target_Exits_Non_Zero_And_Names_The_Value()
    {
        var (exitCode, output) = RunBash(Script, ["--target", "pgvector"], null);
        exitCode.Should().NotBe(0);
        output.Should().Contain("'pgvector'").And.Contain("ravendb, ravendb-7, mongodb, mongodb-indexed");
    }

    [RequiresBashFact]
    public void The_Three_Docker_Messages_Match_The_Vector_Script_But_For_The_Folder()
    {
        var port = FreeTcpPort().ToString(CultureInfo.InvariantCulture);
        foreach (var docker in new[] { FakeDocker.Missing, FakeDocker.NoDaemon, FakeDocker.ComposeFails })
        {
            var bin = FakeBin(docker);
            try
            {
                var env = new Dictionary<string, string> { ["PATH"] = bin, ["HOME"] = bin, ["FAKE_LOG"] = Path.Combine(bin, "calls.log"), ["RAVENDB7_PORT"] = port, ["VECTOR_READY_TIMEOUT"] = "1", ["AGGREGATE_READY_TIMEOUT"] = "1" };
                var aggregate = RunBash(Script, ["--target", "ravendb-7"], env, clear: true);
                var vector = RunBash(Path.Combine(Folder("vector"), "run.sh"), ["--target", "ravendb-7"], env, clear: true);
                aggregate.ExitCode.Should().NotBe(0, docker.ToString());
                aggregate.Output.Should().Contain("error:", docker.ToString());
                aggregate.Output.Replace("benchmarks/aggregate", "benchmarks/<folder>").Should().Be(vector.Output.Replace("benchmarks/vector", "benchmarks/<folder>"), docker.ToString());
            }
            finally
            {
                Directory.Delete(bin, recursive: true);
            }
        }
    }

    [RequiresBashFact]
    public void A_Caller_Mongodb_Url_That_Answers_Makes_No_Docker_Call_And_Is_Forwarded_As_Given()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        var bin = FakeBin(FakeDocker.Missing);
        try
        {
            var url = $"mongodb://127.0.0.1:{port}";
            var (exitCode, output) = RunBash(Script, ["--target", "mongodb-indexed", "--url", url, "--documents", "1000", "--output-prefix", Path.Combine(bin, "out")],
                new Dictionary<string, string> { ["PATH"] = bin, ["HOME"] = bin, ["FAKE_LOG"] = Path.Combine(bin, "calls.log") }, clear: true);

            exitCode.Should().Be(0, output);
            output.Should().NotContain("Docker");
            var forwarded = Calls(bin).Split('\n').Single(l => l.StartsWith("dotnet "));
            forwarded.Should().Contain($"--url {url}").And.Contain("aggregate --target mongodb-indexed").And.Contain("--database aggregate_mongodb_indexed_").And.Contain("--documents 1000");
            forwarded.Split(' ').Count(a => a == "--url").Should().Be(1, "a caller url replaces the default");
        }
        finally
        {
            Directory.Delete(bin, recursive: true);
        }
    }

    [RequiresBashFact]
    public void The_External_Ravendb_Target_Never_Starts_A_Container()
    {
        var bin = FakeBin(FakeDocker.ComposeFails);
        try
        {
            RunBash(Script, ["--target", "ravendb", "--output-prefix", Path.Combine(bin, "out")],
                new Dictionary<string, string> { ["PATH"] = bin, ["HOME"] = bin, ["FAKE_LOG"] = Path.Combine(bin, "calls.log") }, clear: true);
            Calls(bin).Should().NotContain("docker");
        }
        finally
        {
            Directory.Delete(bin, recursive: true);
        }
    }
}
