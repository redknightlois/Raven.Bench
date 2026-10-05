using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Text.RegularExpressions;
using FluentAssertions;
using RavenBench.Tests.Infrastructure;
using Xunit;
using static RavenBench.Tests.VectorBench.VectorRunScriptTests;

namespace RavenBench.Tests.Scripts;

/// <summary>Holds the three benchmark scripts to the rules they share through one sourced helper file, against fake tools on PATH.</summary>
[Trait("Category", "Unit")]
public class RunScriptSharedRulesTests
{
    private static readonly string[] Benches = ["ycsb", "vector", "aggregate"];
    private static string Helper => Path.Combine(Folder("ycsb"), "..", "run-common.sh");

    [Fact]
    public void Each_Script_Sources_The_Helper_File_And_Defines_None_Of_Its_Functions()
    {
        var helper = File.ReadAllText(Helper);
        var shared = Regex.Matches(helper, @"^([a-z_]+)\(\) \{", RegexOptions.Multiline).Select(m => m.Groups[1].Value).ToList();
        shared.Should().Contain(["parse_run_options", "has_option", "host_of_url", "port_of_url", "probe_ready", "require_docker_to_start", "run_database_name", "is_local_host", "on_exit"]);
        Regex.Matches(helper, @"^trap ", RegexOptions.Multiline).Should().HaveCount(1, "one exit trap");

        foreach (var bench in Benches)
        {
            var script = File.ReadAllText(Path.Combine(Folder(bench), "run.sh"));
            script.Should().Contain("source \"$SCRIPT_DIR/../run-common.sh\"", bench);
            script.Should().NotMatchRegex(@"(?m)^trap ", bench);
            foreach (var function in shared)
                script.Should().NotContain(function + "() {", $"{bench} uses the helper's {function}");
        }
    }

    [RequiresBashFact]
    public void The_Three_Scripts_Name_The_Run_Database_By_One_Rule()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        foreach (var (bench, target, url) in new[]
                 {
                     ("ycsb", "mongodb", $"mongodb://127.0.0.1:{port}"),
                     ("vector", "pgvector", $"postgresql://bench:bench@127.0.0.1:{port}/bench"),
                     ("aggregate", "mongodb-indexed", $"mongodb://127.0.0.1:{port}")
                 })
        {
            var bin = FakeBin(FakeDocker.Missing);
            try
            {
                var (exitCode, output) = RunBash(Path.Combine(Folder(bench), "run.sh"), ["--target", target, "--url", url, "--output-prefix", Path.Combine(bin, "out")], Env(bin), clear: true);
                exitCode.Should().Be(0, output);
                var forwarded = Calls(bin).Split('\n').Single(l => l.StartsWith("dotnet ", StringComparison.Ordinal));
                forwarded.Should().MatchRegex($@"--database {bench}_{target.Replace('-', '_')}_\d{{8}}t\d{{6}}_\d+ ", bench);
            }
            finally
            {
                Directory.Delete(bin, recursive: true);
            }
        }
    }

    [RequiresBashFact]
    public void A_Failed_Removal_Of_The_Elasticsearch_Data_Directory_Warns_And_Keeps_The_Run_Status()
    {
        var bin = FakeBin(FakeDocker.ComposeFails);
        try
        {
            foreach (var tool in new[] { "mktemp", "chmod" })
                File.CreateSymbolicLink(Path.Combine(bin, tool), Which(tool));
            WriteTool(bin, "rm", "exit 7");
            var data = Directory.CreateDirectory(Path.Combine(bin, "es-data")).FullName;
            var env = Env(bin);
            env["ELASTICSEARCH_PORT"] = FreeTcpPort().ToString(CultureInfo.InvariantCulture);
            env["ELASTICSEARCH_DATA_DIR"] = data;

            var (exitCode, output) = RunBash(Path.Combine(Folder("vector"), "run.sh"), ["--target", "elasticsearch"], env, clear: true);

            Directory.GetDirectories(data).Should().ContainSingle("the run made its own data directory, which the fake rm could not remove");
            exitCode.Should().Be(1, "the failed container start is the run's status, not the status of the removal");
            output.Should().Contain("did not become ready").And.Contain("warning: could not remove the run directory");
        }
        finally
        {
            File.Delete(Path.Combine(bin, "rm"));
            Directory.Delete(bin, recursive: true);
        }
    }

    [RequiresBashFact]
    public void A_Started_Container_Is_Torn_Down_By_One_Rule_That_Names_The_Named_Volume()
    {
        foreach (var bench in Benches)
        {
            var bin = FakeBin(FakeDocker.ComposeFails);
            try
            {
                var env = Env(bin);
                env["RAVENDB7_PORT"] = FreeTcpPort().ToString(CultureInfo.InvariantCulture);

                var (exitCode, output) = RunBash(Path.Combine(Folder(bench), "run.sh"), ["--target", "ravendb-7"], env, clear: true);

                exitCode.Should().Be(1, bench);
                Calls(bin).Should().Contain(" rm -s -f ravendb-7", bench);
                output.Should().Contain("The named volume of the ravendb-7 service keeps the data this run loaded", bench)
                    .And.Contain($"benchmarks/{bench}/docker-compose.yml down -v", bench);
            }
            finally
            {
                Directory.Delete(bin, recursive: true);
            }
        }
    }

    [RequiresBashFact]
    public void Without_Psql_The_Container_Fallback_Runs_Only_For_A_Local_Host()
    {
        var address = NetworkInterface.GetAllNetworkInterfaces()
            .Where(n => n.OperationalStatus == OperationalStatus.Up)
            .SelectMany(n => n.GetIPProperties().UnicastAddresses)
            .Select(u => u.Address)
            .First(a => a.AddressFamily == AddressFamily.InterNetwork && IPAddress.IsLoopback(a) == false);
        using var listener = new TcpListener(IPAddress.Any, 0);
        listener.Start();
        var url = $"postgresql://bench:bench@{address}:{((IPEndPoint)listener.LocalEndpoint).Port}/bench";

        foreach (var local in new[] { false, true })
        {
            var bin = FakeBin(FakeDocker.ComposeFails);
            try
            {
                WriteTool(bin, "hostname", local ? $"echo {address}" : "echo 192.0.2.1");
                var (exitCode, output) = RunBash(Path.Combine(Folder("ycsb"), "run.sh"), ["--target", "postgresql", "--url", url, "--output-prefix", Path.Combine(bin, "out")], Env(bin), clear: true);

                exitCode.Should().NotBe(0, "the fake docker answers no container, so neither path creates the database");
                output.Should().Contain("Create the database on the database host");
                if (local)
                    Calls(bin).Should().Contain("docker ps --filter publish=", "a local host may use the local container");
                else
                    Calls(bin).Should().NotContain("docker ps").And.NotContain("docker exec", "a remote host never gets a local container's database");
            }
            finally
            {
                Directory.Delete(bin, recursive: true);
            }
        }
    }

    [RequiresBashFact]
    public void A_Cross_Check_Run_Names_The_Files_The_Cross_Check_Writes()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var url = $"postgresql://bench:bench@127.0.0.1:{((IPEndPoint)listener.LocalEndpoint).Port}/bench";
        var bin = FakeBin(FakeDocker.Missing);
        try
        {
            WriteTool(bin, "psql", "echo \"psql $*\" >> \"$FAKE_LOG\"\necho 1");
            var prefix = Path.Combine(bin, "out");
            var (exitCode, output) = RunBash(Path.Combine(Folder("ycsb"), "run.sh"), ["--target", "postgresql", "--cross-check", "--url", url, "--output-prefix", prefix], Env(bin), clear: true);

            exitCode.Should().Be(0, output);
            Calls(bin).Should().Contain("dotnet run").And.Contain(" ycsb-crosscheck --target postgresql");
            var results = output.Split('\n').Single(l => l.StartsWith("Results:", StringComparison.Ordinal));
            results.Should().Contain($"{prefix}-<raw|client>-").And.Contain($"{prefix}-crosscheck.json");
        }
        finally
        {
            Directory.Delete(bin, recursive: true);
        }
    }

    [Fact]
    public void The_Postgresql_Services_Are_Healthy_Only_When_A_Query_Over_Tcp_Succeeds()
    {
        foreach (var (bench, service) in new[] { ("ycsb", "postgresql"), ("vector", "pgvector") })
        {
            var compose = File.ReadAllText(Path.Combine(Folder(bench), "docker-compose.yml"));
            var block = Regex.Match(compose, $@"(?ms)^  {service}:\n(.*?)(?=^  \S|^\S)").Groups[1].Value;
            block.Should().MatchRegex(@"healthcheck:\s+test: \[""CMD-SHELL"", "".*psql -h 127\.0\.0\.1 .*-\w*c 'SELECT 1'", $"--wait honours the {bench} {service} healthcheck");
        }
    }

    private static Dictionary<string, string> Env(string bin) =>
        new() { ["PATH"] = bin, ["HOME"] = bin, ["FAKE_LOG"] = Path.Combine(bin, "calls.log"), ["VECTOR_READY_TIMEOUT"] = "1", ["YCSB_READY_TIMEOUT"] = "1", ["AGGREGATE_READY_TIMEOUT"] = "1" };

    private static void WriteTool(string bin, string name, string body)
    {
        var path = Path.Combine(bin, name);
        File.WriteAllText(path, "#!/bin/sh\n" + body + "\n");
        File.SetUnixFileMode(path, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
    }

    private static string Which(string name) =>
        (Environment.GetEnvironmentVariable("PATH") ?? "").Split(':', StringSplitOptions.RemoveEmptyEntries).Select(d => Path.Combine(d, name)).FirstOrDefault(File.Exists)
        ?? throw new InvalidOperationException($"'{name}' was not found on PATH.");
}
