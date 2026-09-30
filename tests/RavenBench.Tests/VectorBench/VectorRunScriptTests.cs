using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using RavenBench.Core.Diagnostics;
using RavenBench.Tests.Infrastructure;
using Xunit;

namespace RavenBench.Tests.VectorBench;

/// <summary>Drives the vector folder's entry script and holds it to the ycsb script's endpoint and Docker behaviour.</summary>
[Collection(LiveServers.Name)]
public class VectorRunScriptTests
{
    [Fact]
    public void The_Folder_Has_The_Script_The_Compose_File_The_Scenario_And_The_Readme()
    {
        foreach (var file in new[] { "run.sh", "docker-compose.yml", "README.md", "scenario.json" })
            File.Exists(Path.Combine(Folder("vector"), file)).Should().BeTrue(file);
    }

    [Fact]
    public void The_Compose_File_Pins_Both_Images_And_Carries_No_Tuning()
    {
        var compose = File.ReadAllText(Path.Combine(Folder("vector"), "docker-compose.yml"));
        compose.Should().MatchRegex(@"image: pgvector/pgvector:0\.8\.\d+-pg17\s");
        compose.Should().MatchRegex(@"image: ravendb/ravendb:7\.\d+\.\d+\s", "a release tag, not a floating one");
        compose.Should().NotContain("latest");
        foreach (var setting in new[] { "shared_buffers", "maintenance_work_mem", "max_parallel_maintenance_workers" })
            compose.Should().NotContain(setting + "=", "round one runs the shipped defaults");
        compose.Should().NotContain("8080").And.NotContain("8081");
    }

    [Fact]
    public void The_Readme_Documents_The_Command_The_Rows_And_The_Two_Host_Recipe()
    {
        var readme = File.ReadAllText(Path.Combine(Folder("vector"), "README.md"));
        readme.Should().Contain("./benchmarks/vector/run.sh --target");
        foreach (var text in new[] { "Docker is not installed", "the daemon is not reachable", "The database host is not the client", "node_exporter", "Adding a dataset", "Adding a target" })
            readme.Should().Contain(text);
        foreach (var run in new[] { "`load`", "`recall`", "`readers`", "`filtered`", "`under-insert`" })
            readme.Should().Contain(run);
    }

    [RequiresBashFact]
    public void An_Unknown_Target_Exits_Non_Zero_And_Names_The_Value()
    {
        var (exitCode, output) = RunBash(Path.Combine(Folder("vector"), "run.sh"), ["--target", "postgresql"], null);
        exitCode.Should().NotBe(0);
        output.Should().Contain("'postgresql'").And.Contain("ravendb, ravendb-7, pgvector");
    }

    [RequiresBashFact]
    public void The_Three_Docker_Messages_Match_The_Ycsb_Script_But_For_The_Folder()
    {
        var port = FreeTcpPort().ToString(CultureInfo.InvariantCulture);
        foreach (var docker in new[] { FakeDocker.Missing, FakeDocker.NoDaemon, FakeDocker.ComposeFails })
        {
            var bin = FakeBin(docker);
            try
            {
                var env = new Dictionary<string, string> { ["PATH"] = bin, ["HOME"] = bin, ["FAKE_LOG"] = Path.Combine(bin, "calls.log"), ["RAVENDB7_PORT"] = port, ["VECTOR_READY_TIMEOUT"] = "1", ["YCSB_READY_TIMEOUT"] = "1" };
                var vector = RunBash(Path.Combine(Folder("vector"), "run.sh"), ["--target", "ravendb-7"], env, clear: true);
                var ycsb = RunBash(Path.Combine(Folder("ycsb"), "run.sh"), ["--target", "ravendb-7"], env, clear: true);
                vector.ExitCode.Should().NotBe(0, docker.ToString());
                vector.Output.Should().Contain("error:", docker.ToString());
                vector.Output.Replace("benchmarks/vector", "benchmarks/<folder>").Should().Be(ycsb.Output.Replace("benchmarks/ycsb", "benchmarks/<folder>"), docker.ToString());
                if (docker == FakeDocker.ComposeFails)
                    Calls(bin).Should().Contain("docker compose -f").And.Contain(" up -d ").And.Contain("ravendb-7");
            }
            finally
            {
                Directory.Delete(bin, recursive: true);
            }
        }
    }

    [RequiresBashFact]
    public void A_Caller_Url_That_Answers_Makes_No_Docker_Call_And_Is_Forwarded_As_Given()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        var bin = FakeBin(FakeDocker.ComposeFails);
        try
        {
            var url = $"postgresql://bench:bench@127.0.0.1:{port}/bench";
            var (exitCode, output) = RunBash(Path.Combine(Folder("vector"), "run.sh"), ["--target", "pgvector", "--url", url, "--output-prefix", Path.Combine(bin, "out")],
                new Dictionary<string, string> { ["PATH"] = bin, ["HOME"] = bin, ["FAKE_LOG"] = Path.Combine(bin, "calls.log") }, clear: true);

            exitCode.Should().Be(0, output);
            Calls(bin).Should().NotContain("docker", "an answering caller endpoint needs no Docker");
            var forwarded = Calls(bin).Split('\n').Single(l => l.StartsWith("dotnet "));
            forwarded.Should().Contain($"--url {url}").And.Contain("vector --target pgvector").And.Contain("--database vector_pgvector_");
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
            RunBash(Path.Combine(Folder("vector"), "run.sh"), ["--target", "ravendb", "--output-prefix", Path.Combine(bin, "out")],
                new Dictionary<string, string> { ["PATH"] = bin, ["HOME"] = bin, ["FAKE_LOG"] = Path.Combine(bin, "calls.log") }, clear: true);
            Calls(bin).Should().NotContain("docker", "the external server on 8081 is never started by the script, whether it answers or not");
        }
        finally
        {
            Directory.Delete(bin, recursive: true);
        }
    }

    [RequiresBashFact]
    public void An_Elasticsearch_Run_Refuses_A_Port_That_Already_Answers()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port.ToString(CultureInfo.InvariantCulture);
        var bin = FakeBin(FakeDocker.ComposeFails);
        try
        {
            var env = new Dictionary<string, string> { ["PATH"] = bin, ["HOME"] = bin, ["FAKE_LOG"] = Path.Combine(bin, "calls.log"), ["ELASTICSEARCH_PORT"] = port };
            var answering = RunBash(Path.Combine(Folder("vector"), "run.sh"), ["--target", "elasticsearch"], env, clear: true);
            answering.ExitCode.Should().NotBe(0, "a fresh cluster is the default, so an answering port is not reused");
            answering.Output.Should().Contain("ELASTICSEARCH_PORT").And.Contain("fresh elasticsearch container");
            Calls(bin).Should().NotContain("dotnet", "the refusal comes before the harness runs");
        }
        finally
        {
            Directory.Delete(bin, recursive: true);
        }
    }

    private enum FakeDocker
    {
        Missing,
        NoDaemon,
        ComposeFails
    }

    // The tools the scripts need, a dotnet that records its call, and a docker per situation:
    // absent, failing every call, or answering `docker info` and failing everything else.
    private static string FakeBin(FakeDocker docker)
    {
        var directory = Directory.CreateTempSubdirectory("vector-fakes-").FullName;
        foreach (var tool in new[] { "dirname", "date", "mkdir", "sleep", "cat", "head" })
            File.CreateSymbolicLink(Path.Combine(directory, tool), Which(tool));
        Directory.CreateDirectory(Path.Combine(directory, ".dotnet"));
        Fake(Path.Combine(directory, ".dotnet"), "dotnet", "echo \"dotnet $*\" >> \"$FAKE_LOG\"\nexit 0");
        if (docker == FakeDocker.NoDaemon)
            Fake(directory, "docker", "echo \"docker $*\" >> \"$FAKE_LOG\"\nexit 1");
        if (docker == FakeDocker.ComposeFails)
            Fake(directory, "docker", "echo \"docker $*\" >> \"$FAKE_LOG\"\n[ \"$1\" = info ] && exit 0\nexit 1");
        return directory;
    }

    private static string Calls(string bin) => File.Exists(Path.Combine(bin, "calls.log")) ? File.ReadAllText(Path.Combine(bin, "calls.log")) : "";

    private static string Which(string name) =>
        (Environment.GetEnvironmentVariable("PATH") ?? "").Split(':', StringSplitOptions.RemoveEmptyEntries).Select(d => Path.Combine(d, name)).FirstOrDefault(File.Exists)
        ?? throw new InvalidOperationException($"'{name}' was not found on PATH.");

    private static void Fake(string directory, string name, string body)
    {
        var path = Path.Combine(directory, name);
        File.WriteAllText(path, "#!/bin/sh\n" + body + "\n");
        File.SetUnixFileMode(path, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
    }

    private static string Folder(string name) => Path.Combine(RepositoryRootLocator.Find(), "benchmarks", name);

    private static int FreeTcpPort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private static (int ExitCode, string Output) RunBash(string script, string[] arguments, IReadOnlyDictionary<string, string>? environment, bool clear = false)
    {
        var startInfo = new ProcessStartInfo("/bin/bash") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        startInfo.ArgumentList.Add(script);
        foreach (var argument in arguments)
            startInfo.ArgumentList.Add(argument);
        if (clear)
            startInfo.Environment.Clear();
        foreach (var (key, value) in environment ?? new Dictionary<string, string>())
            startInfo.Environment[key] = value;

        using var process = Process.Start(startInfo)!;
        var stdout = process.StandardOutput.ReadToEndAsync();
        var stderr = process.StandardError.ReadToEnd();
        process.WaitForExit();
        return (process.ExitCode, stdout.Result + stderr);
    }
}
