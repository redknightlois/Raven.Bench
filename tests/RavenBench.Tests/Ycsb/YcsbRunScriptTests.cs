using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using System.Threading.Tasks;
using FluentAssertions;
using RavenBench.Core.Diagnostics;
using RavenBench.Core.Transport;
using RavenBench.Tests.Infrastructure;
using Xunit;

namespace RavenBench.Tests.Ycsb;

/// <summary>
/// Drives the folder's entry script and pins that the folder carries the one command, the
/// self-contained compose file and the README. The test runs the script, not a copy of its logic.
/// </summary>
public class YcsbRunScriptTests
{
    [Fact]
    public void The_Folder_Has_The_Script_The_Compose_File_And_The_Readme()
    {
        var folder = Folder();
        File.Exists(Path.Combine(folder, "run.sh")).Should().BeTrue();
        File.Exists(Path.Combine(folder, "docker-compose.yml")).Should().BeTrue();
        File.Exists(Path.Combine(folder, "README.md")).Should().BeTrue();
        File.Exists(Path.Combine(folder, "scenario.json")).Should().BeTrue();
    }

    [RequiresBashFact]
    public void The_Script_Is_Executable_And_Documents_The_One_Command()
    {
        var folder = Folder();
        var script = Path.Combine(folder, "run.sh");

        if (OperatingSystem.IsLinux() || OperatingSystem.IsMacOS())
            File.GetUnixFileMode(script).HasFlag(UnixFileMode.UserExecute).Should().BeTrue();

        var readme = File.ReadAllText(Path.Combine(folder, "README.md"));
        readme.Should().Contain("./benchmarks/ycsb/run.sh --target");
    }

    [RequiresBashFact]
    public void An_Unknown_Target_Exits_Non_Zero_Names_The_Value_And_Writes_No_Result()
    {
        var folder = Folder();
        var resultsDirectory = Path.Combine(folder, "results");
        var before = SnapshotFiles(resultsDirectory);

        var (exitCode, output) = RunBash(Path.Combine(folder, "run.sh"), "--target", "cockroachdb");

        exitCode.Should().NotBe(0, "a rejected target never starts a run");
        output.Should().Contain("cockroachdb");
        SnapshotFiles(resultsDirectory).Should().BeEquivalentTo(before, "a rejected target writes no result");
    }

    [RequiresBashFact]
    public void A_Caller_Supplied_Dead_Endpoint_Fails_And_Writes_No_Result()
    {
        var folder = Folder();
        var resultsDirectory = Path.Combine(folder, "results");
        var before = SnapshotFiles(resultsDirectory);

        var (exitCode, output) = RunBash(
            Path.Combine(folder, "run.sh"),
            "--target", "mongodb",
            "--url", "mongodb://localhost:59999",
            "--database", "ycsb_unused");

        exitCode.Should().NotBe(0, "a dead caller endpoint never starts a run");
        output.Should().Contain("59999");
        SnapshotFiles(resultsDirectory).Should().BeEquivalentTo(before, "a failed endpoint probe writes no result");
    }

    [RequiresBashFact]
    public void The_Script_And_Compose_File_Never_Address_Port_8080()
    {
        foreach (var file in new[] { "run.sh", "docker-compose.yml" })
        {
            File.ReadAllText(Path.Combine(Folder(), file)).Should().NotContain("8080", $"'{file}' must never address the other server");
        }
    }

    [Fact]
    public void The_Compose_File_Is_Self_Contained()
    {
        var compose = File.ReadAllText(Path.Combine(Folder(), "docker-compose.yml"));
        compose.Should().NotContain("/shared", "a clean clone has no shared data directory");
        compose.Should().Contain("ycsb-postgresql-data");
        compose.Should().Contain("ycsb-mongodb-data");
        compose.Should().Contain("ycsb-documentdb-data");
    }

    [Fact]
    public void The_Compose_File_Defines_The_Two_Containerized_RavenDb_Services()
    {
        var compose = File.ReadAllText(Path.Combine(Folder(), "docker-compose.yml"));

        compose.Should().Contain("ravendb-6:");
        compose.Should().Contain("ravendb-7:");
        compose.Should().Contain("ravendb/ravendb:6.2-latest", "the 6.x service names the latest 6.x series");
        compose.Should().Contain("ravendb/ravendb:7.2-latest", "the 7.x service names the latest 7.x series");

        // Each service has its own host port, and both derive the server's public URL from the
        // database host's name, whose default is localhost.
        compose.Should().Contain("RAVENDB6_PORT");
        compose.Should().Contain("RAVENDB7_PORT");
        compose.Should().Contain("RAVENDB_HOST");
        compose.Should().Contain("${RAVENDB_HOST:-localhost}", "the public URL defaults the database host to localhost");
    }

    [Fact]
    public void The_Readme_Documents_The_Three_Situations_And_The_Port_Table()
    {
        var readme = File.ReadAllText(Path.Combine(Folder(), "README.md"));

        readme.Should().Contain("Docker is not installed");
        readme.Should().Contain("the daemon is not reachable");
        readme.Should().Contain("The database host is not the client");
        readme.Should().Contain("RAVENDB_HOST");
        readme.Should().Contain("--url");

        foreach (var target in new[] { "ravendb", "ravendb-6", "ravendb-7", "postgresql", "mongodb", "documentdb" })
            readme.Should().Contain($"| `{target}` |", "the port table names every target");
    }

    [RequiresBashFact]
    public void The_Missing_Docker_And_Denied_Daemon_Messages_Differ_And_Name_Their_Fixes()
    {
        var script = Path.Combine(Folder(), "run.sh");

        var missingPath = CreateSymlinkPath("dirname", "date", "mkdir", "sleep", "cat", "head");
        try
        {
            var missingPort = FreeTcpPort();
            var missing = RunBash(
                script,
                new[] { "--target", "ravendb-6" },
                new Dictionary<string, string>
                {
                    ["PATH"] = missingPath,
                    ["HOME"] = RealHome(),
                    ["RAVENDB6_PORT"] = missingPort.ToString(CultureInfo.InvariantCulture)
                },
                clearEnvironment: true);

            missing.ExitCode.Should().NotBe(0, "a missing Docker never starts a run");
            missing.Output.Should().Contain("Docker is not installed");
            missing.Output.Should().Contain("docker compose -f benchmarks/ycsb/docker-compose.yml up -d ravendb-6");
            missing.Output.Should().Contain("--url", "installing Docker is not the only way out");

            var denied = RunBash(
                script,
                new[] { "--target", "ravendb-6" },
                new Dictionary<string, string>
                {
                    ["DOCKER_HOST"] = "unix:///nonexistent-ycsb-docker.sock",
                    ["RAVENDB6_PORT"] = FreeTcpPort().ToString(CultureInfo.InvariantCulture)
                });

            denied.ExitCode.Should().NotBe(0, "a denied daemon never starts a run");
            denied.Output.Should().Contain("daemon is not reachable");
            denied.Output.Should().Contain("docker' group");
            denied.Output.Should().NotContain("Docker is not installed", "the two failures have different fixes");
            denied.Output.Should().NotBe(missing.Output, "missing Docker and a denied daemon are not the same message");
        }
        finally
        {
            Directory.Delete(missingPath, recursive: true);
        }
    }

    [RequiresMongoFact]
    public async Task A_No_Docker_Client_Completes_Against_An_Answering_Endpoint()
    {
        var folder = Folder();
        var resultsDirectory = Path.Combine(folder, "results");
        var before = SnapshotFiles(resultsDirectory);
        var database = "ycsb_nodocker_" + Guid.NewGuid().ToString("N");
        var path = CreateSymlinkPathWithoutDocker();

        try
        {
            var (exitCode, output) = RunBash(
                Path.Combine(folder, "run.sh"),
                new[]
                {
                    "--target", "mongodb",
                    "--url", MongoTestEndpoints.MongoConnectionString,
                    "--database", database,
                    "--doc-count", "10",
                    "--duration", "1s",
                    "--warmup", "0s",
                    "--step", "2..2"
                },
                new Dictionary<string, string> { ["PATH"] = path });

            exitCode.Should().Be(0, $"a client without Docker completes against an endpoint that answers: {output}");

            var written = SnapshotFiles(resultsDirectory).Except(before).Where(file => file.EndsWith(".json", StringComparison.Ordinal)).ToList();
            written.Should().HaveCount(5, "the run writes one result per run");

            var runs = new List<string>();
            foreach (var file in written)
            {
                using var document = JsonDocument.Parse(File.ReadAllText(file));
                var root = document.RootElement;
                root.GetProperty("Ycsb").TryGetProperty("ImageReference", out _).Should().BeFalse("a client with no Docker records no image");
                root.GetProperty("MachineFingerprint").GetProperty("DatabaseInDocker").GetBoolean().Should().BeFalse();
                runs.Add(root.GetProperty("Ycsb").GetProperty("Run").GetString()!);
            }

            runs.Should().BeEquivalentTo(new[] { "load", "C", "A", "B", "insert-stream" });
        }
        finally
        {
            foreach (var file in SnapshotFiles(resultsDirectory).Except(before))
                File.Delete(file);
            Directory.Delete(path, recursive: true);

            using var cleanup = new MongoYcsbTransport(MongoTestEndpoints.MongoConnectionString, database, MongoYcsbTransport.MongoDbTarget);
            await cleanup.Documents.Database.Client.DropDatabaseAsync(database);
        }
    }

    [RequiresPostgreSqlFact]
    public void The_Script_Creates_The_Postgres_Database_With_Psql_First()
    {
        var script = Path.Combine(Folder(), "run.sh");
        var directory = Directory.CreateTempSubdirectory("ycsb-psql-");
        var marker = Path.Combine(directory.FullName, "psql-called");

        try
        {
            var fakePsql = Path.Combine(directory.FullName, "psql");
            File.WriteAllText(fakePsql, $"#!/bin/sh\n: > \"{marker}\"\nexit 1\n");
            if (OperatingSystem.IsLinux() || OperatingSystem.IsMacOS())
                File.SetUnixFileMode(fakePsql, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);

            var (exitCode, output) = RunBash(
                script,
                new[] { "--target", "postgresql", "--url", PostgreSqlTestEndpoints.ConnectionString },
                new Dictionary<string, string>
                {
                    ["PATH"] = directory.FullName + ":" + (Environment.GetEnvironmentVariable("PATH") ?? string.Empty)
                });

            exitCode.Should().NotBe(0, "the fake psql fails the run");
            File.Exists(marker).Should().BeTrue("the client's psql is tried before the local container");
            output.Should().NotContain("neither 'psql'", "psql was present, so the no-psql failure is not the one raised");
        }
        finally
        {
            directory.Delete(recursive: true);
        }
    }

    [RequiresPostgreSqlFact]
    public void Without_Psql_And_Without_Docker_The_Script_Tells_The_User_To_Create_The_Database()
    {
        var script = Path.Combine(Folder(), "run.sh");
        var minimalPath = CreateSymlinkPath("dirname", "date", "mkdir", "sleep", "cat", "head");

        try
        {
            var (exitCode, output) = RunBash(
                script,
                new[] { "--target", "postgresql", "--url", PostgreSqlTestEndpoints.ConnectionString },
                new Dictionary<string, string> { ["PATH"] = minimalPath });

            exitCode.Should().NotBe(0);
            output.Should().Contain("Create the database on the database host");
            output.Should().Contain("--database", "the message names the way out");
        }
        finally
        {
            Directory.Delete(minimalPath, recursive: true);
        }
    }

    private static string Folder() => Path.Combine(RepositoryRootLocator.Find(), "benchmarks", "ycsb");

    private static string RealHome() => Environment.GetEnvironmentVariable("HOME") ?? "/root";

    private static string[] SnapshotFiles(string directory) =>
        Directory.Exists(directory) ? Directory.GetFiles(directory) : Array.Empty<string>();

    private static string FindOnPath(string name)
    {
        foreach (var directory in (Environment.GetEnvironmentVariable("PATH") ?? string.Empty).Split(':', StringSplitOptions.RemoveEmptyEntries))
        {
            var candidate = Path.Combine(directory, name);
            if (File.Exists(candidate))
                return candidate;
        }

        throw new InvalidOperationException($"'{name}' was not found on PATH.");
    }

    // A PATH directory with named utilities and no docker, so a script that reaches for Docker
    // fails the way a client with no Docker does.
    private static string CreateSymlinkPath(params string[] names)
    {
        var directory = Directory.CreateTempSubdirectory("ycsb-path-");
        foreach (var name in names)
            File.CreateSymbolicLink(Path.Combine(directory.FullName, name), FindOnPath(name));
        return directory.FullName;
    }

    // Every executable on the test process's PATH except docker, so the script finds dotnet, git
    // and the shell utilities while `command -v docker` fails.
    private static string CreateSymlinkPathWithoutDocker()
    {
        var directory = Directory.CreateTempSubdirectory("ycsb-nodocker-");
        var seen = new HashSet<string>(StringComparer.Ordinal);

        foreach (var pathDirectory in (Environment.GetEnvironmentVariable("PATH") ?? string.Empty).Split(':', StringSplitOptions.RemoveEmptyEntries))
        {
            if (Directory.Exists(pathDirectory) == false)
                continue;

            foreach (var file in Directory.EnumerateFiles(pathDirectory))
            {
                var name = Path.GetFileName(file);
                if (name == "docker" || seen.Add(name) == false)
                    continue;

                try
                {
                    File.CreateSymbolicLink(Path.Combine(directory.FullName, name), file);
                }
                catch (IOException)
                {
                    // A duplicate name or an unsupported target is not needed by this run.
                }
            }
        }

        return directory.FullName;
    }

    private static int FreeTcpPort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private static (int ExitCode, string Output) RunBash(string script, params string[] arguments) =>
        RunBash(script, arguments, environment: null, clearEnvironment: false);

    private static (int ExitCode, string Output) RunBash(
        string script,
        string[] arguments,
        IReadOnlyDictionary<string, string>? environment,
        bool clearEnvironment = false)
    {
        var startInfo = new ProcessStartInfo("/bin/bash")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true
        };
        startInfo.ArgumentList.Add(script);
        foreach (var argument in arguments)
            startInfo.ArgumentList.Add(argument);

        if (clearEnvironment)
            startInfo.Environment.Clear();
        if (environment != null)
        {
            foreach (var (key, value) in environment)
                startInfo.Environment[key] = value;
        }

        using var process = Process.Start(startInfo)!;
        var standardOutput = process.StandardOutput.ReadToEnd();
        var standardError = process.StandardError.ReadToEnd();
        process.WaitForExit();

        return (process.ExitCode, standardOutput + standardError);
    }
}
