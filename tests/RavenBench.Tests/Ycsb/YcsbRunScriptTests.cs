using System;
using System.Diagnostics;
using System.IO;
using FluentAssertions;
using RavenBench.Core.Diagnostics;
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

    private static string Folder() => Path.Combine(RepositoryRootLocator.Find(), "benchmarks", "ycsb");

    private static string[] SnapshotFiles(string directory) =>
        Directory.Exists(directory) ? Directory.GetFiles(directory) : Array.Empty<string>();

    private static (int ExitCode, string Output) RunBash(string script, params string[] arguments)
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

        using var process = Process.Start(startInfo)!;
        var standardOutput = process.StandardOutput.ReadToEnd();
        var standardError = process.StandardError.ReadToEnd();
        process.WaitForExit();

        return (process.ExitCode, standardOutput + standardError);
    }
}
