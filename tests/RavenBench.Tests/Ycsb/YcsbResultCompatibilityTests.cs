using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using FluentAssertions;
using RavenBench.Core;
using RavenBench.Core.Reporting;
using RavenBench.Core.Ycsb;
using RavenBench.Reporter;
using RavenBench.Reporting;
using Xunit;

namespace RavenBench.Tests.Ycsb;

/// <summary>
/// A ycsb result is still a Raven.Bench summary: the schema version does not change and the
/// existing reporter loads it back, Ycsb block included.
/// </summary>
public class YcsbResultCompatibilityTests
{
    [Fact]
    public async Task Ycsb_Result_Round_Trips_Through_The_Existing_Summary_Loader()
    {
        var scenario = new YcsbScenario
        {
            Seed = 7,
            Target = "ravendb",
            DocumentCount = 1000,
            DocumentSize = "1KB",
            Concurrency = "8..64x2",
            Distribution = "uniform",
            Warmup = "1s",
            Duration = "1s"
        };

        var summary = new BenchmarkSummary
        {
            Options = new RunOptions { Url = "http://localhost:8081", Database = "ycsb", Profile = WorkloadProfile.Mixed },
            Steps = new List<StepResult>(),
            Verdict = "ycsb",
            ClientCompression = "identity",
            EffectiveHttpVersion = "1.1",
            MachineFingerprint = new MachineFingerprint
            {
                CpuModel = "Test CPU 9000",
                PhysicalCoreCount = 4,
                LogicalCoreCount = 8,
                SmT = true,
                RamBytes = 16L * 1024 * 1024 * 1024,
                Os = "Test OS 1.0",
                Kernel = "1.2.3-test",
                StorageDevice = "/dev/test0",
                Filesystem = "testfs",
                DotNetVersion = "10.0.0-test",
                HarnessCommit = "0123456789abcdef0123456789abcdef01234567",
                DatabaseInDocker = true,
                DatabaseImage = "mongo:8.0"
            },
            Ycsb = new YcsbRunInfo
            {
                Run = "C",
                ResolvedScenario = scenario,
                ProductName = "MongoDB Community",
                ServerVersion = "8.0.30",
                Durability = new DurabilityParity { Setting = "writeConcern", Value = "j=true" },
                ImageReference = "mongo:8.0",
                ImageDigest = "mongo:8.0@sha256:4a0f30875898413139bec44c73c02a05fed172578de65b644dcdcee143ae7306"
            }
        };

        var path = Path.Combine(Path.GetTempPath(), $"ycsb-result-{Guid.NewGuid():N}.json");
        JsonResultsWriter.Write(path, summary);

        var loaded = await SummaryLoader.LoadAsync(path);

        loaded.SchemaVersion.Should().Be(SummaryLoader.ExpectedSchemaVersion);
        loaded.Ycsb.Should().NotBeNull();
        loaded.Ycsb!.Run.Should().Be("C");
        loaded.Ycsb.ResolvedScenario.Should().Be(scenario);
        loaded.Ycsb.ImageReference.Should().Be("mongo:8.0");
        loaded.Ycsb.ImageDigest.Should().Contain("@sha256:", "the recorded digest is the image's repo digest, not only its hash");
        loaded.MachineFingerprint.Should().NotBeNull();
        loaded.MachineFingerprint!.CpuModel.Should().Be("Test CPU 9000");
        loaded.MachineFingerprint.SmT.Should().BeTrue();
        loaded.MachineFingerprint.DatabaseInDocker.Should().BeTrue();
        loaded.MachineFingerprint.DatabaseImage.Should().Be("mongo:8.0");
    }

    [Fact]
    public void A_Result_From_Another_Command_Carries_No_Ycsb_Block()
    {
        var summary = new BenchmarkSummary
        {
            Options = new RunOptions { Url = "http://localhost:8081", Database = "x", Profile = WorkloadProfile.Mixed },
            Steps = new List<StepResult>(),
            Verdict = "ok",
            ClientCompression = "identity",
            EffectiveHttpVersion = "1.1"
        };

        var path = Path.Combine(Path.GetTempPath(), $"plain-result-{Guid.NewGuid():N}.json");
        JsonResultsWriter.Write(path, summary);

        File.ReadAllText(path).Should().NotContain("\"Ycsb\"");
        File.ReadAllText(path).Should().NotContain("\"MachineFingerprint\"");
    }
}
