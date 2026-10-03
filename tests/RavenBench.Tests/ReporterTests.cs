using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using System.Threading.Tasks;
using RavenBench.Core;
using RavenBench.Core.Reporting;
using RavenBench.Reporter;
using RavenBench.Reporter.Commands;
using RavenBench.Reporting;
using Xunit;

namespace RavenBench.Tests;

public class ReporterTests
{
    [Fact]
    public void RunCompatibilityChecker_AreComparable_SameOptions_ReturnsTrue()
    {
        var options = new RunOptions
        {
            Url = "http://localhost:8080",
            Database = "test",
            Profile = WorkloadProfile.QueryById,
            Dataset = "stackoverflow",
            Transport = TransportKind.Raw,
            QueryProfile = QueryProfile.VoronEquality
        };

        var summary1 = new BenchmarkSummary
        {
            Options = options,
            EffectiveHttpVersion = "1.1",
            Steps = new List<StepResult>(),
            Verdict = "Passed",
            ClientCompression = "identity"
        };

        var summary2 = new BenchmarkSummary
        {
            Options = options,
            EffectiveHttpVersion = "1.1",
            Steps = new List<StepResult>(),
            Verdict = "Passed",
            ClientCompression = "identity"
        };

        Assert.True(RunCompatibilityChecker.AreComparable(summary1, summary2));
    }

    [Fact]
    public void RunCompatibilityChecker_AreComparable_DifferentProfile_ReturnsFalse()
    {
        var options1 = new RunOptions
        {
            Url = "http://localhost:8080",
            Database = "test",
            Profile = WorkloadProfile.QueryById
        };
        var options2 = new RunOptions
        {
            Url = "http://localhost:8080",
            Database = "test",
            Profile = WorkloadProfile.Writes
        };

        var summary1 = new BenchmarkSummary
        {
            Options = options1,
            EffectiveHttpVersion = "1.1",
            Steps = new List<StepResult>(),
            Verdict = "Passed",
            ClientCompression = "identity"
        };
        var summary2 = new BenchmarkSummary
        {
            Options = options2,
            EffectiveHttpVersion = "1.1",
            Steps = new List<StepResult>(),
            Verdict = "Passed",
            ClientCompression = "identity"
        };

        Assert.False(RunCompatibilityChecker.AreComparable(summary1, summary2));
    }

    [Fact]
    public void RunCompatibilityChecker_EnsureComparable_Incompatible_Throws()
    {
        var options1 = new RunOptions
        {
            Url = "http://localhost:8080",
            Database = "test",
            Profile = WorkloadProfile.QueryById
        };
        var options2 = new RunOptions
        {
            Url = "http://localhost:8080",
            Database = "test",
            Profile = WorkloadProfile.Writes
        };

        var summary1 = new BenchmarkSummary
        {
            Options = options1,
            EffectiveHttpVersion = "1.1",
            Steps = new List<StepResult>(),
            Verdict = "Passed",
            ClientCompression = "identity"
        };
        var summary2 = new BenchmarkSummary
        {
            Options = options2,
            EffectiveHttpVersion = "1.1",
            Steps = new List<StepResult>(),
            Verdict = "Passed",
            ClientCompression = "identity"
        };

        Assert.Throws<System.InvalidOperationException>(() => RunCompatibilityChecker.EnsureComparable(summary1, summary2));
    }

    public static TheoryData<string, RunOptions> WorkloadDifferences()
    {
        var baseline = new RunOptions { Url = "http://localhost:8080", Database = "test" };
        return new TheoryData<string, RunOptions>
        {
            { "load shape", baseline with { Shape = LoadShape.Rate } },
            { "key distribution", baseline with { Distribution = KeyDistributionKind.Zipfian } },
            { "dataset profile", baseline with { DatasetProfile = "half" } },
            { "dataset size", baseline with { DatasetSize = 3 } },
        };
    }

    [Theory]
    [MemberData(nameof(WorkloadDifferences))]
    public void RunCompatibilityChecker_A_Different_Workload_Field_Is_Not_Comparable_And_Is_Named(string field, RunOptions other)
    {
        BenchmarkSummary Summary(RunOptions options) => new()
        {
            Options = options,
            EffectiveHttpVersion = "1.1",
            Steps = new List<StepResult>(),
            Verdict = "Passed",
            ClientCompression = "identity"
        };
        var baseline = Summary(new RunOptions { Url = "http://localhost:8080", Database = "test" });

        Assert.False(RunCompatibilityChecker.AreComparable(baseline, Summary(other)));
        var error = Assert.Throws<InvalidOperationException>(() => RunCompatibilityChecker.EnsureComparable(baseline, Summary(other)));
        Assert.Contains($"different {field}:", error.Message);
    }

    private static readonly RavenBench.Core.Ycsb.YcsbScenario Scenario = new() { Seed = 7, Target = "ravendb", DocumentCount = 1000, DocumentSize = "1KB", Concurrency = "8", Distribution = "uniform", Warmup = "1s", Duration = "1s" };
    private static readonly MachineFingerprint Machine = new() { CpuModel = "cpu", PhysicalCoreCount = 4, LogicalCoreCount = 8, SmT = true, RamBytes = 1, Os = "os", Kernel = "k", StorageDevice = "d", Filesystem = "fs", DotNetVersion = "10", HarnessCommit = "a", DatabaseInDocker = false };

    private static BenchmarkSummary YcsbSummary(string run, RavenBench.Core.Ycsb.YcsbScenario scenario, MachineFingerprint machine, TransportKind transport = TransportKind.Raw) => new()
    {
        Options = new RunOptions { Url = "http://localhost:8080", Database = "test", Transport = transport },
        EffectiveHttpVersion = "1.1",
        Steps = new List<StepResult>(),
        Verdict = "ycsb",
        ClientCompression = "identity",
        MachineFingerprint = machine,
        Ycsb = new YcsbRunInfo { Run = run, Shape = "closed", Distribution = "uniform", Repetition = 1, IsRowMedian = true, MedianStatistic = "p50", ResolvedScenario = scenario, ProductName = "p", ServerVersion = "v", Durability = new DurabilityParity { Setting = "s", Value = "v" }, ServerColumns = ServerColumnAvailability.FromSteps("p", Array.Empty<StepResult>()) }
    };

    [Fact]
    public void RunCompatibilityChecker_Refuses_A_Different_Workload_Scenario_Or_Entry_Point()
    {
        var a = YcsbSummary("A", Scenario, Machine);

        Assert.True(RunCompatibilityChecker.AreComparable(a, YcsbSummary("A", Scenario, Machine, TransportKind.Client)));
        Assert.Contains("different run:", Assert.Throws<InvalidOperationException>(() => RunCompatibilityChecker.EnsureComparable(a, YcsbSummary("C", Scenario, Machine))).Message);
        Assert.Contains("different scenario:", Assert.Throws<InvalidOperationException>(() => RunCompatibilityChecker.EnsureComparable(a, YcsbSummary("A", Scenario with { DocumentCount = 2000 }, Machine))).Message);
        var plain = new BenchmarkSummary { Options = a.Options, EffectiveHttpVersion = "1.1", Steps = new List<StepResult>(), Verdict = "x", ClientCompression = "identity", MachineFingerprint = Machine };
        Assert.Contains("different entry point:", Assert.Throws<InvalidOperationException>(() => RunCompatibilityChecker.EnsureComparable(a, plain)).Message);
    }

    [Fact]
    public void RunCompatibilityChecker_Refuses_A_Different_Machine_But_Not_A_Different_Harness_Commit()
    {
        var a = YcsbSummary("A", Scenario, Machine);

        Assert.True(RunCompatibilityChecker.AreComparable(a, YcsbSummary("A", Scenario, Machine with { HarnessCommit = "b" })));
        Assert.Contains("different machine fingerprint:", Assert.Throws<InvalidOperationException>(() => RunCompatibilityChecker.EnsureComparable(a, YcsbSummary("A", Scenario, Machine with { CpuModel = "other" }))).Message);
    }

    [Fact]
    public async Task SummaryLoader_RoundTripsJsonResultsWriterOutput()
    {
        var summary = CreateSummary();
        string path = Path.Combine(Path.GetTempPath(), $"ravenbench-summary-{Guid.NewGuid():N}.json");
        try
        {
            JsonResultsWriter.Write(path, summary);

            var loaded = await SummaryLoader.LoadAsync(path);

            Assert.Equal(1, loaded.SchemaVersion);
            Assert.Equal(summary.Verdict, loaded.Verdict);
            Assert.Equal(summary.Options.Database, loaded.Options.Database);
            Assert.Equal(summary.Steps.Count, loaded.Steps.Count);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task SummaryLoader_MissingSchemaVersion_Throws()
    {
        string path = Path.Combine(Path.GetTempPath(), $"ravenbench-summary-{Guid.NewGuid():N}.json");
        try
        {
            await File.WriteAllTextAsync(path, "{ \"verdict\": \"Passed\" }");

            await Assert.ThrowsAsync<InvalidDataException>(() => SummaryLoader.LoadAsync(path));
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task SummaryLoader_UnsupportedSchemaVersion_Throws()
    {
        string path = Path.Combine(Path.GetTempPath(), $"ravenbench-summary-{Guid.NewGuid():N}.json");
        try
        {
            await File.WriteAllTextAsync(path, "{ \"schemaVersion\": 99, \"verdict\": \"Passed\" }");

            var exception = await Assert.ThrowsAsync<InvalidDataException>(() => SummaryLoader.LoadAsync(path));
            Assert.Contains("99", exception.Message);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task SummaryLoader_MalformedJson_Throws()
    {
        string path = Path.Combine(Path.GetTempPath(), $"ravenbench-summary-{Guid.NewGuid():N}.json");
        try
        {
            await File.WriteAllTextAsync(path, "{ not valid json");

            await Assert.ThrowsAnyAsync<JsonException>(() => SummaryLoader.LoadAsync(path));
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void TemplateHtmlBuilder_SubstitutesPayloadAndContext()
    {
        var summary = CreateSummary();

        string html = TemplateHtmlBuilder.Build("single-run.html", "__SUMMARY_JSON__", summary, "My Title", "My Notes");

        Assert.DoesNotContain("__SUMMARY_JSON__", html);
        Assert.DoesNotContain("__REPORT_CONTEXT__", html);
        Assert.Contains("My Title", html);
        Assert.Contains("My Notes", html);
        Assert.Contains("\"schemaVersion\":1", html);
    }

    [Fact]
    public void TemplateHtmlBuilder_EscapesScriptCloseTagInPayload()
    {
        var summary = CreateSummary(notes: "</script><script>alert(1)</script>");

        string html = TemplateHtmlBuilder.Build("single-run.html", "__SUMMARY_JSON__", summary, null, null);

        Assert.DoesNotContain("</script><script>alert(1)", html);
        Assert.Contains("<\\/script><script>alert(1)<\\/script>", html);
    }

    private static BenchmarkSummary CreateSummary(string? notes = null)
    {
        return new BenchmarkSummary
        {
            Options = new RunOptions
            {
                Url = "http://localhost:8080",
                Database = "test",
                Profile = WorkloadProfile.QueryById,
                Dataset = "test",
                Transport = TransportKind.Raw,
                QueryProfile = QueryProfile.VoronEquality
            },
            EffectiveHttpVersion = "1.1",
            Steps = new List<StepResult>
            {
                new StepResult
                {
                    Concurrency = 16,
                    Throughput = 1000,
                    Raw = new Percentiles(1, 1, 1, 1, 10, 12),
                    ErrorRate = 0.01
                }
            },
            Verdict = "Passed",
            ClientCompression = "identity",
            Notes = notes
        };
    }
}
