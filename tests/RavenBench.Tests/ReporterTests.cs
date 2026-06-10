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
            Profile = WorkloadProfile.Reads,
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
            Profile = WorkloadProfile.Reads
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
            Profile = WorkloadProfile.Reads
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
                Profile = WorkloadProfile.Reads,
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
