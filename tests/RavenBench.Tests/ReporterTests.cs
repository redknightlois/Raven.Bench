using System.Collections.Generic;
using RavenBench.Core;
using RavenBench.Core.Reporting;
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
            Transport = "raw",
            QueryProfile = QueryProfile.Equality
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
}