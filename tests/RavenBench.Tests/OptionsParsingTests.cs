using System;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using RavenBench.Cli;
using RavenBench.Core;
using RavenBench.Core.Metrics;
using RavenBench.Core.Workload;
using Xunit;

namespace RavenBench.Tests;

public class CliParsingTests
{
    [Fact]
    public void Converts_Settings_To_RunOptions_With_Concurrency_Range()
    {
        var settings = new ClosedSettings
        {
            Url = "http://localhost:10101",
            Database = "ycsb",
            Concurrency = "16..256x1.5",
            KneeRule = "dthr=3%,dp95=30%",
            MaxErrors = "1%",
            Profile = "mixed"
        };

        var opts = settings.ToRunOptions();

        opts.KneeThroughputDelta.Should().BeApproximately(0.03, 1e-9);
        opts.KneeP95Delta.Should().BeApproximately(0.30, 1e-9);
        opts.MaxErrorRate.Should().BeApproximately(0.01, 1e-9);
        opts.Shape.Should().Be(LoadShape.Closed);
        opts.Step.Start.Should().Be(16);
        opts.Step.End.Should().Be(256);
        opts.Step.Factor.Should().Be(1.5);
    }

    [Fact]
    public void Converts_Weight_Based_Mix_Flags()
    {
        var settings = new ClosedSettings
        {
            Url = "http://localhost:10101",
            Database = "ycsb",
            Reads = "3",
            Writes = "1",
            Updates = "0",
            Profile = "mixed"
        };

        var opts = settings.ToRunOptions();

        opts.Reads.Should().Be(3);
        opts.Writes.Should().Be(1);
        opts.Updates.Should().Be(0);
    }

    [Fact]
    public void Parses_Query_Profile_Defaults_To_VoronEquality()
    {
        var settings = new ClosedSettings
        {
            Url = "http://localhost:8080",
            Database = "test",
            Profile = "stackoverflow-text-search"
        };

        var opts = settings.ToRunOptions();

        opts.QueryProfile.Should().Be(QueryProfile.VoronEquality);
    }

    [Fact]
    public void Parses_Query_Profile_VoronEquality()
    {
        var settings = new ClosedSettings
        {
            Url = "http://localhost:8080",
            Database = "test",
            Profile = "stackoverflow-text-search",
            QueryProfile = "voron-equality"
        };

        var opts = settings.ToRunOptions();

        opts.QueryProfile.Should().Be(QueryProfile.VoronEquality);
    }

    [Fact]
    public void Parses_Query_Profile_IndexEquality()
    {
        var settings = new ClosedSettings
        {
            Url = "http://localhost:8080",
            Database = "test",
            Profile = "stackoverflow-text-search",
            QueryProfile = "index-equality"
        };

        var opts = settings.ToRunOptions();

        opts.QueryProfile.Should().Be(QueryProfile.IndexEquality);
    }

    [Fact]
    public void Parses_Query_Profile_TextSearch()
    {
        var settings = new ClosedSettings
        {
            Url = "http://localhost:8080",
            Database = "test",
            Profile = "stackoverflow-text-search",
            QueryProfile = "text-search"
        };

        var opts = settings.ToRunOptions();

        opts.QueryProfile.Should().Be(QueryProfile.TextSearch);
    }

    [Fact]
    public void Parses_Query_Profile_TextSearchRare()
    {
        var settings = new ClosedSettings
        {
            Url = "http://localhost:8080",
            Database = "test",
            Profile = "stackoverflow-text-search",
            QueryProfile = "text-search-rare"
        };

        var opts = settings.ToRunOptions();

        opts.QueryProfile.Should().Be(QueryProfile.TextSearchRare);
    }

    [Fact]
    public void Parses_Query_Profile_TextSearchCommon()
    {
        var settings = new ClosedSettings
        {
            Url = "http://localhost:8080",
            Database = "test",
            Profile = "stackoverflow-text-search",
            QueryProfile = "text-search-common"
        };

        var opts = settings.ToRunOptions();

        opts.QueryProfile.Should().Be(QueryProfile.TextSearchCommon);
    }

    [Fact]
    public void Parses_Query_Profile_TextSearchMixed()
    {
        var settings = new ClosedSettings
        {
            Url = "http://localhost:8080",
            Database = "test",
            Profile = "stackoverflow-text-search",
            QueryProfile = "text-search-mixed"
        };

        var opts = settings.ToRunOptions();

        opts.QueryProfile.Should().Be(QueryProfile.TextSearchMixed);
    }

    [Fact]
    public void Parses_Query_Profile_Range()
    {
        var settings = new ClosedSettings
        {
            Url = "http://localhost:8080",
            Database = "test",
            Profile = "query-users-by-name",
            QueryProfile = "range"
        };

        var opts = settings.ToRunOptions();

        opts.QueryProfile.Should().Be(QueryProfile.Range);
    }

    [Fact]
    public void Throws_On_Invalid_Query_Profile()
    {
        var settings = new ClosedSettings
        {
            Url = "http://localhost:8080",
            Database = "test",
            Profile = "stackoverflow-text-search",
            QueryProfile = "invalid"
        };

        var act = () => settings.ToRunOptions();

        act.Should().Throw<ArgumentException>()
            .WithMessage("Invalid query profile: invalid. Valid options: voron-equality, index-equality, range, text-prefix, text-search, text-search-rare, text-search-common, text-search-mixed");
    }

    [Fact]
    public void Parses_Step_Plan_With_Factor()
    {
        var stepPlan = CliParsing.ParseStepPlan("8..512x2");
        
        stepPlan.Start.Should().Be(8);
        stepPlan.End.Should().Be(512);
        stepPlan.Factor.Should().Be(2.0);
    }

    [Fact]
    public void Parses_Step_Plan_Without_Factor()
    {
        var stepPlan = CliParsing.ParseStepPlan("16..256");
        
        stepPlan.Start.Should().Be(16);
        stepPlan.End.Should().Be(256);
        stepPlan.Factor.Should().Be(2.0); // Default factor
    }

    [Fact]
    public void ClosedSettings_Uses_Step_Parameter()
    {
        var settings = new ClosedSettings
        {
            Url = "http://localhost:10101",
            Database = "ycsb",
            Step = "32..1024x1.5",
            Profile = "mixed"
        };

        var opts = settings.ToRunOptions();

        opts.Step.Start.Should().Be(32);
        opts.Step.End.Should().Be(1024);
        opts.Step.Factor.Should().Be(1.5);
        opts.Shape.Should().Be(LoadShape.Closed);
    }

    [Fact]
    public void RateSettings_RoundTrip_Parsing()
    {
        var settings = new RateSettings
        {
            Url = "http://localhost:10101",
            Database = "ycsb",
            Step = "200..20000x1.5",
            Profile = "mixed"
        };

        var opts = settings.ToRunOptions();

        opts.Shape.Should().Be(LoadShape.Rate);
        opts.Step.Start.Should().Be(200);
        opts.Step.End.Should().Be(20000);
        opts.Step.Factor.Should().Be(1.5);
    }

    [Fact]
    public async Task BenchmarkExecutor_Calls_Warmup_When_Configured()
    {
        // Regression test: ensure warm-up is executed when Warmup > 0
        var mockLoadGenerator = new MockLoadGenerator();
        var options = new RunOptions
        {
            Url = "http://localhost:10101",
            Database = "test",
            Profile = WorkloadProfile.Writes,
            Warmup = TimeSpan.FromMilliseconds(100),
            Duration = TimeSpan.FromMilliseconds(50)
        };

        var executor = new BenchmarkExecutor(options, new TestTransport(), new MockWorkload(), new ProcessCpuTracker());

        // Execute a step - this should call warmup
        await executor.ExecuteStepAsync(mockLoadGenerator, 0, 1, CancellationToken.None);

        // Verify that warmup was called
        mockLoadGenerator.WarmupCalled.Should().BeTrue("Warm-up should be executed when Warmup > 0");
        mockLoadGenerator.MeasurementCalled.Should().BeTrue("Measurement should also be executed");
    }

    [Fact]
    public async Task BenchmarkExecutor_Skips_Warmup_When_Zero()
    {
        // Ensure warm-up is skipped when Warmup = 0
        var mockLoadGenerator = new MockLoadGenerator();
        var options = new RunOptions
        {
            Url = "http://localhost:10101",
            Database = "test",
            Profile = WorkloadProfile.Writes,
            Warmup = TimeSpan.Zero,
            Duration = TimeSpan.FromMilliseconds(50)
        };

        var executor = new BenchmarkExecutor(options, new TestTransport(), new MockWorkload(), new ProcessCpuTracker());

        // Execute a step - this should skip warmup
        await executor.ExecuteStepAsync(mockLoadGenerator, 0, 1, CancellationToken.None);

        // Verify that warmup was NOT called
        mockLoadGenerator.WarmupCalled.Should().BeFalse("Warm-up should be skipped when Warmup = 0");
        mockLoadGenerator.MeasurementCalled.Should().BeTrue("Measurement should still be executed");
    }

    private sealed class MockLoadGenerator : ILoadGenerator
    {
        public int Concurrency => 1;
        public double? TargetThroughput => null;
        public bool WarmupCalled { get; private set; }
        public bool MeasurementCalled { get; private set; }

        public Task<WarmupDiagnostics> ExecuteWarmupAsync(TimeSpan duration, CancellationToken cancellationToken)
        {
            WarmupCalled = true;
            return Task.FromResult(WarmupDiagnostics.Empty);
        }

        public Task<(LatencyRecorder latencyRecorder, LoadGeneratorMetrics metrics)> ExecuteMeasurementAsync(
            TimeSpan duration, CancellationToken cancellationToken)
        {
            MeasurementCalled = true;
            var recorder = new LatencyRecorder(true);
            var metrics = new LoadGeneratorMetrics
            {
                Throughput = 1.0,
                ErrorRate = 0.0,
                BytesOut = 100,
                BytesIn = 50,
                NetworkUtilization = 0.01,
                ScheduledOperations = 1,
                OperationsCompleted = 1
            };
            return Task.FromResult((recorder, metrics));
        }

        public void SetBaselineLatency(long baselineLatencyMicros)
        {
            // No-op for test
        }
    }

    private sealed class MockWorkload : IWorkload
    {
        public OperationBase NextOperation(Random rng)
        {
            return new ReadOperation { Id = "test-doc" };
        }

        public IWorkload? CreateWarmupWorkload(long preloadCount, IKeyDistribution distribution) => null;
    }

    [Fact]
    public void ApplyOutputOptions_Preserves_Step_And_Shape()
    {
        var originalOpts = new RunOptions
        {
            Url = "http://localhost:8080",
            Database = "test",
            Shape = LoadShape.Rate,
            Step = new StepPlan(100, 1000, 2.0),
            Profile = WorkloadProfile.Mixed
        };

        var modifiedOpts = CliParsing.ApplyOutputOptions(originalOpts);

        modifiedOpts.Shape.Should().Be(LoadShape.Rate);
        modifiedOpts.Step.Start.Should().Be(100);
        modifiedOpts.Step.End.Should().Be(1000);
        modifiedOpts.Step.Factor.Should().Be(2.0);
    }

    [Fact]
    public void ParseDuration_Handles_Seconds_Suffix()
    {
        var duration = CliParsing.ParseDuration("10s");
        duration.Should().Be(TimeSpan.FromSeconds(10));
    }

    [Fact]
    public void ParseDuration_Handles_Minutes_Suffix()
    {
        var duration = CliParsing.ParseDuration("5m");
        duration.Should().Be(TimeSpan.FromMinutes(5));
    }

    [Fact]
    public void ParseDuration_Handles_Milliseconds_Suffix()
    {
        var duration = CliParsing.ParseDuration("250ms");
        duration.Should().Be(TimeSpan.FromMilliseconds(250));
    }

    [Fact]
    public void ParseDuration_Handles_Plain_Number_As_Seconds()
    {
        var duration = CliParsing.ParseDuration("30");
        duration.Should().Be(TimeSpan.FromSeconds(30));
    }

    [Fact]
    public void ParseDuration_Handles_Full_TimeSpan_String()
    {
        var duration = CliParsing.ParseDuration("00:16:21.7371071");
        duration.Should().Be(new TimeSpan(0, 0, 16, 21, 737).Add(TimeSpan.FromTicks(1071)));
    }

    [Fact]
    public void ParseDuration_Handles_Simple_TimeSpan_String()
    {
        var duration = CliParsing.ParseDuration("01:30:00");
        duration.Should().Be(TimeSpan.FromHours(1).Add(TimeSpan.FromMinutes(30)));
    }

    [Fact]
    public void ParseDuration_Handles_TimeSpan_With_Days()
    {
        var duration = CliParsing.ParseDuration("1.02:30:00");
        duration.Should().Be(TimeSpan.FromDays(1).Add(TimeSpan.FromHours(2)).Add(TimeSpan.FromMinutes(30)));
    }

    [Fact]
    public void ParseDuration_Is_Case_Insensitive_For_Custom_Formats()
    {
        var duration1 = CliParsing.ParseDuration("100MS");
        var duration2 = CliParsing.ParseDuration("100ms");
        duration1.Should().Be(duration2);
        duration1.Should().Be(TimeSpan.FromMilliseconds(100));
    }
}
