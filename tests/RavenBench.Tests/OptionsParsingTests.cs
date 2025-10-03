using FluentAssertions;
using RavenBench.Cli;
using RavenBench.Util;
using Xunit;

namespace RavenBench.Tests;

public class CliParsingTests
{
    [Fact]
    public void Converts_Settings_To_RunOptions_With_Concurrency_Range()
    {
        var settings = new RunSettings
        {
            Url = "http://localhost:10101",
            Database = "ycsb",
            Concurrency = "16..256x1.5",
            KneeRule = "dthr=3%,dp95=30%",
            MaxErrors = "1%",
            Profile = "mixed"
        };

        var opts = settings.ToRunOptions();

        opts.ConcurrencyStart.Should().Be(16);
        opts.ConcurrencyEnd.Should().Be(256);
        opts.ConcurrencyFactor.Should().Be(1.5);
        opts.KneeThroughputDelta.Should().BeApproximately(0.03, 1e-9);
        opts.KneeP95Delta.Should().BeApproximately(0.30, 1e-9);
        opts.MaxErrorRate.Should().BeApproximately(0.01, 1e-9);
    }

    [Fact]
    public void Converts_Weight_Based_Mix_Flags()
    {
        var settings = new RunSettings
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
}
