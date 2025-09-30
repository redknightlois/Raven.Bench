using System;
using FluentAssertions;
using RavenBench.Cli;
using Xunit;

namespace RavenBench.Tests.Cli;

public class RunSettingsSnmpTests
{
    [Fact]
    public void ToRunOptions_MapsSnmpEnabled()
    {
        var settings = new RunSettings
        {
            Url = "http://localhost:8080",
            Database = "test",
            SnmpEnabled = true
        };

        var options = settings.ToRunOptions();

        options.Snmp.Enabled.Should().BeTrue();
    }

    [Fact]
    public void ToRunOptions_MapsSnmpEnabledByDefault()
    {
        var settings = new RunSettings
        {
            Url = "http://localhost:8080",
            Database = "test"
        };

        var options = settings.ToRunOptions();

        options.Snmp.Enabled.Should().BeTrue();
    }

    [Fact]
    public void ToRunOptions_MapsSnmpPort()
    {
        var settings = new RunSettings
        {
            Url = "http://localhost:8080",
            Database = "test",
            SnmpEnabled = true,
            SnmpPort = 161
        };

        var options = settings.ToRunOptions();

        options.SnmpPort.Should().Be(161);
    }

    [Fact]
    public void ToRunOptions_DefaultsSnmpPortTo161()
    {
        var settings = new RunSettings
        {
            Url = "http://localhost:8080",
            Database = "test",
            SnmpEnabled = true
        };

        var options = settings.ToRunOptions();

        options.SnmpPort.Should().Be(161);
    }

    [Fact]
    public void ToRunOptions_MapsSnmpInterval()
    {
        var settings = new RunSettings
        {
            Url = "http://localhost:8080",
            Database = "test",
            SnmpEnabled = true,
            SnmpInterval = "10s"
        };

        var options = settings.ToRunOptions();

        options.SnmpPollInterval.Should().Be(TimeSpan.FromSeconds(10));
    }

    [Fact]
    public void ToRunOptions_DefaultsSnmpIntervalTo250Milliseconds()
    {
        var settings = new RunSettings
        {
            Url = "http://localhost:8080",
            Database = "test",
            SnmpEnabled = true
        };

        var options = settings.ToRunOptions();

        options.SnmpPollInterval.Should().Be(TimeSpan.FromMilliseconds(250));
    }
}
