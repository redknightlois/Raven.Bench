using System;
using FluentAssertions;
using RavenBench.Metrics.Snmp;
using RavenBench.Util;
using Xunit;

namespace RavenBench.Tests.Snmp;

public class SnmpProfileTests
{
    [Fact]
    public void GetOidsForProfile_Minimal_Returns4Oids()
    {
        // Act
        var oids = SnmpOids.GetOidsForProfile(SnmpProfile.Minimal);

        // Assert
        oids.Should().HaveCount(4);
        oids.Should().Contain(SnmpOids.MachineCpu);
        oids.Should().Contain(SnmpOids.ProcessCpu);
        oids.Should().Contain(SnmpOids.ManagedMemory);
        oids.Should().Contain(SnmpOids.UnmanagedMemory);
    }

    [Fact]
    public void GetOidsForProfile_Extended_ServerWide_Returns8Oids()
    {
        // Act - server-wide metrics only (no database index)
        var oids = SnmpOids.GetOidsForProfile(SnmpProfile.Extended);

        // Assert
        oids.Should().HaveCount(8);

        // Minimal metrics
        oids.Should().Contain(SnmpOids.MachineCpu);
        oids.Should().Contain(SnmpOids.ProcessCpu);
        oids.Should().Contain(SnmpOids.ManagedMemory);
        oids.Should().Contain(SnmpOids.UnmanagedMemory);

        // Extended server-wide metrics
        oids.Should().Contain(SnmpOids.DirtyMemory);
        oids.Should().Contain(SnmpOids.Load1Min);
        oids.Should().Contain(SnmpOids.Load5Min);
        oids.Should().Contain(SnmpOids.Load15Min);
    }

    [Fact]
    public void GetOidsForProfile_Extended_WithDatabaseIndex_Returns14Oids()
    {
        // Act - with database index for database-specific metrics
        var databaseIndex = 1L;
        var oids = SnmpOids.GetOidsForProfile(SnmpProfile.Extended, databaseIndex);

        // Assert
        oids.Should().HaveCount(14);

        // Server-wide metrics
        oids.Should().Contain(SnmpOids.MachineCpu);
        oids.Should().Contain(SnmpOids.ProcessCpu);
        oids.Should().Contain(SnmpOids.ManagedMemory);
        oids.Should().Contain(SnmpOids.UnmanagedMemory);
        oids.Should().Contain(SnmpOids.DirtyMemory);
        oids.Should().Contain(SnmpOids.Load1Min);
        oids.Should().Contain(SnmpOids.Load5Min);
        oids.Should().Contain(SnmpOids.Load15Min);

        // Database-specific metrics (formatted with database index)
        oids.Should().Contain($"1.3.6.1.4.1.45751.1.1.5.2.{databaseIndex}.2.7");  // IoReadOps
        oids.Should().Contain($"1.3.6.1.4.1.45751.1.1.5.2.{databaseIndex}.2.8");  // IoWriteOps
        oids.Should().Contain($"1.3.6.1.4.1.45751.1.1.5.2.{databaseIndex}.2.9");  // IoReadBytes
        oids.Should().Contain($"1.3.6.1.4.1.45751.1.1.5.2.{databaseIndex}.2.10"); // IoWriteBytes
        oids.Should().Contain($"1.3.6.1.4.1.45751.1.1.5.2.{databaseIndex}.3.6");  // RequestCount
        oids.Should().Contain($"1.3.6.1.4.1.45751.1.1.5.2.{databaseIndex}.3.5");  // RequestsPerSecond
    }

    [Fact]
    public void SnmpOptions_Disabled_HasEnabledFalse()
    {
        // Act
        var options = SnmpOptions.Disabled;

        // Assert
        options.Enabled.Should().BeFalse();
    }

    [Fact]
    public void SnmpOptions_Defaults_AreCorrect()
    {
        // Arrange & Act
        var options = new SnmpOptions { Enabled = true };

        // Assert
        options.Enabled.Should().BeTrue();
        options.Port.Should().Be(161);
        options.PollInterval.Should().Be(TimeSpan.FromMilliseconds(250));
        options.Profile.Should().Be(SnmpProfile.Minimal);
        options.Timeout.Should().Be(TimeSpan.FromSeconds(5));
    }

    [Fact]
    public void SnmpOptions_Community_IsFixedToRavenDB()
    {
        // Act
        var community = SnmpOptions.Community;

        // Assert
        community.Should().Be("ravendb");
    }

    [Fact]
    public void SnmpSample_IsEmpty_WhenNoMetrics()
    {
        // Arrange
        var sample = new SnmpSample();

        // Act & Assert
        sample.IsEmpty.Should().BeTrue();
    }

    [Fact]
    public void SnmpSample_IsNotEmpty_WithAtLeastOneMetric()
    {
        // Arrange
        var sample = new SnmpSample { MachineCpu = 50.0 };

        // Act & Assert
        sample.IsEmpty.Should().BeFalse();
    }

    [Fact]
    public void SnmpRates_IsEmpty_WhenNoMetrics()
    {
        // Arrange
        var rates = new SnmpRates();

        // Act & Assert
        rates.IsEmpty.Should().BeTrue();
    }

    [Fact]
    public void SnmpRates_IsNotEmpty_WithGaugeMetric()
    {
        // Arrange
        var rates = new SnmpRates { MachineCpu = 50.0 };

        // Act & Assert
        rates.IsEmpty.Should().BeFalse();
    }

    [Fact]
    public void SnmpRates_IsNotEmpty_WithRateMetric()
    {
        // Arrange
        var rates = new SnmpRates { IoReadOpsPerSec = 100.0 };

        // Act & Assert
        rates.IsEmpty.Should().BeFalse();
    }
}