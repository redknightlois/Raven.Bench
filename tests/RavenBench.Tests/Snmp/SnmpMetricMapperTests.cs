using System;
using System.Collections.Generic;
using FluentAssertions;
using Lextm.SharpSnmpLib;
using RavenBench.Metrics.Snmp;
using Xunit;

namespace RavenBench.Tests.Snmp;

public class SnmpMetricMapperTests
{
    [Fact]
    public void MapToSample_WithGauge32_ExtractsValues()
    {
        // Arrange
        var values = new Dictionary<string, Variable>
        {
            [SnmpOids.MachineCpu] = new Variable(new ObjectIdentifier(SnmpOids.MachineCpu), new Gauge32(75)),
            [SnmpOids.ProcessCpu] = new Variable(new ObjectIdentifier(SnmpOids.ProcessCpu), new Gauge32(50))
        };

        // Act
        var sample = SnmpMetricMapper.MapToSample(values);

        // Assert
        sample.MachineCpu.Should().Be(75.0);
        sample.ProcessCpu.Should().Be(50.0);
    }

    [Fact]
    public void MapToSample_WithGauge32Rates_ExtractsValues()
    {
        // Arrange - use database-specific OIDs (database index = 1)
        var dbIoReadOps = "1.3.6.1.4.1.45751.1.1.5.2.1.2.7";
        var dbIoWriteOps = "1.3.6.1.4.1.45751.1.1.5.2.1.2.8";
        var values = new Dictionary<string, Variable>
        {
            [dbIoReadOps] = new Variable(new ObjectIdentifier(dbIoReadOps), new Gauge32(123)),
            [dbIoWriteOps] = new Variable(new ObjectIdentifier(dbIoWriteOps), new Gauge32(789))
        };

        // Act
        var sample = SnmpMetricMapper.MapToSample(values);

        // Assert
        sample.IoReadOpsPerSec.Should().Be(123.0);
        sample.IoWriteOpsPerSec.Should().Be(789.0);
    }

    [Fact]
    public void MapToSample_WithMissingValues_ReturnsNulls()
    {
        // Arrange
        var values = new Dictionary<string, Variable>
        {
            [SnmpOids.MachineCpu] = new Variable(new ObjectIdentifier(SnmpOids.MachineCpu), new Gauge32(50))
            // ProcessCpu missing
        };

        // Act
        var sample = SnmpMetricMapper.MapToSample(values);

        // Assert
        sample.MachineCpu.Should().Be(50.0);
        sample.ProcessCpu.Should().BeNull();
    }

    [Fact]
    public void MapToSample_WithEmptyDictionary_ReturnsEmptySample()
    {
        // Arrange
        var values = new Dictionary<string, Variable>();

        // Act
        var sample = SnmpMetricMapper.MapToSample(values);

        // Assert
        sample.IsEmpty.Should().BeTrue();
        sample.MachineCpu.Should().BeNull();
        sample.ProcessCpu.Should().BeNull();
    }

    [Fact]
    public void MapToSample_SetsTimestamp()
    {
        // Arrange
        var values = new Dictionary<string, Variable>
        {
            [SnmpOids.MachineCpu] = new Variable(new ObjectIdentifier(SnmpOids.MachineCpu), new Gauge32(50))
        };
        var beforeTime = DateTime.UtcNow;

        // Act
        var sample = SnmpMetricMapper.MapToSample(values);
        var afterTime = DateTime.UtcNow;

        // Assert
        sample.Timestamp.Should().BeOnOrAfter(beforeTime);
        sample.Timestamp.Should().BeOnOrBefore(afterTime);
    }

    [Fact]
    public void MapToSample_WithAllMinimalMetrics_ExtractsCorrectly()
    {
        // Arrange
        var values = new Dictionary<string, Variable>
        {
            [SnmpOids.MachineCpu] = new Variable(new ObjectIdentifier(SnmpOids.MachineCpu), new Gauge32(75)),
            [SnmpOids.ProcessCpu] = new Variable(new ObjectIdentifier(SnmpOids.ProcessCpu), new Gauge32(50)),
            [SnmpOids.ManagedMemory] = new Variable(new ObjectIdentifier(SnmpOids.ManagedMemory), new Gauge32(2048)),
            [SnmpOids.UnmanagedMemory] = new Variable(new ObjectIdentifier(SnmpOids.UnmanagedMemory), new Gauge32(512))
        };

        // Act
        var sample = SnmpMetricMapper.MapToSample(values);

        // Assert
        sample.MachineCpu.Should().Be(75.0);
        sample.ProcessCpu.Should().Be(50.0);
        sample.ManagedMemoryMb.Should().Be(2048);
        sample.UnmanagedMemoryMb.Should().Be(512);
        sample.IsEmpty.Should().BeFalse();
    }

    [Fact]
    public void MapToSample_WithExtendedMetrics_ExtractsAll()
    {
        // Arrange
        var values = new Dictionary<string, Variable>
        {
            [SnmpOids.MachineCpu] = new Variable(new ObjectIdentifier(SnmpOids.MachineCpu), new Gauge32(75)),
            [SnmpOids.ProcessCpu] = new Variable(new ObjectIdentifier(SnmpOids.ProcessCpu), new Gauge32(50)),
            [SnmpOids.ManagedMemory] = new Variable(new ObjectIdentifier(SnmpOids.ManagedMemory), new Gauge32(2048)),
            [SnmpOids.UnmanagedMemory] = new Variable(new ObjectIdentifier(SnmpOids.UnmanagedMemory), new Gauge32(512)),
            [SnmpOids.DirtyMemory] = new Variable(new ObjectIdentifier(SnmpOids.DirtyMemory), new Gauge32(128)),
            [SnmpOids.Load1Min] = new Variable(new ObjectIdentifier(SnmpOids.Load1Min), new Gauge32(150)),
            [SnmpOids.Load5Min] = new Variable(new ObjectIdentifier(SnmpOids.Load5Min), new Gauge32(120)),
            [SnmpOids.Load15Min] = new Variable(new ObjectIdentifier(SnmpOids.Load15Min), new Gauge32(100)),
            ["1.3.6.1.4.1.45751.1.1.5.2.1.2.7"] = new Variable(new ObjectIdentifier("1.3.6.1.4.1.45751.1.1.5.2.1.2.7"), new Counter64(10000)),
            ["1.3.6.1.4.1.45751.1.1.5.2.1.2.8"] = new Variable(new ObjectIdentifier("1.3.6.1.4.1.45751.1.1.5.2.1.2.8"), new Gauge32(5000)),
            ["1.3.6.1.4.1.45751.1.1.5.2.1.2.9"] = new Variable(new ObjectIdentifier("1.3.6.1.4.1.45751.1.1.5.2.1.2.9"), new Gauge32(1024)),
            ["1.3.6.1.4.1.45751.1.1.5.2.1.2.10"] = new Variable(new ObjectIdentifier("1.3.6.1.4.1.45751.1.1.5.2.1.2.10"), new Gauge32(512)),
            ["1.3.6.1.4.1.45751.1.1.5.2.1.3.6"] = new Variable(new ObjectIdentifier("1.3.6.1.4.1.45751.1.1.5.2.1.3.6"), new Integer32(100000)),
            ["1.3.6.1.4.1.45751.1.1.5.2.1.3.5"] = new Variable(new ObjectIdentifier("1.3.6.1.4.1.45751.1.1.5.2.1.3.5"), new Gauge32(1500))
        };

        // Act
        var sample = SnmpMetricMapper.MapToSample(values);

        // Assert
        sample.MachineCpu.Should().Be(75.0);
        sample.ProcessCpu.Should().Be(50.0);
        sample.ManagedMemoryMb.Should().Be(2048);
        sample.UnmanagedMemoryMb.Should().Be(512);
        sample.DirtyMemoryMb.Should().Be(128);
        sample.Load1Min.Should().BeNull("Windows doesn't provide load averages");
        sample.Load5Min.Should().BeNull();
        sample.Load15Min.Should().BeNull();
        sample.IoReadOpsPerSec.Should().Be(10000.0);
        sample.IoWriteOpsPerSec.Should().Be(5000.0);
        sample.IoReadKbPerSec.Should().Be(1024.0);
        sample.IoWriteKbPerSec.Should().Be(512.0);
        sample.TotalRequests.Should().Be(100000);
        sample.RequestsPerSec.Should().Be(1500.0);
    }

    [Fact]
    public void MapMetrics_BackwardCompatibility_ReturnsLegacyTuple()
    {
        // Arrange
        var values = new Dictionary<string, Variable>
        {
            [SnmpOids.MachineCpu] = new Variable(new ObjectIdentifier(SnmpOids.MachineCpu), new Gauge32(75)),
            [SnmpOids.ProcessCpu] = new Variable(new ObjectIdentifier(SnmpOids.ProcessCpu), new Gauge32(50)),
            [SnmpOids.ManagedMemory] = new Variable(new ObjectIdentifier(SnmpOids.ManagedMemory), new Gauge32(2048)),
            [SnmpOids.UnmanagedMemory] = new Variable(new ObjectIdentifier(SnmpOids.UnmanagedMemory), new Gauge32(512))
        };

        // Act
        var (machineCpu, processCpu, managedMemoryMb, unmanagedMemoryMb) = SnmpMetricMapper.MapMetrics(values);

        // Assert
        machineCpu.Should().Be(75.0);
        processCpu.Should().Be(50.0);
        managedMemoryMb.Should().Be(2048);
        unmanagedMemoryMb.Should().Be(512);
    }

    [Fact]
    public void MapToSample_WithInteger32_ConvertsToLong()
    {
        // Arrange - database-specific RequestCount OID
        var dbRequestCount = "1.3.6.1.4.1.45751.1.1.5.2.1.3.6";
        var values = new Dictionary<string, Variable>
        {
            [dbRequestCount] = new Variable(new ObjectIdentifier(dbRequestCount), new Integer32(50000))
        };

        // Act
        var sample = SnmpMetricMapper.MapToSample(values);

        // Assert
        sample.TotalRequests.Should().Be(50000);
    }

    [Fact]
    public void MapToSample_WithInteger32_ConvertsCorrectly()
    {
        // Arrange
        var values = new Dictionary<string, Variable>
        {
            [SnmpOids.Load1Min] = new Variable(new ObjectIdentifier(SnmpOids.Load1Min), new Integer32(150))
        };

        // Act
        var sample = SnmpMetricMapper.MapToSample(values);

        // Assert
        sample.Load1Min.Should().Be(150.0);
    }
}