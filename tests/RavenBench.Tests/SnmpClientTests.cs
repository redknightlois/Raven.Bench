using System.Collections.Generic;
using System.Threading.Tasks;
using FluentAssertions;
using Lextm.SharpSnmpLib;
using RavenBench.Core.Metrics.Snmp;
using Xunit;

namespace RavenBench.Tests;

public class SnmpMetricMapperTests
{
    [Fact]
    public void MapMetrics_ValidGauge32Values_ReturnsCorrectMetrics()
    {
        // Arrange
        var values = new Dictionary<string, Variable>
        {
            [SnmpOids.MachineCpu] = new Variable(new ObjectIdentifier(SnmpOids.MachineCpu), new Gauge32(75)),
            [SnmpOids.ProcessCpu] = new Variable(new ObjectIdentifier(SnmpOids.ProcessCpu), new Gauge32(60)),
            [SnmpOids.ManagedMemory] = new Variable(new ObjectIdentifier(SnmpOids.ManagedMemory), new Gauge32(1024)),
            [SnmpOids.UnmanagedMemory] = new Variable(new ObjectIdentifier(SnmpOids.UnmanagedMemory), new Gauge32(2048))
        };

        // Act
        var (machineCpu, processCpu, managedMemoryMb, unmanagedMemoryMb) = SnmpMetricMapper.MapMetrics(values);

        // Assert
        machineCpu.Should().Be(75.0);
        processCpu.Should().Be(60.0);
        managedMemoryMb.Should().Be(1024L); // 1024 MB
        unmanagedMemoryMb.Should().Be(2048L); // 2048 MB
    }

    [Fact]
    public void MapMetrics_MissingOids_ReturnsNullValues()
    {
        // Arrange
        var values = new Dictionary<string, Variable>();

        // Act
        var (machineCpu, processCpu, managedMemoryMb, unmanagedMemoryMb) = SnmpMetricMapper.MapMetrics(values);

        // Assert
        machineCpu.Should().BeNull();
        processCpu.Should().BeNull();
        managedMemoryMb.Should().BeNull();
        unmanagedMemoryMb.Should().BeNull();
    }

    [Fact]
    public void MapMetrics_WrongDataTypes_ReturnsNullForInvalidTypes()
    {
        // Arrange
        var values = new Dictionary<string, Variable>
        {
            [SnmpOids.MachineCpu] = new Variable(new ObjectIdentifier(SnmpOids.MachineCpu), new OctetString("wrong type")),
            [SnmpOids.ProcessCpu] = new Variable(new ObjectIdentifier(SnmpOids.ProcessCpu), new Gauge32(40)),
            [SnmpOids.ManagedMemory] = new Variable(new ObjectIdentifier(SnmpOids.ManagedMemory), new OctetString("wrong type")),
            [SnmpOids.UnmanagedMemory] = new Variable(new ObjectIdentifier(SnmpOids.UnmanagedMemory), new Gauge32(50))
        };

        // Act
        var (machineCpu, processCpu, managedMemoryMb, unmanagedMemoryMb) = SnmpMetricMapper.MapMetrics(values);

        // Assert
        machineCpu.Should().BeNull(); // Wrong type
        processCpu.Should().Be(40.0); // Correct type
        managedMemoryMb.Should().BeNull(); // Wrong type
        unmanagedMemoryMb.Should().Be(50L); // Correct type
    }

    [Fact]
    public void MapMetrics_EmptyDictionary_ReturnsNullValues()
    {
        // Arrange
        var values = new Dictionary<string, Variable>();

        // Act
        var (machineCpu, processCpu, managedMemoryMb, unmanagedMemoryMb) = SnmpMetricMapper.MapMetrics(values);

        // Assert
        machineCpu.Should().BeNull();
        processCpu.Should().BeNull();
        managedMemoryMb.Should().BeNull();
        unmanagedMemoryMb.Should().BeNull();
    }
}