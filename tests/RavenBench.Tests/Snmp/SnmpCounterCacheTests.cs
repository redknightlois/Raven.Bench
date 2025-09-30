using System;
using FluentAssertions;
using RavenBench.Metrics.Snmp;
using Xunit;

namespace RavenBench.Tests.Snmp;

public class SnmpCounterCacheTests
{
    [Fact]
    public void FirstSample_ReturnsNull()
    {
        // Arrange
        var cache = new SnmpCounterCache();
        var sample = new SnmpSample
        {
            Timestamp = DateTime.UtcNow,
            MachineCpu = 50.0,
            IoReadOpsPerSec = 100.0
        };

        // Act
        var rates = cache.ComputeRates(sample);

        // Assert
        rates.Should().BeNull("first sample establishes baseline");
    }

    [Fact]
    public void SecondSample_ReturnsRates()
    {
        // Arrange
        var cache = new SnmpCounterCache();
        var time1 = DateTime.UtcNow;
        var time2 = time1.AddSeconds(2);

        var sample1 = new SnmpSample
        {
            Timestamp = time1,
            IoReadOpsPerSec = 100.0,
            IoWriteOpsPerSec = 50.0
        };

        var sample2 = new SnmpSample
        {
            Timestamp = time2,
            IoReadOpsPerSec = 200.0,
            IoWriteOpsPerSec = 100.0
        };

        // Act
        cache.ComputeRates(sample1); // Establish baseline
        var rates = cache.ComputeRates(sample2);

        // Assert
        rates.Should().NotBeNull();
        rates!.IoReadOpsPerSec.Should().Be(200.0, "rates are passed through directly");
        rates.IoWriteOpsPerSec.Should().Be(100.0, "rates are passed through directly");
    }

    [Fact]
    public void GaugeMetrics_CopiedDirectly()
    {
        // Arrange
        var cache = new SnmpCounterCache();
        var time1 = DateTime.UtcNow;
        var time2 = time1.AddSeconds(1);

        var sample1 = new SnmpSample
        {
            Timestamp = time1,
            MachineCpu = 30.0,
            ProcessCpu = 20.0,
            ManagedMemoryMb = 1024,
            Load1Min = 1.5
        };

        var sample2 = new SnmpSample
        {
            Timestamp = time2,
            MachineCpu = 45.0,
            ProcessCpu = 35.0,
            ManagedMemoryMb = 2048,
            Load1Min = 2.0
        };

        // Act
        cache.ComputeRates(sample1);
        var rates = cache.ComputeRates(sample2);

        // Assert
        rates.Should().NotBeNull();
        rates!.MachineCpu.Should().Be(45.0);
        rates.ProcessCpu.Should().Be(35.0);
        rates.ManagedMemoryMb.Should().Be(2048);
        rates.Load1Min.Should().Be(2.0);
    }

    [Fact]
    public void NullMetrics_ResultInNullRates()
    {
        // Arrange
        var cache = new SnmpCounterCache();
        var time1 = DateTime.UtcNow;
        var time2 = time1.AddSeconds(1);

        var sample1 = new SnmpSample
        {
            Timestamp = time1,
            IoReadOpsPerSec = null
        };

        var sample2 = new SnmpSample
        {
            Timestamp = time2,
            IoReadOpsPerSec = null
        };

        // Act
        cache.ComputeRates(sample1);
        var rates = cache.ComputeRates(sample2);

        // Assert
        rates.Should().NotBeNull();
        rates!.IoReadOpsPerSec.Should().BeNull();
    }

    [Fact]
    public void Reset_ClearsPreviousSample()
    {
        // Arrange
        var cache = new SnmpCounterCache();
        var time1 = DateTime.UtcNow;
        var time2 = time1.AddSeconds(1);

        var sample1 = new SnmpSample
        {
            Timestamp = time1,
            IoReadOpsPerSec = 100.0
        };

        var sample2 = new SnmpSample
        {
            Timestamp = time2,
            IoReadOpsPerSec = 150.0
        };

        // Act
        cache.ComputeRates(sample1);
        cache.Reset();
        var rates = cache.ComputeRates(sample2);

        // Assert
        rates.Should().BeNull("reset should clear baseline");
    }

    [Fact]
    public void AllMetrics_PassedThrough()
    {
        // Arrange
        var cache = new SnmpCounterCache();
        var time1 = DateTime.UtcNow;
        var time2 = time1.AddSeconds(2);

        var sample1 = new SnmpSample
        {
            Timestamp = time1,
            IoReadOpsPerSec = 100.0,
            TotalRequests = 10000
        };

        var sample2 = new SnmpSample
        {
            Timestamp = time2,
            IoReadOpsPerSec = 300.0,
            IoWriteOpsPerSec = 200.0,
            IoReadKbPerSec = 256.0,
            IoWriteKbPerSec = 128.0,
            RequestsPerSec = 1000.0,
            TotalRequests = 12000
        };

        // Act
        cache.ComputeRates(sample1);
        var rates = cache.ComputeRates(sample2);

        // Assert
        rates.Should().NotBeNull();
        rates!.IoReadOpsPerSec.Should().Be(300.0);
        rates.IoWriteOpsPerSec.Should().Be(200.0);
        rates.IoReadBytesPerSec.Should().BeApproximately(256.0 * 1024, 0.01);
        rates.IoWriteBytesPerSec.Should().BeApproximately(128.0 * 1024, 0.01);
        rates.ServerRequestsPerSec.Should().Be(1000.0);
        rates.ErrorsPerSec.Should().BeNull("not available from RavenDB");
    }

    [Fact]
    public void ConsecutiveSamples_UseLatestValues()
    {
        // Arrange
        var cache = new SnmpCounterCache();
        var time1 = DateTime.UtcNow;
        var time2 = time1.AddSeconds(1);
        var time3 = time2.AddSeconds(1);

        var sample1 = new SnmpSample { Timestamp = time1, IoReadOpsPerSec = 100.0 };
        var sample2 = new SnmpSample { Timestamp = time2, IoReadOpsPerSec = 200.0 };
        var sample3 = new SnmpSample { Timestamp = time3, IoReadOpsPerSec = 150.0 };

        // Act
        var rates1 = cache.ComputeRates(sample1);
        var rates2 = cache.ComputeRates(sample2);
        var rates3 = cache.ComputeRates(sample3);

        // Assert
        rates1.Should().BeNull("first sample");
        rates2.Should().NotBeNull();
        rates2!.IoReadOpsPerSec.Should().Be(200.0);
        rates3.Should().NotBeNull();
        rates3!.IoReadOpsPerSec.Should().Be(150.0);
    }
}
