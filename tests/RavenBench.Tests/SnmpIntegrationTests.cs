using System.Collections.Generic;
using System.Threading.Tasks;
using FluentAssertions;
using RavenBench.Metrics;
using RavenBench.Transport;
using RavenBench.Util;
using Xunit;

namespace RavenBench.Tests;

public class SnmpIntegrationTests
{
    [Fact]
    public void ServerMetricsTracker_WithSnmpEnabled_InitializesCorrectly()
    {
        // Arrange
        var transport = new TestTransport();
        var options = new RunOptions
        {
            Url = "http://localhost:8080",
            Database = "test",
            Snmp = new SnmpOptions { Enabled = true, Port = 161 },
            Profile = WorkloadProfile.Mixed
        };

        // Act
        using var tracker = new ServerMetricsTracker(transport, options);

        // Assert
        tracker.Should().NotBeNull();
        var metrics = tracker.Current;
        metrics.Should().NotBeNull();
        metrics.IsValid.Should().BeTrue();
    }

    [Fact]
    public void ServerMetricsTracker_SnmpDisabled_InitializesCorrectly()
    {
        // Arrange
        var transport = new TestTransport();
        var options = new RunOptions
        {
            Url = "http://localhost:8080",
            Database = "test",
            Snmp = SnmpOptions.Disabled,
            Profile = WorkloadProfile.Mixed
        };

        // Act
        using var tracker = new ServerMetricsTracker(transport, options);

        // Assert
        tracker.Should().NotBeNull();
        var metrics = tracker.Current;
        metrics.Should().NotBeNull();
        metrics.IsValid.Should().BeTrue();
    }

    [Fact]
    public void ServerMetricsTracker_StartStop_HandlesSnmpConfiguration()
    {
        // Arrange
        var transport = new TestTransport();
        var options = new RunOptions
        {
            Url = "http://localhost:8080",
            Database = "test",
            Snmp = new SnmpOptions { Enabled = true, Port = 161 },
            Profile = WorkloadProfile.Mixed
        };

        // Act
        using var tracker = new ServerMetricsTracker(transport, options);
        tracker.Start();
        var metrics1 = tracker.Current;
        tracker.Stop();
        var metrics2 = tracker.Current;

        // Assert
        metrics1.Should().NotBeNull();
        metrics2.Should().NotBeNull();
        metrics1.IsValid.Should().BeTrue();
        metrics2.IsValid.Should().BeTrue();
    }

    [Fact]
    public async Task ServerMetricsTracker_ConcurrentAccess_ThreadSafe()
    {
        // Arrange
        var transport = new TestTransport();
        var options = new RunOptions
        {
            Url = "http://localhost:8080",
            Database = "test",
            Snmp = new SnmpOptions { Enabled = true },
            Profile = WorkloadProfile.Mixed
        };

        // Act
        using var tracker = new ServerMetricsTracker(transport, options);
        tracker.Start();

        // Start multiple tasks that read metrics concurrently
        var tasks = new Task[10];
        for (int i = 0; i < 10; i++)
        {
            tasks[i] = Task.Run(async () =>
            {
                for (int j = 0; j < 10; j++)
                {
                    var metrics = tracker.Current;
                    metrics.IsValid.Should().BeTrue();
                    await Task.Delay(10);
                }
            });
        }

        await Task.WhenAll(tasks);
        tracker.Stop();

        // Assert
        // Should complete without exceptions
    }

    [Fact]
    public void ServerMetricsTracker_Dispose_HandlesSnmpConfiguration()
    {
        // Arrange
        var transport = new TestTransport();
        var options = new RunOptions
        {
            Url = "http://localhost:8080",
            Database = "test",
            Snmp = new SnmpOptions { Enabled = true },
            Profile = WorkloadProfile.Mixed
        };

        // Act
        var tracker = new ServerMetricsTracker(transport, options);
        tracker.Start();
        tracker.Dispose(); // Should stop polling

        // Assert
        // No exception should be thrown, and tracker should be disposed
        tracker.Current.IsValid.Should().BeTrue();
    }
}
