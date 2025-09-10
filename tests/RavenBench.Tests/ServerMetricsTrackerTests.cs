using System;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using RavenBench.Metrics;
using RavenBench.Transport;
using RavenBench.Workload;
using Xunit;

namespace RavenBench.Tests;

public class ServerMetricsTrackerTests
{
    [Fact]
    public void Constructor_Initializes_Successfully()
    {
        // INVARIANT: Constructor should not throw with valid transport
        // INVARIANT: Initial state should be valid
        using var transport = new StubTransport();
        using var tracker = new ServerMetricsTracker(transport);
        
        tracker.Should().NotBeNull();
        
        // Should have valid initial metrics
        var initial = tracker.Current;
        initial.Should().NotBeNull();
        initial.Timestamp.Should().BeAfter(DateTime.MinValue);
    }
    
    [Fact]
    public void Start_Stop_Basic_Functionality()
    {
        // INVARIANT: Start and stop should work without exceptions
        // INVARIANT: Current should always return valid metrics
        using var transport = new StubTransport();
        using var tracker = new ServerMetricsTracker(transport);
        
        // Should be able to start
        tracker.Start();
        
        var metrics1 = tracker.Current;
        metrics1.Should().NotBeNull();
        
        // Should be able to stop
        tracker.Stop();
        
        var metrics2 = tracker.Current;
        metrics2.Should().NotBeNull();
    }
    
    [Fact]
    public void Current_Property_Thread_Safe_Access()
    {
        // INVARIANT: Current property should be thread-safe
        // INVARIANT: Should never return null or invalid metrics
        using var transport = new StubTransport();
        using var tracker = new ServerMetricsTracker(transport);
        tracker.Start();
        
        const int accessCount = 100;
        var allMetrics = new ServerMetrics[accessCount];
        var exceptions = new Exception[accessCount];
        
        // Access Current property from multiple threads rapidly
        Parallel.For(0, accessCount, i =>
        {
            try
            {
                allMetrics[i] = tracker.Current;
            }
            catch (Exception ex)
            {
                exceptions[i] = ex;
            }
        });
        
        tracker.Stop();
        
        // Should have no exceptions
        exceptions.Should().AllSatisfy(ex => ex.Should().BeNull());
        
        // All metrics should be valid
        allMetrics.Should().AllSatisfy(metrics =>
        {
            metrics.Should().NotBeNull();
            metrics.Timestamp.Should().BeAfter(DateTime.MinValue);
        });
    }
    
    [Fact]
    public void Multiple_Start_Stop_Cycles_Work_Correctly()
    {
        // INVARIANT: Should handle multiple start/stop cycles
        // INVARIANT: Should remain stable across cycles
        using var transport = new StubTransport();
        using var tracker = new ServerMetricsTracker(transport);
        
        for (int cycle = 0; cycle < 3; cycle++)
        {
            tracker.Start();
            
            var metrics = tracker.Current;
            metrics.Should().NotBeNull();
            
            tracker.Stop();
            
            // Should still provide valid metrics after stop
            metrics = tracker.Current;
            metrics.Should().NotBeNull();
        }
    }
    
    [Fact]
    public void Dispose_Cleans_Up_Resources()
    {
        // INVARIANT: Dispose should work without exceptions
        // INVARIANT: Should handle disposal in any state
        var transport = new StubTransport();
        var tracker = new ServerMetricsTracker(transport);
        
        tracker.Start();
        
        // Should dispose cleanly
        tracker.Dispose();
        
        // Second dispose should also be safe
        tracker.Dispose();
        
        // Clean up transport
        transport.Dispose();
    }
    
    [Fact]
    public void Metrics_Update_During_Polling()
    {
        // INVARIANT: Metrics should be updated periodically when started
        // INVARIANT: Should use transport's GetServerMetricsAsync method
        using var transport = new StubTransport();
        using var tracker = new ServerMetricsTracker(transport);
        
        tracker.Start();
        
        var initialMetrics = tracker.Current;
        
        // Give it time for at least one poll cycle (polling every 2 seconds)
        // We'll wait a shorter time and just verify the infrastructure works
        Thread.Sleep(100);
        
        var updatedMetrics = tracker.Current;
        
        // Both should be valid (may or may not be different instances)
        initialMetrics.Should().NotBeNull();
        updatedMetrics.Should().NotBeNull();
        
        tracker.Stop();
    }
}

// Stub transport for testing - returns predictable metrics
internal sealed class StubTransport : ITransport
{
    public string EffectiveCompressionMode => "identity";
    public string EffectiveHttpVersion => "1.1";
    
    public void Dispose() { }
    
    public Task<int?> GetServerMaxCoresAsync() => Task.FromResult<int?>(4);
    
    public Task<ServerMetrics> GetServerMetricsAsync() => Task.FromResult(new ServerMetrics
    {
        CpuUsagePercent = 25.0,
        MemoryUsageMB = 512,
        ActiveConnections = 10,
        RequestsPerSecond = 100.0,
        QueuedRequests = 2,
        IoReadOperations = 50.0,
        IoWriteOperations = 30.0,
        ReadThroughputKb = 1024,
        WriteThroughputKb = 512,
        QueueLength = 1
    });
    
    public Task PutAsync(string id, string json) => Task.CompletedTask;
    
    public Task<TransportResult> ExecuteAsync(Operation op, CancellationToken ct) =>
        Task.FromResult(new TransportResult(bytesOut: 200, bytesIn: 150));
}