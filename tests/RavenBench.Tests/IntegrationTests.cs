using System;
using System.Linq;
using System.Threading.Tasks;
using FluentAssertions;
using RavenBench.Metrics;
using RavenBench.Reporting;
using RavenBench.Transport;
using RavenBench.Util;
using RavenBench.Workload;
using Xunit;

namespace RavenBench.Tests;

public class IntegrationTests
{
    [Fact]
    public void BenchmarkRun_Structure_Validation()
    {
        // INVARIANT: BenchmarkRun should have proper structure and validation
        // INVARIANT: All metrics should be within reasonable bounds
        
        var result = IntegrationTestHelper.CreateSampleBenchmarkRun(3);
        
        // Validate overall structure
        result.Should().NotBeNull();
        result.Steps.Should().NotBeEmpty();
        result.Steps.Should().HaveCount(3);
        
        // Validate concurrency progression
        var concurrencies = result.Steps.Select(s => s.Concurrency).ToArray();
        concurrencies.Should().BeInAscendingOrder();
        
        // Validate all steps have reasonable metrics
        result.Steps.Should().AllSatisfy(step =>
        {
            step.Throughput.Should().BeGreaterThan(0);
            step.ErrorRate.Should().BeInRange(0, 1);
            step.P50Ms.Should().BeGreaterThan(0);
            step.P95Ms.Should().BeGreaterOrEqualTo(step.P50Ms);
            step.P99Ms.Should().BeGreaterOrEqualTo(step.P95Ms);
            step.ClientCpu.Should().BeGreaterOrEqualTo(0);
            step.NetworkUtilization.Should().BeGreaterOrEqualTo(0);
            step.BytesOut.Should().BeGreaterThan(0);
            step.BytesIn.Should().BeGreaterThan(0);
        });
        
        // Validate transport metadata
        result.ClientCompression.Should().NotBeNullOrEmpty();
        result.EffectiveHttpVersion.Should().NotBeNullOrEmpty();
        result.MaxNetworkUtilization.Should().BeGreaterOrEqualTo(0);
    }
    
    [Fact]
    public async Task Transport_Server_Metrics_Integration()
    {
        // INVARIANT: Transport should provide server metrics correctly
        // INVARIANT: Server metrics should have valid values
        
        using var transport = new StubTransport();
        
        var metrics = await transport.GetServerMetricsAsync();
        
        metrics.Should().NotBeNull();
        metrics.CpuUsagePercent.Should().BeGreaterOrEqualTo(0);
        metrics.MemoryUsageMB.Should().BeGreaterThan(0);
        metrics.ActiveConnections.Should().BeGreaterOrEqualTo(0);
        metrics.RequestsPerSecond.Should().BeGreaterOrEqualTo(0);
        metrics.Timestamp.Should().BeAfter(DateTime.MinValue);
    }
    
    [Fact]
    public void PayloadGeneration_Integration_With_Workload()
    {
        // INVARIANT: PayloadGenerator should integrate correctly with workload system
        // INVARIANT: Generated payloads should match size expectations
        
        var distribution = new UniformDistribution();
        var workload = new MixedWorkload(
            WorkloadMix.FromWeights(0, 100, 0),
            distribution,
            2048 // 2KB documents
        );
        
        var rng = new Random(42);
        
        // Generate several operations to test consistency
        for (int i = 0; i < 10; i++)
        {
            var op = workload.NextOperation(rng);
            
            op.Should().NotBeNull();
            op.Type.Should().NotBe(OperationType.ReadById); // Should be write since 100% writes
            
            if (op.Payload != null)
            {
                // Payload should be valid JSON
                var document = System.Text.Json.JsonDocument.Parse(op.Payload);
                document.RootElement.ValueKind.Should().Be(System.Text.Json.JsonValueKind.Object);
                
                // Should be reasonably sized
                var payloadBytes = System.Text.Encoding.UTF8.GetByteCount(op.Payload);
                payloadBytes.Should().BeGreaterThan(100);
                payloadBytes.Should().BeLessThan(5000); // Within reasonable bounds for 2KB target
            }
        }
    }
    
    private static RunOptions CreateBasicOptions() => new()
    {
        Url = "http://localhost:8080",
        Database = "test", 
        Writes = 100,
        Distribution = "uniform",
        Compression = "identity",
        DocumentSizeBytes = 1024,
        Warmup = TimeSpan.FromMilliseconds(25),
        Duration = TimeSpan.FromMilliseconds(50),
        ConcurrencyStart = 2,
        ConcurrencyEnd = 2, // Single concurrency for simple tests
        ConcurrencyFactor = 2
    };
}

// Simple integration test that just validates basic workflow components
internal static class IntegrationTestHelper
{
    public static BenchmarkRun CreateSampleBenchmarkRun(int stepCount = 2)
    {
        var steps = new System.Collections.Generic.List<StepResult>();
        
        for (int i = 0; i < stepCount; i++)
        {
            var concurrency = (i + 1) * 2; // 2, 4, 6, etc.
            steps.Add(new StepResult
            {
                Concurrency = concurrency,
                Throughput = 100.0 + i * 50, // Increasing throughput
                ErrorRate = 0.01, // 1% error rate
                BytesOut = 1000 + i * 100,
                BytesIn = 800 + i * 80,
                P50Ms = 10.0 + i,
                P95Ms = 20.0 + i,
                P99Ms = 30.0 + i,
                ClientCpu = 0.25 + i * 0.1,
                NetworkUtilization = 0.1 + i * 0.05
            });
        }
        
        return new BenchmarkRun
        {
            Steps = steps,
            MaxNetworkUtilization = steps.Max(s => s.NetworkUtilization),
            ClientCompression = "identity",
            EffectiveHttpVersion = "1.1"
        };
    }
}

// Note: StubTransport is defined in ServerMetricsTrackerTests.cs and reused here