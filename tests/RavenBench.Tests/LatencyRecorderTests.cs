using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using RavenBench.Metrics;
using Xunit;

namespace RavenBench.Tests;

public class LatencyRecorderTests
{
    [Fact]
    public void Record_And_GetPercentile_Basic_Functionality()
    {
        // INVARIANT: Recorded values should be reflected in percentile calculations
        // INVARIANT: Percentiles should be monotonically increasing (p50 <= p95 <= p99)
        var recorder = new LatencyRecorder(recordLatencies: true);
        
        // Record some known values: 100, 200, 300, ..., 1000 microseconds
        for (int i = 1; i <= 10; i++)
        {
            recorder.Record(i * 100);
        }
        
        var p50 = recorder.GetPercentile(50);
        var p95 = recorder.GetPercentile(95);
        var p99 = recorder.GetPercentile(99);
        
        // Should have reasonable values
        p50.Should().BeGreaterThan(0);
        p95.Should().BeGreaterThan(0);
        p99.Should().BeGreaterThan(0);
        
        // Percentiles should be monotonically increasing
        p50.Should().BeLessOrEqualTo(p95);
        p95.Should().BeLessOrEqualTo(p99);
    }
    
    [Fact]
    public void Disabled_Recording_Returns_Zero_Percentiles()
    {
        // INVARIANT: When recording is disabled, percentiles should return 0
        // INVARIANT: Recording should not consume resources when disabled
        var recorder = new LatencyRecorder(recordLatencies: false);
        
        // Record some values - should be ignored
        recorder.Record(1000);
        recorder.Record(2000);
        recorder.Record(3000);
        
        // All percentiles should return 0 since recording is disabled
        recorder.GetPercentile(50).Should().Be(0);
        recorder.GetPercentile(95).Should().Be(0);
        recorder.GetPercentile(99).Should().Be(0);
    }
    
    [Fact]
    public void Empty_Recorder_Returns_Zero_Percentiles()
    {
        // INVARIANT: Empty recorder should handle percentile requests gracefully
        var recorder = new LatencyRecorder(recordLatencies: true);
        
        // No recordings made
        recorder.GetPercentile(50).Should().Be(0);
        recorder.GetPercentile(95).Should().Be(0);
        recorder.GetPercentile(99).Should().Be(0);
    }
    
    [Fact]
    public void Single_Value_Returns_That_Value_For_All_Percentiles()
    {
        // INVARIANT: Single value should be returned for all percentiles
        var recorder = new LatencyRecorder(recordLatencies: true);
        
        const long singleValue = 1500;
        recorder.Record(singleValue);
        
        recorder.GetPercentile(50).Should().Be(singleValue);
        recorder.GetPercentile(95).Should().Be(singleValue);
        recorder.GetPercentile(99).Should().Be(singleValue);
    }
    
    [Fact]
    public async Task Concurrent_Recording_Does_Not_Cause_Exceptions()
    {
        // INVARIANT: Concurrent recordings should not cause exceptions or corruption
        var recorder = new LatencyRecorder(recordLatencies: true, maxSamples: 10000);
        const int threadCount = 5;
        const int recordsPerThread = 1000;
        
        var tasks = Enumerable.Range(0, threadCount)
            .Select(threadId => Task.Run(() =>
            {
                for (int i = 0; i < recordsPerThread; i++)
                {
                    // Use thread-specific values to detect potential corruption
                    recorder.Record(threadId * 1000 + i);
                }
            }))
            .ToArray();
        
        // Should complete without exceptions
        await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(5));
        
        // Should be able to calculate percentiles after concurrent recording
        var p50 = recorder.GetPercentile(50);
        var p95 = recorder.GetPercentile(95);
        var p99 = recorder.GetPercentile(99);
        
        p50.Should().BeGreaterThan(0);
        p95.Should().BeGreaterThan(0);
        p99.Should().BeGreaterThan(0);
        
        // Percentiles should still be monotonic
        p50.Should().BeLessOrEqualTo(p95);
        p95.Should().BeLessOrEqualTo(p99);
    }
    
    [Fact]
    public void Reservoir_Sampling_Maintains_Statistical_Properties()
    {
        // INVARIANT: Even with overflow, percentiles should remain statistically reasonable
        // INVARIANT: Reservoir should not exceed maxSamples capacity
        const int maxSamples = 1000;
        const int totalRecords = 50000; // Much more than reservoir capacity
        
        var recorder = new LatencyRecorder(recordLatencies: true, maxSamples);
        
        // Record a large number of values with known distribution (1 to 50000)
        for (int i = 1; i <= totalRecords; i++)
        {
            recorder.Record(i);
        }
        
        var p50 = recorder.GetPercentile(50);
        var p95 = recorder.GetPercentile(95);
        var p99 = recorder.GetPercentile(99);
        
        // For a uniform distribution 1-50000, we expect:
        // - p50 around 25000 (but reservoir sampling will have variance)
        // - p95 around 47500
        // - p99 around 49500
        
        // Values should be within the recorded range
        p50.Should().BeInRange(1, totalRecords);
        p95.Should().BeInRange(1, totalRecords);
        p99.Should().BeInRange(1, totalRecords);
        
        // Should maintain reasonable distribution properties
        // (Allow wide tolerance due to reservoir sampling variance)
        p50.Should().BeInRange(totalRecords * 0.2, totalRecords * 0.8);
        p95.Should().BeInRange(totalRecords * 0.8, totalRecords);
        p99.Should().BeInRange(totalRecords * 0.9, totalRecords);
    }
}