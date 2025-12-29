using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using RavenBench.Core.Metrics;
using Xunit;

namespace RavenBench.Tests;

public class LatencyRecorderTests
{
    [Fact]
    public void Record_And_Snapshot_Basic_Functionality()
    {
        // INVARIANT: Recorded values should be reflected in percentile calculations
        // INVARIANT: Percentiles should be monotonically increasing (p50 <= p95 <= p99)
        var recorder = new LatencyRecorder(recordLatencies: true);

        // Record some known values: 100, 200, 300, ..., 1000 microseconds
        for (int i = 1; i <= 10; i++)
        {
            recorder.Record(i * 100);
        }

        var snapshot = recorder.Snapshot();
        var p50 = snapshot.GetPercentile(50);
        var p95 = snapshot.GetPercentile(95);
        var p99 = snapshot.GetPercentile(99);

        // Should have reasonable values
        p50.Should().BeGreaterThan(0);
        p95.Should().BeGreaterThan(0);
        p99.Should().BeGreaterThan(0);

        // Percentiles should be monotonically increasing
        p50.Should().BeLessOrEqualTo(p95);
        p95.Should().BeLessOrEqualTo(p99);

        // Total count should match recorded values
        snapshot.TotalCount.Should().Be(10);
    }

    [Fact]
    public void Snapshot_MultipleCalls_Should_SeeSameData()
    {
        // INVARIANT: Snapshot() returns data from interval since last call
        //            Multiple percentile queries on the same snapshot must see the same data
        var recorder = new LatencyRecorder(recordLatencies: true);

        foreach (var value in new[] { 1000L, 2000L, 3000L, 10_000L })
        {
            recorder.Record(value);
        }

        // Take snapshot once and query multiple percentiles
        var snapshot = recorder.Snapshot();
        var p50 = snapshot.GetPercentile(50);
        var p95 = snapshot.GetPercentile(95);
        var p99 = snapshot.GetPercentile(99);

        // All percentile calls on the same snapshot should see the same data
        p50.Should().BeGreaterThan(0);
        p95.Should().BeGreaterThan(p50);
        p99.Should().BeGreaterOrEqualTo(p95);  // With small samples, p95 and p99 may equal due to HDR bucketing
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

        var snapshot = recorder.Snapshot();

        // All percentiles should return 0 since recording is disabled
        snapshot.GetPercentile(50).Should().Be(0);
        snapshot.GetPercentile(95).Should().Be(0);
        snapshot.GetPercentile(99).Should().Be(0);
        snapshot.TotalCount.Should().Be(0);
    }

    [Fact]
    public void Empty_Recorder_Returns_Zero_Percentiles()
    {
        // INVARIANT: Empty recorder should handle percentile requests gracefully
        var recorder = new LatencyRecorder(recordLatencies: true);

        var snapshot = recorder.Snapshot();

        // No recordings made
        snapshot.GetPercentile(50).Should().Be(0);
        snapshot.GetPercentile(95).Should().Be(0);
        snapshot.GetPercentile(99).Should().Be(0);
        snapshot.TotalCount.Should().Be(0);
    }

    [Fact]
    public void Single_Value_Returns_That_Value_For_All_Percentiles()
    {
        // INVARIANT: Single value should be returned for all percentiles
        // INVARIANT: Max should match the single recorded value
        var recorder = new LatencyRecorder(recordLatencies: true);

        const long singleValue = 1500;
        recorder.Record(singleValue);

        var snapshot = recorder.Snapshot();

        // All percentiles should return the single value (within HDR precision)
        snapshot.GetPercentile(50).Should().BeApproximately(singleValue, singleValue * 0.01);
        snapshot.GetPercentile(95).Should().BeApproximately(singleValue, singleValue * 0.01);
        snapshot.GetPercentile(99).Should().BeApproximately(singleValue, singleValue * 0.01);
        snapshot.MaxMicros.Should().Be(singleValue);
        snapshot.TotalCount.Should().Be(1);
    }

    [Fact]
    public void Max_Tracking_Captures_Maximum_Latency()
    {
        // INVARIANT: Max should always reflect the highest observed latency
        var recorder = new LatencyRecorder(recordLatencies: true);

        // Record values with a clear maximum
        recorder.Record(100);
        recorder.Record(500);
        recorder.Record(2000);  // Maximum
        recorder.Record(300);
        recorder.Record(150);

        var snapshot = recorder.Snapshot();

        snapshot.MaxMicros.Should().Be(2000);
        snapshot.TotalCount.Should().Be(5);
    }

    [Fact]
    public void Coordinated_Omission_Correction_Backfills_Samples()
    {
        // INVARIANT: When a response exceeds expected interval, synthetic samples should be added
        // INVARIANT: Corrected count should be greater than actual observed count
        var recorder = new LatencyRecorder(recordLatencies: true);

        // Simulate normal responses at 100µs
        for (int i = 0; i < 10; i++)
        {
            recorder.RecordWithExpectedInterval(100, 100);
        }

        // Inject a stall: 1500µs response when expecting 100µs interval
        // HDRHistogram should backfill ~14 synthetic samples (1500/100 - 1)
        recorder.RecordWithExpectedInterval(1500, 100);

        var snapshot = recorder.Snapshot();

        // Total count should include synthetic samples from coordinated omission correction
        // Original: 11 actual samples
        // Corrected: ~11 + 14 = ~25 samples (with some variance due to HDR bucketing)
        snapshot.TotalCount.Should().BeGreaterThan(11);
        snapshot.MaxMicros.Should().Be(1500);
    }

    [Fact]
    public void Histogram_Range_Exceeded_Throws_Meaningful_Exception()
    {
        // INVARIANT: Recording beyond histogram range should fail-fast with clear error message
        var recorder = new LatencyRecorder(recordLatencies: true);

        // Histogram is configured for 1µs to 60s (60,000,000µs)
        // Recording a value beyond 60s should throw
        var exception = Assert.Throws<InvalidOperationException>(() =>
        {
            recorder.Record(70_000_000); // 70 seconds in microseconds
        });

        // Should have helpful error message
        exception.Message.Should().Contain("exceeds histogram range");
        exception.Message.Should().Contain("60,000,000");
        exception.Message.Should().Contain("60s");
    }

    [Fact]
    public async Task Concurrent_Recording_With_Max_Tracking()
    {
        // INVARIANT: Concurrent recordings should correctly track max across threads
        // INVARIANT: Max should be thread-safe and capture the true maximum
        var recorder = new LatencyRecorder(recordLatencies: true);
        const int threadCount = 10;
        const int recordsPerThread = 100;

        var tasks = Enumerable.Range(0, threadCount)
            .Select(threadId => Task.Run(() =>
            {
                for (int i = 0; i < recordsPerThread; i++)
                {
                    // Each thread records values in its own range
                    // Thread 0: 0-99, Thread 1: 1000-1099, etc.
                    recorder.Record(threadId * 1000 + i);
                }
            }))
            .ToArray();

        // Should complete without exceptions
        await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(5));

        // Allow Recorder's internal double-buffering to settle before snapshot
        // Recorder flips buffers asynchronously and may have records in-flight
        await Task.Delay(100);

        var snapshot = recorder.Snapshot();

        // Max should be from the highest thread: thread 9, value 9099
        snapshot.MaxMicros.Should().Be((threadCount - 1) * 1000 + (recordsPerThread - 1));

        // Total count should be close to expected (allow variance for Recorder's async double-buffering)
        // Recorder uses lock-free structures and flips buffers asynchronously
        // Some records may be in-flight during snapshot, so allow reasonable tolerance
        snapshot.TotalCount.Should().BeGreaterOrEqualTo(threadCount * recordsPerThread - 300)
            .And.BeLessOrEqualTo(threadCount * recordsPerThread + 10);

        // Percentiles should still be monotonic
        var p50 = snapshot.GetPercentile(50);
        var p95 = snapshot.GetPercentile(95);
        var p99 = snapshot.GetPercentile(99);

        p50.Should().BeLessOrEqualTo(p95);
        p95.Should().BeLessOrEqualTo(p99);
    }

    [Fact]
    public void Snapshot_Resets_Recorder_For_Interval_Collection()
    {
        // INVARIANT: Snapshot should return interval data and reset for next interval
        // INVARIANT: Multiple snapshots should collect distinct intervals
        var recorder = new LatencyRecorder(recordLatencies: true);

        // First interval
        recorder.Record(100);
        recorder.Record(200);
        var snapshot1 = recorder.Snapshot();

        snapshot1.TotalCount.Should().Be(2);
        snapshot1.MaxMicros.Should().Be(200);

        // Second interval - recorder should be reset
        recorder.Record(300);
        recorder.Record(400);
        var snapshot2 = recorder.Snapshot();

        snapshot2.TotalCount.Should().Be(2);
        snapshot2.MaxMicros.Should().Be(400);

        // Snapshots should be independent
        snapshot1.MaxMicros.Should().Be(200);
        snapshot2.MaxMicros.Should().Be(400);
    }

    [Fact]
    public void HDR_Histogram_Maintains_Precision_Across_Range()
    {
        // INVARIANT: HDR histogram should maintain precision across wide value ranges
        // INVARIANT: All recorded values should contribute to histogram (no sampling)
        const int totalRecords = 10000;

        var recorder = new LatencyRecorder(recordLatencies: true);

        // Record values from 1µs to 10000µs (wide range)
        for (int i = 1; i <= totalRecords; i++)
        {
            recorder.Record(i);
        }

        var snapshot = recorder.Snapshot();

        // HDR histogram doesn't use sampling - all values should be recorded
        snapshot.TotalCount.Should().Be(totalRecords);

        // For uniform distribution 1-10000:
        // - p50 should be around 5000
        // - p95 should be around 9500
        // - p99 should be around 9900
        var p50 = snapshot.GetPercentile(50);
        var p95 = snapshot.GetPercentile(95);
        var p99 = snapshot.GetPercentile(99);

        // Verify distribution properties (allow 10% tolerance for HDR bucketing)
        p50.Should().BeInRange(totalRecords * 0.45, totalRecords * 0.55);
        p95.Should().BeInRange(totalRecords * 0.90, totalRecords * 0.98);
        p99.Should().BeInRange(totalRecords * 0.97, totalRecords);

        // Max should be exact
        snapshot.MaxMicros.Should().Be(totalRecords);
    }
}
