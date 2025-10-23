using System;
using System.Threading;
using HdrHistogram;

namespace RavenBench.Core.Metrics;

/// <summary>
/// HDRHistogram-based latency recorder that corrects for coordinated omission.
///
/// Coordinated omission occurs in closed-loop benchmarks when a slow response delays
/// subsequent requests. The worker waits on the slow response before issuing the next request,
/// causing "should-have-been" samples to be omitted from the latency distribution.
///
/// This recorder uses HdrHistogram.Recorder with expected interval correction to backfill
/// missing samples, providing accurate tail latency measurements.
/// </summary>
/// <remarks>
/// Based on Gil Tene's "How NOT to Measure Latency" methodology.
/// See: https://www.youtube.com/watch?v=lJ8ydIuPFeU
/// </remarks>
public sealed class LatencyRecorder : IDisposable
{
    private readonly bool _enabled;
    private readonly Recorder _recorder;
    private long _maxMicros;

    /// <summary>
    /// Creates a new latency recorder.
    /// </summary>
    /// <param name="recordLatencies">Whether to actually record latencies. When false, all operations are no-ops.</param>
    /// <param name="maxSamples">Unused. Kept for backward compatibility but ignored since HDRHistogram manages its own storage.</param>
    public LatencyRecorder(bool recordLatencies, int maxSamples = 100_000)
    {
        _enabled = recordLatencies;

        if (_enabled)
        {
            // Configure HDRHistogram:
            // - lowestDiscernibleValue: 1 µs (minimum measurable latency)
            // - highestTrackableValue: 60 seconds in microseconds (1 minute max latency)
            // - significantDigits: 3 (0.1% precision across the range)
            //
            // This configuration covers latencies from 1µs to 60s with high precision.
            // If a latency exceeds 60s, the histogram will throw an exception (fail-fast).
            _recorder = new Recorder(
                lowestDiscernibleValue: 1,
                highestTrackableValue: 60_000_000,  // 60 seconds in microseconds
                numberOfSignificantValueDigits: 3,
                histogramFactory: (instanceId, low, high, digits) => new LongHistogram(low, high, digits));

            _maxMicros = 0;
        }
        else
        {
            _recorder = null!;  // Not needed when disabled
        }
    }

    /// <summary>
    /// Records a latency measurement without coordinated omission correction.
    /// Use this for initial measurements or when coordinated omission is not a concern.
    /// </summary>
    /// <param name="micros">Observed latency in microseconds.</param>
    public void Record(long micros)
    {
        if (_enabled == false) return;

        try
        {
            _recorder.RecordValue(micros);

            // Track maximum latency using lock-free atomic operations
            long currentMax;
            do
            {
                currentMax = Volatile.Read(ref _maxMicros);
                if (micros <= currentMax)
                    break;
            } while (Interlocked.CompareExchange(ref _maxMicros, micros, currentMax) != currentMax);
        }
        catch (IndexOutOfRangeException ex)
        {
            // Histogram range exceeded - this indicates a configuration issue
            throw new InvalidOperationException(
                $"Latency value {micros}µs exceeds histogram range (max: 60,000,000µs = 60s). " +
                "This suggests either an extreme latency event or a measurement error. " +
                "Consider increasing highestTrackableValue if latencies > 60s are expected.",
                ex);
        }
    }

    /// <summary>
    /// Records a latency measurement with coordinated omission correction.
    ///
    /// When a response is delayed, this method backfills the histogram with samples
    /// at the expected interval to account for requests that "should have been" issued
    /// during the stall.
    /// </summary>
    /// <param name="observedMicros">Actual observed latency in microseconds.</param>
    /// <param name="expectedIntervalMicros">Expected interval between requests in microseconds.
    /// This represents the baseline inter-request time. When observedMicros >> expectedIntervalMicros,
    /// coordinated omission correction adds synthetic samples to fill the gap.</param>
    public void RecordWithExpectedInterval(long observedMicros, long expectedIntervalMicros)
    {
        if (_enabled == false) return;

        try
        {
            if (expectedIntervalMicros > 0)
            {
                // Use HDRHistogram's built-in coordinated omission correction
                // This will add synthetic samples at the expected interval
                _recorder.RecordValueWithExpectedInterval(observedMicros, expectedIntervalMicros);
            }
            else
            {
                // No expected interval provided - record without correction
                _recorder.RecordValue(observedMicros);
            }

            // Track maximum latency (only from actual observed values, not synthetic)
            long currentMax;
            do
            {
                currentMax = Volatile.Read(ref _maxMicros);
                if (observedMicros <= currentMax)
                    break;
            } while (Interlocked.CompareExchange(ref _maxMicros, observedMicros, currentMax) != currentMax);
        }
        catch (IndexOutOfRangeException ex)
        {
            throw new InvalidOperationException(
                $"Latency value {observedMicros}µs exceeds histogram range (max: 60,000,000µs = 60s). " +
                "This suggests either an extreme latency event or a measurement error. " +
                "Consider increasing highestTrackableValue if latencies > 60s are expected.",
                ex);
        }
    }

    /// <summary>
    /// Takes a snapshot of the current histogram state and resets the recorder.
    /// This enables per-step histogram collection in a multi-step benchmark.
    /// </summary>
    /// <returns>A snapshot containing the interval histogram and max latency.</returns>
    public HistogramSnapshot Snapshot()
    {
        if (_enabled == false)
            return new HistogramSnapshot(null, 0);

        // GetIntervalHistogram returns the histogram since last call and resets the recorder
        var histogram = _recorder.GetIntervalHistogram();
        var maxMicros = Volatile.Read(ref _maxMicros);

        // Reset max for next interval
        Interlocked.Exchange(ref _maxMicros, 0);

        return new HistogramSnapshot(histogram, maxMicros);
    }

    public void Dispose()
    {
        // Recorder doesn't require explicit disposal, but follow IDisposable pattern
        // for future-proofing and resource management consistency
    }
}

/// <summary>
/// Represents a point-in-time snapshot of histogram data.
/// </summary>
public sealed class HistogramSnapshot
{
    private readonly HistogramBase? _histogram;

    /// <summary>
    /// Maximum observed latency in this interval, in microseconds.
    /// </summary>
    public long MaxMicros { get; }

    /// <summary>
    /// Total number of samples recorded (including coordinated omission corrections).
    /// </summary>
    public long TotalCount => _histogram?.TotalCount ?? 0;

    internal HistogramSnapshot(HistogramBase? histogram, long maxMicros)
    {
        _histogram = histogram;
        MaxMicros = maxMicros;
    }

    /// <summary>
    /// Gets a percentile value from the snapshot.
    /// </summary>
    /// <param name="percentile">Percentile to retrieve (0.0-100.0).</param>
    /// <returns>Latency value in microseconds at the given percentile.</returns>
    public double GetPercentile(double percentile)
    {
        if (_histogram == null) return 0;
        if (_histogram.TotalCount == 0) return 0;

        return _histogram.GetValueAtPercentile(percentile);
    }

    /// <summary>
    /// Gets the underlying histogram for advanced operations.
    /// Returns null if recording was disabled.
    /// </summary>
    public HistogramBase? GetHistogram() => _histogram;
}
