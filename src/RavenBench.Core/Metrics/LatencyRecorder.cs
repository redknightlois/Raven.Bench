using System;
using System.Threading;
using HdrHistogram;

namespace RavenBench.Core.Metrics;

/// <summary>
/// HDRHistogram-based latency recorder holding one sample per recorded operation and no synthetic samples.
/// A closed-loop step measures the service time each worker observes; a rate step measures from each
/// arrival's due time, which is where coordinated omission is accounted for.
/// </summary>
public sealed class LatencyRecorder : IDisposable
{
    public const long MaxTrackableMicros = 3_600_000_000;

    private readonly bool _enabled;
    private readonly Recorder _recorder;
    private long _maxMicros;

    /// <summary>
    /// Creates a new latency recorder.
    /// </summary>
    /// <param name="recordLatencies">Whether to actually record latencies. When false, all operations are no-ops.</param>
    public LatencyRecorder(bool recordLatencies)
    {
        _enabled = recordLatencies;

        if (_enabled)
        {
            // Configure HDRHistogram:
            // - lowestDiscernibleValue: 1 µs (minimum measurable latency)
            // - highestTrackableValue: 1 hour in microseconds; a rate-mode request is charged from its due time, so a backlog on a slow engine is legitimately minutes late
            // - significantDigits: 3 (0.1% precision across the range)
            //
            // A latency beyond 1 hour is a measurement error and throws (fail-fast).
            _recorder = new Recorder(
                lowestDiscernibleValue: 1,
                highestTrackableValue: MaxTrackableMicros,
                numberOfSignificantValueDigits: 3,
                // Every worker thread records concurrently, so the histogram must be thread-safe.
                histogramFactory: (instanceId, low, high, digits) => new LongConcurrentHistogram(low, high, digits));

            _maxMicros = 0;
        }
        else
        {
            _recorder = null!;  // Not needed when disabled
        }
    }

    /// <summary>
    /// Records a latency measurement.
    /// </summary>
    /// <param name="micros">Observed latency in microseconds.</param>
    public void Record(long micros)
    {
        if (_enabled == false) return;

        try
        {
            _recorder.RecordValue(micros);

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
                RangeMessage(micros),
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
        var maxMicros = Interlocked.Exchange(ref _maxMicros, 0);

        return new HistogramSnapshot(histogram, maxMicros);
    }

    private static string RangeMessage(long micros) =>
        $"Latency value {micros}µs exceeds histogram range (max: {MaxTrackableMicros:N0}µs = 1h). " +
        "This suggests a measurement error rather than a real latency.";

    public void Dispose()
    {
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
    /// Total number of samples recorded.
    /// </summary>
    public long TotalCount => _histogram?.TotalCount ?? 0;

    /// <summary>Mean recorded latency in microseconds; zero when empty.</summary>
    public double MeanMicros => _histogram is { TotalCount: > 0 } h ? h.GetMean() : 0;

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
