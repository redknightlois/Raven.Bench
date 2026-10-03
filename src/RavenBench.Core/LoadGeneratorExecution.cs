using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using RavenBench.Core.Metrics;
using RavenBench.Core.Reporting;
using RavenBench.Core.Transport;
using RavenBench.Core.Workload;

namespace RavenBench.Core;

public static class LoadGeneratorExecution
{
    /// <summary>The operations the latency figures hold: every completed operation, failed and timed-out ones included. Cancelled operations add no sample.</summary>
    public const string LatencySamples = "succeeded, failed and timed-out operations; cancelled excluded";

    /// <summary>
    /// The one rate definition for the step throughput and the rolling rate: documents carried by succeeded operations, per second.
    /// Failed requests (often rejected fast) add nothing, and a bulk batch counts its documents.
    /// </summary>
    public static double Rate(long recordsCompleted, double seconds) => seconds > 0 ? recordsCompleted / seconds : 0;

    /// <summary>
    /// Optional callback invoked on the first occurrence of each unique error message.
    /// Wire up from BenchmarkRunner to surface errors without requiring --verbose.
    /// </summary>
    public static Action<string>? OnFirstError { get; set; }

    private static int _firstErrorLogged = 0;

    public static void ResetErrorTracking() => Interlocked.Exchange(ref _firstErrorLogged, 0);

    /// <summary>
    /// Executes one operation and records its latency measured from <paramref name="startTimestamp"/>
    /// (a <see cref="Stopwatch.GetTimestamp"/> value). Callers pass the moment the request was *due*:
    /// for closed-loop that is the instant the worker dequeues it; for rate-based load it is the token's
    /// scheduled time, so queue wait under saturation is included rather than coordinately omitted.
    /// When <paramref name="expectedIntervalMicros"/> &gt; 0, HDRHistogram backfills omitted samples.
    /// </summary>
    public static async Task<WorkItemResult> ExecuteOperationAsync(
        IYcsbTransport transport,
        OperationBase operation,
        LatencyRecorder latencyRecorder,
        long startTimestamp,
        long expectedIntervalMicros,
        CancellationToken cancellationToken)
    {
        bool isError = false;
        string? errorDetails = null;
        long bytesOut = 0;
        long bytesIn = 0;
        string? indexName = null;
        int? resultCount = null;
        bool? isStale = null;

        try
        {
            var result = await transport.ExecuteAsync(operation, cancellationToken);
            if (result.Cancelled)
                return new WorkItemResult { Cancelled = true };
            if (result.IsSuccess == false)
            {
                isError = true;
                errorDetails = result.ErrorDetails;
                if (errorDetails != null && Interlocked.Exchange(ref _firstErrorLogged, 1) == 0)
                    OnFirstError?.Invoke(errorDetails);
            }
            else
            {
                bytesOut = result.BytesOut;
                bytesIn = result.BytesIn;
                indexName = result.IndexName;
                resultCount = result.ResultCount;
                isStale = result.IsStale;
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return new WorkItemResult { Cancelled = true };
        }
        catch (Exception ex)
        {
            isError = true;
            errorDetails = ex.Message;
        }

        // Every operation that reaches here adds a sample, failed and timed-out ones included.
        var end = Stopwatch.GetTimestamp();
        var latencyMicros = Math.Max(1, (long)Math.Round((end - startTimestamp) * 1_000_000.0 / Stopwatch.Frequency));
        try
        {
            if (expectedIntervalMicros > 0)
                latencyRecorder.RecordWithExpectedInterval(latencyMicros, expectedIntervalMicros);
            else
                latencyRecorder.Record(latencyMicros);
        }
        catch (InvalidOperationException)
        {
            // Histogram overflow: a latency above the limit ends the step as an error.
            isError = true;
        }

        return new WorkItemResult
        {
            IsError = isError,
            RecordCount = operation.RecordCount,
            ErrorDetails = errorDetails,
            BytesOut = bytesOut,
            BytesIn = bytesIn,
            LatencyMicros = latencyMicros,
            IndexName = indexName,
            ResultCount = resultCount,
            IsStale = isStale
        };
    }

    public static LoadGeneratorMetrics BuildMetrics(
        LoadGeneratorCounters counters,
        TimeSpan duration,
        long scheduledCount,
        bool isWarmup,
        RollingRateStats? rollingRate = null,
        Percentiles? sendLateness = null)
    {
        var completed = counters.OperationsCompleted;
        var errorCount = counters.ErrorCount;
        var errorRate = completed > 0 ? (double)errorCount / completed : 0.0;
        var throughput = Rate(counters.RecordsCompleted, duration.TotalSeconds);
        var bytesOut = counters.BytesOut;
        var bytesIn = counters.BytesIn;

        return new LoadGeneratorMetrics
        {
            Throughput = throughput,
            ErrorRate = errorRate,
            BytesOut = bytesOut,
            BytesIn = bytesIn,
            Duration = duration,
            Reason = isWarmup ? "warmup" : null,
            ScheduledOperations = scheduledCount,
            OperationsCompleted = completed,
            RollingRate = rollingRate,
            SendLateness = sendLateness,
            Query = counters.QuerySnapshot()
        };
    }

    /// <summary>
    /// Fraction (0..1) of a <paramref name="linkMbps"/> link consumed by the combined in+out byte volume.
    /// Lives here, the one place that owns the byte counters, but requires link capacity — a deployment
    /// fact — to be supplied by the caller rather than assumed.
    /// </summary>
    public static double Utilization(long bytesOut, long bytesIn, TimeSpan duration, double linkMbps)
    {
        var linkBps = linkMbps * 1_000_000.0;
        if (duration.TotalSeconds <= 0 || linkBps <= 0)
            return 0;

        var bitsPerSecond = (bytesOut + bytesIn) / duration.TotalSeconds * 8.0;
        return Math.Min(bitsPerSecond / linkBps, 1.0);
    }
}

public readonly struct WorkItemResult
{
    public bool IsError { get; init; }

    /// <summary>
    /// True when the operation was aborted by external cancellation; excluded from all counters.
    /// </summary>
    public bool Cancelled { get; init; }
    public string? ErrorDetails { get; init; }
    public long BytesOut { get; init; }
    public long BytesIn { get; init; }
    public long LatencyMicros { get; init; }

    /// <summary>Documents the operation carried; one for every operation but a bulk batch.</summary>
    public int RecordCount { get; init; }
    public string? IndexName { get; init; }
    public int? ResultCount { get; init; }
    public bool? IsStale { get; init; }
}

public sealed class LoadGeneratorCounters
{
    private readonly QueryStats _query = new();
    private long _operations;
    private long _records;
    private long _errors;
    private long _bytesOut;
    private long _bytesIn;

    public void Record(in WorkItemResult result)
    {
        if (result.Cancelled)
            return;

        Interlocked.Increment(ref _operations);
        if (result.IsError)
        {
            Interlocked.Increment(ref _errors);
            return;
        }

        Interlocked.Add(ref _records, result.RecordCount);

        if (result.BytesOut != 0)
            Interlocked.Add(ref _bytesOut, result.BytesOut);
        if (result.BytesIn != 0)
            Interlocked.Add(ref _bytesIn, result.BytesIn);

        _query.Record(result.IndexName, result.ResultCount, result.IsStale);
    }

    public long OperationsCompleted => Volatile.Read(ref _operations);

    /// <summary>The unit of every throughput figure, the step throughput and the rolling rate alike.</summary>
    public const string ThroughputUnit = "documents/s of succeeded operations";

    /// <summary>Documents written or read by the operations that succeeded: the count every throughput figure divides by its window.</summary>
    public long RecordsCompleted => Volatile.Read(ref _records);
    public long ErrorCount => Volatile.Read(ref _errors);
    public long BytesOut => Volatile.Read(ref _bytesOut);
    public long BytesIn => Volatile.Read(ref _bytesIn);
    public QueryStatsSnapshot QuerySnapshot() => _query.Snapshot();
}
