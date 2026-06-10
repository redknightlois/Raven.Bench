using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using RavenBench.Core.Metrics;
using RavenBench.Core.Transport;
using RavenBench.Core.Workload;

namespace RavenBench.Core;

public static class LoadGeneratorExecution
{
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
        ITransport transport,
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
        long latencyMicros = 0;
        string? indexName = null;
        int? resultCount = null;
        bool? isStale = null;

        bool cancelled = false;

        try
        {
            var result = await transport.ExecuteAsync(operation, cancellationToken);
            if (result.Cancelled)
            {
                cancelled = true;
                return new WorkItemResult { Cancelled = true };
            }
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
        catch (Exception ex)
        {
            isError = true;
            errorDetails = ex.Message;
        }
        finally
        {
            var end = Stopwatch.GetTimestamp();
            latencyMicros = Math.Max(1, (long)Math.Round((end - startTimestamp) * 1_000_000.0 / Stopwatch.Frequency));
            if (cancelled == false && isError == false)
            {
                try
                {
                    if (expectedIntervalMicros > 0)
                        latencyRecorder.RecordWithExpectedInterval(latencyMicros, expectedIntervalMicros);
                    else
                        latencyRecorder.Record(latencyMicros);
                }
                catch (InvalidOperationException)
                {
                    // Histogram overflow - latency exceeded 60s. This indicates extreme server degradation.
                    // Treat as error to trigger early termination of the benchmark step.
                    isError = true;
                }
            }
        }

        return new WorkItemResult
        {
            IsError = isError,
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
        RollingRateStats? rollingRate = null)
    {
        var completed = counters.OperationsCompleted;
        var errorCount = counters.ErrorCount;
        var errorRate = completed > 0 ? (double)errorCount / completed : 0.0;
        // Goodput: successful operations only. Failed requests (often rejected fast) must not inflate throughput.
        var throughput = duration.TotalSeconds > 0
            ? (completed - errorCount) / duration.TotalSeconds
            : 0;
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
    public string? IndexName { get; init; }
    public int? ResultCount { get; init; }
    public bool? IsStale { get; init; }
}

public sealed class LoadGeneratorCounters
{
    private readonly QueryStats _query = new();
    private long _operations;
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

        if (result.BytesOut != 0)
            Interlocked.Add(ref _bytesOut, result.BytesOut);
        if (result.BytesIn != 0)
            Interlocked.Add(ref _bytesIn, result.BytesIn);

        _query.Record(result.IndexName, result.ResultCount, result.IsStale);
    }

    public long OperationsCompleted => Volatile.Read(ref _operations);
    public long ErrorCount => Volatile.Read(ref _errors);
    public long BytesOut => Volatile.Read(ref _bytesOut);
    public long BytesIn => Volatile.Read(ref _bytesIn);
    public QueryStatsSnapshot QuerySnapshot() => _query.Snapshot();
}
