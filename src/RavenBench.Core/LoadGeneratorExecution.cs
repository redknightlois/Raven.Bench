using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using RavenBench.Core.Metrics;
using RavenBench.Core.Transport;
using RavenBench.Core.Workload;

namespace RavenBench.Core;

internal static class LoadGeneratorExecution
{
    public static async Task<WorkItemResult> ExecuteOperationAsync(
        ITransport transport,
        OperationBase operation,
        LatencyRecorder latencyRecorder,
        long baselineLatencyMicros,
        CancellationToken cancellationToken)
    {
        var start = Stopwatch.GetTimestamp();
        bool isError = false;
        long bytesOut = 0;
        long bytesIn = 0;
        long latencyMicros = 0;

        try
        {
            var result = await transport.ExecuteAsync(operation, cancellationToken);
            bytesOut = result.BytesOut;
            bytesIn = result.BytesIn;
        }
        catch (Exception)
        {
            isError = true;
        }
        finally
        {
            var end = Stopwatch.GetTimestamp();
            latencyMicros = Math.Max(1, (long)Math.Round((end - start) * 1_000_000.0 / Stopwatch.Frequency));
            if (isError == false)
            {
                if (baselineLatencyMicros > 0)
                    latencyRecorder.RecordWithExpectedInterval(latencyMicros, baselineLatencyMicros);
                else
                    latencyRecorder.Record(latencyMicros);
            }
        }

        return new WorkItemResult
        {
            IsError = isError,
            BytesOut = bytesOut,
            BytesIn = bytesIn,
            LatencyMicros = latencyMicros
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
        var throughput = duration.TotalSeconds > 0
            ? completed / duration.TotalSeconds
            : 0;
        var errorCount = counters.ErrorCount;
        var errorRate = completed > 0 ? (double)errorCount / completed : 0.0;
        var bytesOut = counters.BytesOut;
        var bytesIn = counters.BytesIn;

        return new LoadGeneratorMetrics
        {
            Throughput = throughput,
            ErrorRate = errorRate,
            BytesOut = bytesOut,
            BytesIn = bytesIn,
            NetworkUtilization = CalculateNetworkUtilization(bytesOut, bytesIn, duration),
            Reason = isWarmup ? "warmup" : null,
            ScheduledOperations = scheduledCount,
            OperationsCompleted = completed,
            RollingRate = rollingRate
        };
    }

    public static double CalculateNetworkUtilization(long bytesOut, long bytesIn, TimeSpan duration)
    {
        if (duration.TotalSeconds <= 0)
            return 0;

        var totalBytes = bytesOut + bytesIn;
        var bytesPerSecond = totalBytes / duration.TotalSeconds;
        var bitsPerSecond = bytesPerSecond * 8;

        return Math.Min(bitsPerSecond / (1_000_000_000.0 / 100), 100.0);
    }
}

internal readonly struct WorkItemResult
{
    public bool IsError { get; init; }
    public long BytesOut { get; init; }
    public long BytesIn { get; init; }
    public long LatencyMicros { get; init; }
}

internal sealed class LoadGeneratorCounters
{
    private long _operations;
    private long _errors;
    private long _bytesOut;
    private long _bytesIn;

    public void Record(in WorkItemResult result)
    {
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
    }

    public long OperationsCompleted => Volatile.Read(ref _operations);
    public long ErrorCount => Volatile.Read(ref _errors);
    public long BytesOut => Volatile.Read(ref _bytesOut);
    public long BytesIn => Volatile.Read(ref _bytesIn);
}
