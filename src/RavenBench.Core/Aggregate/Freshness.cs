using System.Collections.Concurrent;
using System.Diagnostics;
using RavenBench.Core.Workload;

namespace RavenBench.Core.Aggregate;

/// <summary>A distribution in milliseconds; the percentiles are null when it holds no value.</summary>
public sealed record MillisecondDistribution(long Count, double? P50, double? P90, double? P99, double? Max)
{
    /// <summary>Nearest-rank percentiles over the values.</summary>
    public static MillisecondDistribution Of(IReadOnlyCollection<double> values)
    {
        var sorted = values.Order().ToArray();
        double? Rank(double p) => sorted.Length == 0 ? null : sorted[Math.Max(0, (int)Math.Ceiling(p * sorted.Length) - 1)];
        return new MillisecondDistribution(sorted.Length, Rank(0.50), Rank(0.90), Rank(0.99), sorted.Length == 0 ? null : sorted[^1]);
    }
}

/// <summary>Freshness over one run: observed writes as a distribution, and the writes no answer reflected.</summary>
/// <remarks><see cref="AnswersWithTrackedGroupNotFirst"/> counts the answers whose first group was not the tracked group; the under-write load keeps it zero.</remarks>
public sealed record FreshnessInfo(string Definition, string TrackedGroup, long Baseline, long Writes, long Observed, long Unobserved, MillisecondDistribution Distribution, long AnswersWithTrackedGroupNotFirst);

/// <summary>
/// Measures freshness from the answers alone, the same way for every product. Every tracked write
/// moves one document into the tracked group and nothing else ever adds to it, so after the k-th
/// acknowledged write the group's value is the baseline plus k, and an answer whose value for the
/// group is at least baseline plus k reflects write k. The freshness of write k is the time from its
/// acknowledgement to the first answer received at or after it that reflects it; an answer received
/// before the acknowledgement never counts. Times are <see cref="Stopwatch.GetTimestamp"/> values,
/// the clock the fixed-rate runner schedules with. Thread-safe.
/// </summary>
public sealed class FreshnessTracker(string trackedGroup, long baseline)
{
    public const string Definition = "time from a write's acknowledgement to the first query answer received at or after it whose value for the tracked group includes it; a write no answer reflected before the run ended is unobserved";

    public string TrackedGroup { get; } = trackedGroup;

    private readonly object _lock = new();
    private readonly List<long> _acknowledged = [];
    private readonly List<(long Received, long Reflected)> _answers = [];
    private long _notFirst;

    /// <summary>Records the acknowledgement of the next write, in acknowledgement order.</summary>
    public void Acknowledged(long timestamp)
    {
        lock (_lock)
            _acknowledged.Add(timestamp);
    }

    /// <summary>Records one answer; an answer that does not carry the tracked group reflects nothing.</summary>
    public void Answered(IReadOnlyList<AggregateGroup> groups, long receivedTimestamp)
    {
        if (groups.Count == 0 || AggregateOrdering.KeyComparer.Equals(groups[0].Key, trackedGroup) == false)
            Interlocked.Increment(ref _notFirst);
        foreach (var g in groups)
        {
            if (AggregateOrdering.KeyComparer.Equals(g.Key, trackedGroup) == false)
                continue;
            lock (_lock)
                _answers.Add((receivedTimestamp, g.Value - baseline));
            return;
        }
    }

    public FreshnessInfo Complete()
    {
        long[] acks;
        (long Received, long Reflected)[] answers;
        lock (_lock)
        {
            acks = _acknowledged.ToArray();
            answers = _answers.OrderBy(a => a.Received).ToArray();
        }

        var freshness = new List<double>(acks.Length);
        int i = 0;
        for (int k = 0; k < acks.Length; k++)
        {
            // Both thresholds rise with k, so an answer that fails write k fails every later write.
            while (i < answers.Length && (answers[i].Received < acks[k] || answers[i].Reflected < k + 1))
                i++;
            if (i == answers.Length)
                break;
            freshness.Add((answers[i].Received - acks[k]) * 1000.0 / Stopwatch.Frequency);
        }
        return new FreshnessInfo(Definition, trackedGroup, baseline, acks.Length, freshness.Count, acks.Length - freshness.Count, MillisecondDistribution.Of(freshness), Interlocked.Read(ref _notFirst));
    }
}

/// <summary>
/// What a paced writer held: the requested rate, the acknowledged writes over the writer's own measured duration, and why it stopped.
/// <see cref="Shortfall"/> is null when the writer held the requested rate, and names by how much and why it fell short otherwise.
/// </summary>
public sealed record HeldWriteRate(double RequestedPerSecond, double HeldPerSecond, long Acknowledged, double WriterSeconds, bool HeldRequested, string StopReason, MillisecondDistribution WriteLatency)
{
    /// <summary>The most writes the writer had in flight at once.</summary>
    public int Writers { get; init; }

    public string? Shortfall { get; init; }

    /// <summary>The share of the requested rate a writer must reach to be reported as having held it.</summary>
    public const double HeldTolerance = 0.95;

    public static HeldWriteRate Of(double requestedPerSecond, int writers, long acknowledged, double seconds, string stopReason, IReadOnlyCollection<double> latenciesMs)
    {
        var held = acknowledged / seconds;
        var latency = MillisecondDistribution.Of(latenciesMs);
        bool heldRequested = held >= requestedPerSecond * HeldTolerance;
        var meanMs = latenciesMs.Count == 0 ? 0 : latenciesMs.Average();
        string? shortfall = heldRequested ? null
            : $"held {held:F0} of {requestedPerSecond:F0} updates/s ({held / requestedPerSecond:P0}): {stopReason}; {writers} writer(s) in flight at a mean write latency of {meanMs:F1} ms hold at most {(meanMs > 0 ? writers * 1000 / meanMs : double.PositiveInfinity):F0} updates/s";
        return new HeldWriteRate(requestedPerSecond, held, acknowledged, seconds, heldRequested, stopReason, latency) { Writers = writers, Shortfall = shortfall };
    }
}

/// <summary>
/// Sends writes on a fixed schedule, with at most <c>writers</c> writes in flight. A write that falls
/// behind its slot is sent as soon as a writer is free; no slot is skipped. With one writer the n-th
/// acknowledgement is always the n-th write. The first write is always sent and awaited to its
/// acknowledgement, even when the stop comes first. A write that throws stops every writer and the
/// exception propagates; only acknowledged writes count toward the held rate.
/// </summary>
public static class PacedWriter
{
    /// <param name="writers">The number of writers, and so the most writes in flight at once.</param>
    /// <param name="write">Sends the next write of the given writer and returns when it is acknowledged; it returns false when no write is left to send, which stops every writer.</param>
    /// <param name="acknowledged">Receives the acknowledgement timestamp of each write.</param>
    public static async Task<HeldWriteRate> RunAsync(double ratePerSecond, int writers, Func<int, CancellationToken, Task<bool>> write, Action<long> acknowledged, CancellationToken stop)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(writers, 1);
        using var halt = CancellationTokenSource.CreateLinkedTokenSource(stop);
        var latencies = new ConcurrentQueue<double>();
        var clock = Stopwatch.StartNew();
        string reason = "the run ended";
        long next = -1, acked = 0;

        async Task Writer(int writer)
        {
            while (true)
            {
                long n = Interlocked.Increment(ref next);
                if (n > 0 && halt.IsCancellationRequested)
                    return;
                var due = TimeSpan.FromSeconds(n / ratePerSecond) - clock.Elapsed;
                if (due > TimeSpan.Zero)
                    await Task.Delay(due, halt.Token).ContinueWith(_ => { }, TaskScheduler.Default);
                if (n > 0 && halt.IsCancellationRequested)
                    return;
                long sent = Stopwatch.GetTimestamp();
                bool more;
                try
                {
                    more = await write(writer, n == 0 ? CancellationToken.None : halt.Token);
                }
                catch (OperationCanceledException) when (n > 0 && halt.IsCancellationRequested)
                {
                    return;
                }
                catch
                {
                    halt.Cancel();
                    throw;
                }
                if (more == false)
                {
                    reason = "no document was left to update";
                    halt.Cancel();
                    return;
                }
                long ack = Stopwatch.GetTimestamp();
                acknowledged(ack);
                latencies.Enqueue((ack - sent) * 1000.0 / Stopwatch.Frequency);
                Interlocked.Increment(ref acked);
            }
        }

        await Task.WhenAll(Enumerable.Range(0, writers).Select(w => Task.Run(() => Writer(w), CancellationToken.None)));
        clock.Stop();
        return HeldWriteRate.Of(ratePerSecond, writers, acked, clock.Elapsed.TotalSeconds, reason, latencies);
    }
}
