using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using RavenBench.Core.Metrics;
using RavenBench.Core.Reporting;
using RavenBench.Core.Transport;
using RavenBench.Core.Workload;

namespace RavenBench.Core
{
    /// <summary>
    /// Load generator that maintains a target requests per second (RPS) rate.
    /// Uses paced scheduling with bounded concurrency and shared execution helpers.
    /// </summary>
    public sealed class RateLoadGenerator : ILoadGenerator
    {
        private readonly IYcsbTransport _transport;
        private readonly IWorkload _workload;
        private readonly double _targetRps;
        private readonly int _maxConcurrency;
        private readonly Random _rng;

        public int Concurrency => _maxConcurrency;
        public double? TargetThroughput => _targetRps;

        public RateLoadGenerator(
            IYcsbTransport transport,
            IWorkload workload,
            double targetRps,
            int maxConcurrency,
            Random rng)
        {
            _transport = transport ?? throw new ArgumentNullException(nameof(transport));
            _workload = workload ?? throw new ArgumentNullException(nameof(workload));
            _targetRps = targetRps;
            _maxConcurrency = maxConcurrency;
            _rng = rng ?? throw new ArgumentNullException(nameof(rng));
        }

        public async Task ExecuteWarmupAsync(TimeSpan duration, CancellationToken cancellationToken)
        {
            await ExecuteAsync(duration, _targetRps, isWarmup: true, cancellationToken);
        }

        public async Task<(LatencyRecorder latencyRecorder, LoadGeneratorMetrics metrics)> ExecuteMeasurementAsync(
            TimeSpan duration, CancellationToken cancellationToken)
        {
            return await ExecuteAsync(duration, _targetRps, isWarmup: false, cancellationToken);
        }

        public void SetBaselineLatency(long baselineLatencyMicros)
        {
            // Rate mode derives its expected schedule from the target rate, not from a measured baseline.
        }

        private async Task<(LatencyRecorder latencyRecorder, LoadGeneratorMetrics metrics)> ExecuteAsync(
            TimeSpan duration, double targetRps, bool isWarmup, CancellationToken cancellationToken)
        {
            var latencyRecorder = new LatencyRecorder(isWarmup == false);
            var latenessRecorder = new LatencyRecorder(isWarmup == false);
            var counters = new LoadGeneratorCounters();
            var measurementStopwatch = Stopwatch.StartNew();
            RollingRateStats? rollingStats = null;
            RollingRateSampler? rollingSampler = null;

            await using var scheduler = new TokenBucketScheduler(
                targetRps,
                // Give each worker a few in-flight permits; this keeps pacing predictable while still allowing short spikes.
                burstCapacity: Math.Max(_maxConcurrency * 4, 32),
                cancellationToken);

            var workers = StartWorkers(
                scheduler,
                latencyRecorder,
                latenessRecorder,
                counters,
                cancellationToken);

            if (isWarmup == false && targetRps > 0)
            {
                rollingSampler = new RollingRateSampler(
                    window: TimeSpan.FromSeconds(3),
                    interval: TimeSpan.FromMilliseconds(250));
                rollingSampler.Start(counters, measurementStopwatch, cancellationToken);
            }

            try
            {
                await Task.Delay(duration, cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // Propagate cancellation below after draining outstanding work.
            }
            finally
            {
                await scheduler.StopAsync();
                await Task.WhenAll(workers);

                if (rollingSampler != null)
                {
                    await rollingSampler.DisposeAsync();
                    rollingStats = rollingSampler.Snapshot();
                }
            }

            if (isWarmup == false && scheduler.DroppedTokens > 0)
            {
                Console.Error.WriteLine(
                    $"[Raven.Bench] WARNING: token bucket dropped {scheduler.DroppedTokens} scheduled arrivals at burst capacity; the run is saturated and latency correction is bounded.");
            }

            var metrics = LoadGeneratorExecution.BuildMetrics(
                counters,
                measurementStopwatch.Elapsed,
                scheduler.ScheduledOperations,
                isWarmup,
                rollingStats,
                isWarmup ? null : ToMilliseconds(latenessRecorder.Snapshot()));

            return (latencyRecorder, metrics);
        }

        private Task[] StartWorkers(
            TokenBucketScheduler scheduler,
            LatencyRecorder latencyRecorder,
            LatencyRecorder latenessRecorder,
            LoadGeneratorCounters counters,
            CancellationToken cancellationToken)
        {
            var workers = new Task[_maxConcurrency];
            for (int i = 0; i < _maxConcurrency; i++)
            {
                // Each worker's source is a successive draw from the run-seeded source, in worker
                // order; never seed + workerIndex, which lets two runs share a worker stream.
                // The draws fix the order of operations only: content comes from (seed, id, size).
                var workerRng = new Random(_rng.Next());
                workers[i] = Task.Run(async () =>
                {
                    try
                    {
                        await foreach (var dueTimestamp in scheduler.ConsumeAsync(cancellationToken))
                        {
                            var operation = _workload.NextOperation(workerRng);
                            var lateTicks = Math.Max(0, Stopwatch.GetTimestamp() - dueTimestamp);
                            latenessRecorder.Record(lateTicks * 1_000_000 / Stopwatch.Frequency);

                            // Measure from the token's scheduled time, not pickup: any wait while all
                            // workers were busy is real client-observed latency, not to be omitted.
                            var result = await LoadGeneratorExecution.ExecuteOperationAsync(
                                _transport,
                                operation,
                                latencyRecorder,
                                dueTimestamp,
                                expectedIntervalMicros: 0,
                                cancellationToken);
                            counters.Record(result);
                        }
                    }
                    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                    {
                        // Expected when measurement is cancelled by caller.
                    }
                    catch (TaskCanceledException)
                    {
                        // Channel enumeration can surface TaskCanceledException on shutdown.
                    }
                }, CancellationToken.None);
            }

            return workers;
        }

        private static Percentiles ToMilliseconds(HistogramSnapshot snapshot) => new(
            snapshot.GetPercentile(50) / 1000.0,
            snapshot.GetPercentile(75) / 1000.0,
            snapshot.GetPercentile(90) / 1000.0,
            snapshot.GetPercentile(95) / 1000.0,
            snapshot.GetPercentile(99) / 1000.0,
            snapshot.GetPercentile(99.9) / 1000.0);

        /// <summary>
        /// Token-bucket scheduler that releases work permits at the requested rate. Each permit carries
        /// its scheduled (due) time as a <see cref="Stopwatch.GetTimestamp"/> value on the same clock the
        /// producer paces with. The producer runs on a dedicated thread and sleeps until the next due time,
        /// so a permit leaves at its due time rather than at the producer's next wake-up. Workers consume
        /// permits independently, so when they fall behind the due times sit in the past and the lag
        /// surfaces as latency instead of being lost.
        /// </summary>
        private sealed class TokenBucketScheduler : IAsyncDisposable
        {
            // Bounds one sleep so a stop request is seen promptly at low rates.
            private static readonly long MaxSleepTicks = Stopwatch.Frequency / 20;

            private readonly Channel<long> _tokens;
            private readonly TokenPacer _pacer;
            private readonly CancellationToken _cancellationToken;
            private readonly CancellationTokenSource _producerCts = new();
            private readonly Task _producerTask;
            private int _stopped;

            /// <summary>Tokens released; counts arrivals scheduled, which may exceed completed operations.</summary>
            public long ScheduledOperations => _pacer.ReleasedTokens;

            /// <summary>Scheduled arrivals discarded when the bucket hit burst capacity; non-zero means bounded coordinated omission.</summary>
            public long DroppedTokens => _pacer.DroppedTokens;

            public TokenBucketScheduler(double ratePerSecond, int burstCapacity, CancellationToken cancellationToken)
            {
                _pacer = new TokenPacer(ratePerSecond, burstCapacity, Stopwatch.Frequency);
                _cancellationToken = cancellationToken;

                _tokens = Channel.CreateBounded<long>(new BoundedChannelOptions(burstCapacity)
                {
                    SingleReader = false,
                    SingleWriter = true,
                    FullMode = BoundedChannelFullMode.Wait
                });

                _producerTask = Task.Factory.StartNew(Replenish, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
            }

            public IAsyncEnumerable<long> ConsumeAsync(CancellationToken cancellationToken)
            {
                return _tokens.Reader.ReadAllAsync(cancellationToken);
            }

            /// <summary>Stops the producer. Counters are final once this completes.</summary>
            public async Task StopAsync()
            {
                if (Interlocked.Exchange(ref _stopped, 1) == 1)
                    return;

                _producerCts.Cancel();
                _tokens.Writer.TryComplete();

                try
                {
                    await _producerTask.ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    // Expected when stopping.
                }
            }

            public async ValueTask DisposeAsync()
            {
                await StopAsync().ConfigureAwait(false);
                _producerCts.Dispose();
            }

            private bool Running => _producerCts.IsCancellationRequested == false && _cancellationToken.IsCancellationRequested == false;

            private void Replenish()
            {
                var writer = _tokens.Writer;
                try
                {
                    while (Running)
                    {
                        var (first, count) = _pacer.Release(Stopwatch.GetTimestamp());
                        for (long sequence = first; sequence < first + count; sequence++)
                        {
                            var due = _pacer.DueTicks(sequence);
                            while (writer.TryWrite(due) == false)
                            {
                                // All workers are busy and the burst is queued: block this thread until a slot frees.
                                if (writer.WaitToWriteAsync(_producerCts.Token).AsTask().GetAwaiter().GetResult() == false)
                                    return;
                            }
                        }

                        SleepUntil(Math.Min(_pacer.NextDueTicks, Stopwatch.GetTimestamp() + MaxSleepTicks));
                    }
                }
                catch (OperationCanceledException)
                {
                    // Graceful shutdown.
                }
                finally
                {
                    writer.TryComplete();
                }
            }

            private static void SleepUntil(long timestamp)
            {
                if (UseClockNanosleep)
                {
                    var target = new Timespec { Seconds = timestamp / NanosPerSecond, Nanoseconds = timestamp % NanosPerSecond };
                    // An interrupted sleep returns early; the caller re-reads the clock, so the loop tolerates it.
                    clock_nanosleep(ClockMonotonic, TimerAbstime, ref target, IntPtr.Zero);
                    return;
                }

                // Coarse timers: sleep to within one timer tick of the target, then yield-spin the bounded tail.
                var sleep = Stopwatch.GetElapsedTime(Stopwatch.GetTimestamp(), timestamp) - CoarseTimerTick;
                if (sleep > TimeSpan.Zero)
                    Thread.Sleep(sleep);

                var spinner = new SpinWait();
                while (Stopwatch.GetTimestamp() < timestamp)
                    spinner.SpinOnce(sleep1Threshold: -1);
            }

            private const long NanosPerSecond = 1_000_000_000;
            private const int ClockMonotonic = 1;
            private const int TimerAbstime = 1;

            // Bounds the yield-spin to the last millisecond before a due time. A host timer coarser than this still
            // oversleeps, and the send lateness reports it; a high-resolution waitable timer would remove that ceiling.
            private static readonly TimeSpan CoarseTimerTick = TimeSpan.FromMilliseconds(1);

            private static readonly bool UseClockNanosleep = StopwatchIsClockMonotonic();

            // clock_nanosleep takes Stopwatch timestamps as absolute targets only when both read CLOCK_MONOTONIC in
            // nanoseconds; the Timespec layout assumes a 64-bit time_t and long.
            private static bool StopwatchIsClockMonotonic()
            {
                if (OperatingSystem.IsLinux() == false || Environment.Is64BitProcess == false || Stopwatch.Frequency != NanosPerSecond)
                    return false;

                var before = Stopwatch.GetTimestamp();
                if (clock_gettime(ClockMonotonic, out var now) != 0)
                    return false;
                var after = Stopwatch.GetTimestamp();
                var monotonic = now.Seconds * NanosPerSecond + now.Nanoseconds;
                return monotonic >= before && monotonic <= after;
            }

            private struct Timespec
            {
                public long Seconds;
                public long Nanoseconds;
            }

            [System.Runtime.InteropServices.DllImport("libc", SetLastError = false)]
            private static extern int clock_gettime(int clockId, out Timespec time);

            [System.Runtime.InteropServices.DllImport("libc", SetLastError = false)]
            private static extern int clock_nanosleep(int clockId, int flags, ref Timespec request, IntPtr remain);
        }

        /// <summary>
        /// Pure pacing arithmetic on one clock. The first <see cref="Release"/> call fixes the schedule origin, so
        /// token n is due at origin + n / rate on the same clock the caller passes to <see cref="Release"/>.
        /// A call releases every token due at or before now. Overdue tokens beyond the burst capacity are skipped
        /// and counted as dropped. Timestamps must not decrease.
        /// </summary>
        internal sealed class TokenPacer
        {
            private readonly double _ticksPerToken;
            private readonly int _burstCapacity;
            private long _originTicks;
            private bool _started;
            private long _next;

            public long DroppedTokens { get; private set; }
            public long ReleasedTokens { get; private set; }

            public TokenPacer(double ratePerSecond, int burstCapacity, long ticksPerSecond)
            {
                ArgumentOutOfRangeException.ThrowIfNegativeOrZero(ratePerSecond);
                ArgumentOutOfRangeException.ThrowIfNegativeOrZero(burstCapacity);
                ArgumentOutOfRangeException.ThrowIfNegativeOrZero(ticksPerSecond);
                _ticksPerToken = ticksPerSecond / ratePerSecond;
                _burstCapacity = burstCapacity;
            }

            /// <summary>Due time of token <paramref name="sequence"/>; never before its ideal instant.</summary>
            public long DueTicks(long sequence) => _originTicks + (long)Math.Ceiling(sequence * _ticksPerToken);

            /// <summary>Due time of the next unreleased token.</summary>
            public long NextDueTicks => DueTicks(_next);

            /// <summary>Returns the sequence range of the tokens released at <paramref name="nowTicks"/>.</summary>
            public (long First, int Count) Release(long nowTicks)
            {
                if (_started == false)
                {
                    _originTicks = nowTicks;
                    _started = true;
                }

                var owed = (long)Math.Floor((nowTicks - _originTicks) / _ticksPerToken) + 1 - _next;
                if (owed <= 0)
                    return (_next, 0);

                if (owed > _burstCapacity)
                {
                    DroppedTokens += owed - _burstCapacity;
                    _next += owed - _burstCapacity;
                    owed = _burstCapacity;
                }

                var first = _next;
                _next += owed;
                ReleasedTokens += owed;
                return (first, (int)owed);
            }
        }

        /// <summary>
        /// Periodically samples completed operations to compute rolling throughput statistics over a fixed window.
        /// </summary>
        internal sealed class RollingRateSampler : IAsyncDisposable
        {
            private readonly TimeSpan _window;
            private readonly TimeSpan _interval;
            private readonly Queue<(double timeSeconds, long completed)> _history = new();
            private readonly List<double> _samples = new();
            private readonly object _lock = new();
            private readonly CancellationTokenSource _cts = new();
            private CancellationTokenSource? _linkedCts;
            private Task? _samplerTask;
            private double _lastSample;
            private bool _hasLastSample;

            public RollingRateSampler(TimeSpan window, TimeSpan interval)
            {
                _window = window;
                _interval = interval;
            }

            public void Start(LoadGeneratorCounters counters, Stopwatch stopwatch, CancellationToken cancellationToken)
            {
                _linkedCts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token, cancellationToken);
                var token = _linkedCts.Token;
                _samplerTask = Task.Run(async () =>
                {
                    while (token.IsCancellationRequested == false)
                    {
                        try
                        {
                            await Task.Delay(_interval, token).ConfigureAwait(false);
                        }
                        catch (OperationCanceledException)
                        {
                            break;
                        }

                        var elapsedSeconds = stopwatch.Elapsed.TotalSeconds;
                        var completed = counters.OperationsCompleted;
                        RecordSample(elapsedSeconds, completed);
                    }
                }, CancellationToken.None);
            }

            internal void RecordSample(double elapsedSeconds, long completed)
            {
                lock (_lock)
                {
                    _history.Enqueue((elapsedSeconds, completed));

                    while (_history.Count > 0)
                    {
                        var head = _history.Peek();
                        if (elapsedSeconds - head.timeSeconds > _window.TotalSeconds)
                            _history.Dequeue();
                        else
                            break;
                    }

                    if (_history.Count <= 1)
                        return;

                    var oldest = _history.Peek();
                    var deltaOps = completed - oldest.completed;
                    var deltaSeconds = elapsedSeconds - oldest.timeSeconds;
                    if (deltaSeconds <= 0)
                        return;

                    var rps = deltaOps / deltaSeconds;
                    _samples.Add(rps);
                    _lastSample = rps;
                    _hasLastSample = true;
                }
            }

            public async Task StopAsync()
            {
                _cts.Cancel();
                _linkedCts?.Cancel();

                if (_samplerTask != null)
                {
                    try
                    {
                        await _samplerTask.ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                        // Expected when stopping the sampler.
                    }
                }

                _linkedCts?.Dispose();
            }

            public RollingRateStats Snapshot()
            {
                lock (_lock)
                {
                    if (_samples.Count == 0)
                        return RollingRateStats.Empty;

                    var ordered = _samples.ToArray();
                    Array.Sort(ordered);
                    var mid = ordered.Length / 2;
                    double median = ordered.Length % 2 == 0
                        ? (ordered[mid - 1] + ordered[mid]) / 2.0
                        : ordered[mid];

                    double sum = 0;
                    double min = double.MaxValue;
                    double max = double.MinValue;
                    foreach (var sample in _samples)
                    {
                        sum += sample;
                        if (sample < min)
                            min = sample;
                        if (sample > max)
                            max = sample;
                    }

                    var last = _hasLastSample ? _lastSample : ordered[^1];

                    return new RollingRateStats
                    {
                        Median = median,
                        Mean = sum / _samples.Count,
                        Min = min,
                        Max = max,
                        Last = last,
                        SampleCount = _samples.Count
                    };
                }
            }

            public async ValueTask DisposeAsync()
            {
                await StopAsync().ConfigureAwait(false);
                _cts.Dispose();
            }
        }
    }
}
