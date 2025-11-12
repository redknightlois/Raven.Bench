using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using RavenBench.Core.Metrics;
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
        private readonly ITransport _transport;
        private readonly IWorkload _workload;
        private readonly double _targetRps;
        private readonly int _maxConcurrency;
        private readonly Random _rng;
        private readonly object _workloadLock = new();
        private long _expectedIntervalMicros;

        public int Concurrency => _maxConcurrency;
        public double? TargetThroughput => _targetRps;

        public RateLoadGenerator(
            ITransport transport,
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
            var warmupRps = _targetRps * 0.5; // Use 50% of target rate for warmup
            await ExecuteAsync(duration, warmupRps, isWarmup: true, cancellationToken);
        }

        public async Task<(LatencyRecorder latencyRecorder, LoadGeneratorMetrics metrics)> ExecuteMeasurementAsync(
            TimeSpan duration, CancellationToken cancellationToken)
        {
            return await ExecuteAsync(duration, _targetRps, isWarmup: false, cancellationToken);
        }

        public void SetBaselineLatency(long baselineLatencyMicros)
        {
            _expectedIntervalMicros = baselineLatencyMicros;
        }

        private async Task<(LatencyRecorder latencyRecorder, LoadGeneratorMetrics metrics)> ExecuteAsync(
            TimeSpan duration, double targetRps, bool isWarmup, CancellationToken cancellationToken)
        {
            var latencyRecorder = new LatencyRecorder(!isWarmup);
            var counters = new LoadGeneratorCounters();
            var scheduledCount = 0L;
            var measurementStopwatch = Stopwatch.StartNew();
            RollingRateStats? rollingStats = null;
            RollingRateSampler? rollingSampler = null;

            // Pre-compute the expected interval so coordinated omission correction stays accurate.
            ResolveIntervalMicros(targetRps);

            await using var scheduler = new TokenBucketScheduler(
                targetRps,
                // Give each worker a few in-flight permits; this keeps pacing predictable while still allowing short spikes.
                burstCapacity: Math.Max(_maxConcurrency * 4, 32),
                cancellationToken);

            var workers = StartWorkers(
                scheduler,
                latencyRecorder,
                counters,
                () => Interlocked.Increment(ref scheduledCount),
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

            var metrics = LoadGeneratorExecution.BuildMetrics(
                counters,
                measurementStopwatch.Elapsed,
                scheduledCount,
                isWarmup,
                rollingStats);

            return (latencyRecorder, metrics);
        }

        private Task[] StartWorkers(
            TokenBucketScheduler scheduler,
            LatencyRecorder latencyRecorder,
            LoadGeneratorCounters counters,
            Action onOperationScheduled,
            CancellationToken cancellationToken)
        {
            var workers = new Task[_maxConcurrency];
            for (int i = 0; i < _maxConcurrency; i++)
            {
                workers[i] = Task.Run(async () =>
                {
                    try
                    {
                        await foreach (var _ in scheduler.ConsumeAsync(cancellationToken))
                        {
                            var operation = CreateOperation();
                            onOperationScheduled();

                            var result = await LoadGeneratorExecution.ExecuteOperationAsync(
                                _transport,
                                operation,
                                latencyRecorder,
                                _expectedIntervalMicros,
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

        private OperationBase CreateOperation()
        {
            lock (_workloadLock)
            {
                return _workload.NextOperation(_rng);
            }
        }

        private long ResolveIntervalMicros(double targetRps)
        {
            var intervalMicros = targetRps > 0
                ? Math.Max(1, (long)Math.Round(1_000_000.0 / targetRps))
                : _expectedIntervalMicros;

            if (intervalMicros > 0)
                Interlocked.Exchange(ref _expectedIntervalMicros, intervalMicros);

            return intervalMicros;
        }

        /// <summary>
        /// Simple token-bucket scheduler that releases work permits at the requested rate.
        /// Workers consume the permits independently, which keeps pacing stable even
        /// when the producer thread is delayed or preempted.
        /// </summary>
        private sealed class TokenBucketScheduler : IAsyncDisposable
        {
            private readonly Channel<bool> _tokens;
            private readonly double _ratePerSecond;
            private readonly int _burstCapacity;
            private readonly CancellationToken _cancellationToken;
            private readonly CancellationTokenSource _producerCts = new();
            private readonly Task _producerTask;
            private double _carry;
            private int _stopped;

            public TokenBucketScheduler(double ratePerSecond, int burstCapacity, CancellationToken cancellationToken)
            {
                _ratePerSecond = Math.Max(ratePerSecond, 0);
                _burstCapacity = Math.Max(1, burstCapacity);
                _cancellationToken = cancellationToken;

                _tokens = Channel.CreateBounded<bool>(new BoundedChannelOptions(_burstCapacity)
                {
                    SingleReader = false,
                    SingleWriter = true,
                    FullMode = BoundedChannelFullMode.Wait
                });

                _producerTask = Task.Run(ReplenishAsync);
            }

            public IAsyncEnumerable<bool> ConsumeAsync(CancellationToken cancellationToken)
            {
                return _tokens.Reader.ReadAllAsync(cancellationToken);
            }

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

            private async Task ReplenishAsync()
            {
                if (_ratePerSecond <= 0)
                {
                    _tokens.Writer.TryComplete();
                    return;
                }

                // Use a bounded interval so replenishment frequency scales with both rate and burst capacity.
                var targetIntervalMs = CalculateReplenishmentDelay(_ratePerSecond, _burstCapacity);
                var targetIntervalTicks = (long)(targetIntervalMs * TimeSpan.TicksPerMillisecond);
                var stopwatch = Stopwatch.StartNew();
                var lastTick = stopwatch.ElapsedTicks;

                try
                {
                    while (_producerCts.IsCancellationRequested == false && _cancellationToken.IsCancellationRequested == false)
                    {
                        var nowTicks = stopwatch.Elapsed.Ticks;
                        var deltaTicks = nowTicks - lastTick;

                        if (deltaTicks > 0)
                        {
                            var deltaSeconds = deltaTicks / (double)TimeSpan.TicksPerSecond;
                            var tokensToAdd = deltaSeconds * _ratePerSecond;

                            if (tokensToAdd > 0)
                            {
                                _carry = Math.Min(_carry + tokensToAdd, _burstCapacity);
                                var wholeTokens = (int)Math.Floor(_carry);

                                if (wholeTokens > 0)
                                {
                                    _carry -= wholeTokens;
                                    await WriteTokensAsync(_tokens.Writer, wholeTokens, _producerCts.Token).ConfigureAwait(false);
                                }
                            }

                            lastTick = nowTicks;
                        }

                        // Schedule the next wake-up relative to this cycle and wait just long enough to stay on pace.
                        var nextTick = nowTicks + targetIntervalTicks;
                        await WaitForNextTickAsync(nextTick, stopwatch, _producerCts.Token).ConfigureAwait(false);
                    }
                }
                catch (OperationCanceledException)
                {
                    // Graceful shutdown.
                }
                finally
                {
                    _tokens.Writer.TryComplete();
                }
            }

            private static double CalculateReplenishmentDelay(double ratePerSecond, int burstCapacity)
            {
                if (ratePerSecond <= 0)
                    return 1.0;

                var tokensPerMs = ratePerSecond / 1000.0;
                if (tokensPerMs <= 0)
                    return 1.0;

                var effectiveBurst = Math.Max(1, Math.Min(burstCapacity, 500));
                var intervalMs = effectiveBurst / tokensPerMs;
                return Math.Clamp(intervalMs, 0.1, 1.0);
            }

            private static async Task WriteTokensAsync(ChannelWriter<bool> writer, int count, CancellationToken cancellationToken)
            {
                for (int i = 0; i < count; i++)
                {
                    // Fast-path: try to write synchronously; only await when the channel is momentarily full.
                    if (writer.TryWrite(true))
                        continue;

                    await writer.WriteAsync(true, cancellationToken).ConfigureAwait(false);
                }
            }

            private async Task WaitForNextTickAsync(long targetTick, Stopwatch stopwatch, CancellationToken producerToken)
            {
                while (producerToken.IsCancellationRequested == false && _cancellationToken.IsCancellationRequested == false)
                {
                    var remainingTicks = targetTick - stopwatch.ElapsedTicks;
                    if (remainingTicks <= 0)
                        return;

                    var remainingMs = remainingTicks / (double)TimeSpan.TicksPerMillisecond;
                    if (remainingMs >= 1.0)
                    {
                        // Millisecond waits use Task.Delay to keep the producer CPU-friendly.
                        await Task.Delay(TimeSpan.FromMilliseconds(remainingMs), producerToken).ConfigureAwait(false);
                    }
                    else
                    {
                        // Sub-millisecond waits spin to avoid overshooting; SpinUntil handles progressive back-off for us.
                        SpinWait.SpinUntil(
                            () => stopwatch.Elapsed.Ticks >= targetTick ||
                                  producerToken.IsCancellationRequested ||
                                  _cancellationToken.IsCancellationRequested);

                        if (producerToken.IsCancellationRequested || _cancellationToken.IsCancellationRequested)
                            return;
                    }
                }
            }
        }

        /// <summary>
        /// Periodically samples completed operations to compute rolling throughput statistics over a fixed window.
        /// </summary>
        private sealed class RollingRateSampler : IAsyncDisposable
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

            private void RecordSample(double elapsedSeconds, long completed)
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
