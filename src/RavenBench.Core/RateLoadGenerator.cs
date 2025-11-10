using System;
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
            var channel = Channel.CreateBounded<OperationBase>(_maxConcurrency);
            var counters = new LoadGeneratorCounters();
            var workers = StartWorkers(channel.Reader, latencyRecorder, counters, cancellationToken);

            var intervalMicros = ResolveIntervalMicros(targetRps);
            var interval = intervalMicros > 0 ? TimeSpan.FromTicks(intervalMicros * 10) : TimeSpan.Zero;
            var stopwatch = Stopwatch.StartNew();
            long scheduledCount = 0;
            var nextDue = stopwatch.Elapsed;

            while (stopwatch.Elapsed < duration && cancellationToken.IsCancellationRequested == false)
            {
                if (interval > TimeSpan.Zero)
                {
                    var remaining = nextDue - stopwatch.Elapsed;
                    if (remaining > TimeSpan.Zero)
                    {
                        if (remaining > TimeSpan.FromMilliseconds(1))
                            await Task.Delay(remaining, cancellationToken);
                        else
                            await Task.Yield();
                        continue;
                    }
                }

                var operation = _workload.NextOperation(_rng);
                await channel.Writer.WriteAsync(operation, cancellationToken);
                Interlocked.Increment(ref scheduledCount);

                if (interval > TimeSpan.Zero)
                {
                    if (nextDue <= stopwatch.Elapsed)
                        nextDue = stopwatch.Elapsed + interval;
                    else
                        nextDue += interval;
                }
            }

            var measurementDuration = stopwatch.Elapsed;

            channel.Writer.Complete();
            await Task.WhenAll(workers);

            var metrics = LoadGeneratorExecution.BuildMetrics(counters, measurementDuration, scheduledCount, isWarmup);
            return (latencyRecorder, metrics);
        }

        private Task[] StartWorkers(
            ChannelReader<OperationBase> reader,
            LatencyRecorder latencyRecorder,
            LoadGeneratorCounters counters,
            CancellationToken cancellationToken)
        {
            var workers = new Task[_maxConcurrency];
            for (int i = 0; i < _maxConcurrency; i++)
            {
                workers[i] = Task.Run(async () =>
                {
                    await foreach (var operation in reader.ReadAllAsync(cancellationToken))
                    {
                        var result = await LoadGeneratorExecution.ExecuteOperationAsync(
                            _transport,
                            operation,
                            latencyRecorder,
                            _expectedIntervalMicros,
                            cancellationToken);
                        counters.Record(result);
                    }
                }, cancellationToken);
            }

            return workers;
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
    }
}
