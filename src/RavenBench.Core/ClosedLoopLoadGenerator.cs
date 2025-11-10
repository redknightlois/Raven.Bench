using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using HdrHistogram;
using RavenBench.Core.Metrics;
using RavenBench.Core.Transport;
using RavenBench.Core.Workload;

namespace RavenBench.Core
{
    /// <summary>
    /// Load generator that maintains a fixed number of concurrent operations (closed-loop).
    /// Each worker completes one operation before starting the next.
    /// </summary>
    public sealed class ClosedLoopLoadGenerator : ILoadGenerator
    {
        private readonly ITransport _transport;
        private readonly IWorkload _workload;
        private readonly int _concurrency;
        private readonly Random _rng;
        private long _baselineLatencyMicros;

        public int Concurrency => _concurrency;
        public double? TargetThroughput => null; // Closed-loop doesn't target specific throughput

        public ClosedLoopLoadGenerator(
            ITransport transport,
            IWorkload workload,
            int concurrency,
            Random rng)
        {
            _transport = transport ?? throw new ArgumentNullException(nameof(transport));
            _workload = workload ?? throw new ArgumentNullException(nameof(workload));
            _concurrency = concurrency;
            _rng = rng ?? throw new ArgumentNullException(nameof(rng));
        }

        public async Task ExecuteWarmupAsync(TimeSpan duration, CancellationToken cancellationToken)
        {
            await ExecuteAsync(duration, isWarmup: true, cancellationToken);
        }

        public async Task<(LatencyRecorder latencyRecorder, LoadGeneratorMetrics metrics)> ExecuteMeasurementAsync(
            TimeSpan duration, CancellationToken cancellationToken)
        {
            var (latencyRecorder, metrics) = await ExecuteAsync(duration, isWarmup: false, cancellationToken);
            return (latencyRecorder, metrics);
        }

        public void SetBaselineLatency(long baselineLatencyMicros)
        {
            _baselineLatencyMicros = baselineLatencyMicros;
        }

        private async Task<(LatencyRecorder latencyRecorder, LoadGeneratorMetrics metrics)> ExecuteAsync(
            TimeSpan duration, bool isWarmup, CancellationToken cancellationToken)
        {
            var latencyRecorder = new LatencyRecorder(!isWarmup);
            var bufferSize = Math.Max(_concurrency, 1);
            var channel = Channel.CreateBounded<OperationBase>(bufferSize);
            var counters = new LoadGeneratorCounters();
            Recorder? warmupRecorder = isWarmup
                ? new Recorder(1, 60_000_000, 3, (instanceId, low, high, digits) => new LongHistogram(low, high, digits))
                : null;

            var stopwatch = Stopwatch.StartNew();
            var endTime = stopwatch.Elapsed + duration;

            var workerTasks = new Task[_concurrency];
            for (int i = 0; i < _concurrency; i++)
            {
                workerTasks[i] = Task.Run(async () =>
                {
                    await foreach (var operation in channel.Reader.ReadAllAsync(cancellationToken))
                    {
                        var result = await LoadGeneratorExecution.ExecuteOperationAsync(
                            _transport,
                            operation,
                            latencyRecorder,
                            _baselineLatencyMicros,
                            cancellationToken);

                        counters.Record(result);
                        if (warmupRecorder != null && result.IsError == false)
                            warmupRecorder.RecordValue(result.LatencyMicros);
                    }
                }, cancellationToken);
            }

            long scheduledCount = 0;
            while (stopwatch.Elapsed < endTime && cancellationToken.IsCancellationRequested == false)
            {
                var operation = _workload.NextOperation(_rng);
                Interlocked.Increment(ref scheduledCount);
                await channel.Writer.WriteAsync(operation, cancellationToken);
            }

            channel.Writer.Complete();
            await Task.WhenAll(workerTasks);

            var actualDuration = stopwatch.Elapsed;
            var metrics = LoadGeneratorExecution.BuildMetrics(counters, actualDuration, scheduledCount, isWarmup);

            if (isWarmup)
                UpdateBaselineFromWarmup(warmupRecorder);

            return (latencyRecorder, metrics);
        }

        private void UpdateBaselineFromWarmup(Recorder? warmupRecorder)
        {
            if (warmupRecorder == null)
                return;

            var histogram = warmupRecorder.GetIntervalHistogram();
            if (histogram == null || histogram.TotalCount == 0)
                return;

            var median = histogram.GetValueAtPercentile(50.0);
            if (median <= 0)
                return;

            Interlocked.Exchange(ref _baselineLatencyMicros, (long)median);
        }
    }
}
