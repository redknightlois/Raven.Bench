using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
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
        private readonly IYcsbTransport _transport;
        private readonly IWorkload _workload;
        private readonly int _concurrency;
        private readonly int _workers;
        private readonly Random _rng;

        public int Concurrency => _concurrency;
        public double? TargetThroughput => null; // Closed-loop doesn't target specific throughput

        public ClosedLoopLoadGenerator(
            IYcsbTransport transport,
            IWorkload workload,
            int concurrency,
            Random rng,
            int pipelineDepth = 1)
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(pipelineDepth, 1);
            _transport = transport ?? throw new ArgumentNullException(nameof(transport));
            _workload = workload ?? throw new ArgumentNullException(nameof(workload));
            _concurrency = concurrency;
            // Concurrency counts connections; each carries up to pipelineDepth requests, one per worker.
            _workers = checked(concurrency * pipelineDepth);
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

        private async Task<(LatencyRecorder latencyRecorder, LoadGeneratorMetrics metrics)> ExecuteAsync(
            TimeSpan duration, bool isWarmup, CancellationToken cancellationToken)
        {
            var latencyRecorder = new LatencyRecorder(isWarmup == false);
            var counters = new LoadGeneratorCounters();

            var stopwatch = Stopwatch.StartNew();
            var endTime = stopwatch.Elapsed + duration;
            var source = new object();
            long scheduledCount = 0;

            var workerTasks = new Task[_workers];
            for (int i = 0; i < _workers; i++)
            {
                workerTasks[i] = Task.Run(async () =>
                {
                    while (true)
                    {
                        OperationBase operation;
                        // Every operation is drawn in turn from the one run-seeded source; the
                        // concurrency decides how many are drawn before the step ends and which worker
                        // runs each one, never the sequence itself. System.Random is not thread-safe,
                        // so the source is only touched under this lock.
                        lock (source)
                        {
                            if (stopwatch.Elapsed >= endTime || cancellationToken.IsCancellationRequested || _workload.IsExhausted)
                                return;
                            operation = _workload.NextOperation(_rng);
                            scheduledCount++;
                        }

                        var result = await LoadGeneratorExecution.ExecuteOperationAsync(
                            _transport,
                            operation,
                            latencyRecorder,
                            Stopwatch.GetTimestamp(),
                            cancellationToken);

                        counters.Record(result);
                    }
                }, cancellationToken);
            }

            await Task.WhenAll(workerTasks);

            var actualDuration = stopwatch.Elapsed;
            var metrics = LoadGeneratorExecution.BuildMetrics(counters, actualDuration, scheduledCount, isWarmup);

            return (latencyRecorder, metrics);
        }
    }
}
