using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using RavenBench.Core.Diagnostics;
using RavenBench.Core.Metrics;
using RavenBench.Core.Transport;
using RavenBench.Core.Workload;
using RavenBench.Core.Reporting;
using RavenBench;

namespace RavenBench.Core
{
    /// <summary>
    /// Executes benchmark steps with warmup and measurement phases,
    /// supporting different load generation strategies.
    /// </summary>
    public sealed class BenchmarkExecutor
    {
        private readonly RunOptions _options;
        private readonly IYcsbTransport _transport;
        private readonly IWorkload _workload;
        private readonly ProcessCpuTracker _cpuTracker;
        private readonly ServerMetricsTracker? _serverTracker;
        private readonly NodeExporterClient? _nodeExporter;
        private readonly string? _runName;

        /// <param name="runName">
        /// The run of a ycsb sequence these steps belong to, so an invalid step names the result
        /// file it landed in. Null for a command that writes one result.
        /// </param>
        public BenchmarkExecutor(
            RunOptions options,
            IYcsbTransport transport,
            IWorkload workload,
            ProcessCpuTracker cpuTracker,
            ServerMetricsTracker? serverTracker = null,
            string? runName = null,
            NodeExporterClient? nodeExporter = null)
        {
            _nodeExporter = nodeExporter;
            _runName = runName;
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _transport = transport ?? throw new ArgumentNullException(nameof(transport));
            _workload = workload ?? throw new ArgumentNullException(nameof(workload));
            _cpuTracker = cpuTracker ?? throw new ArgumentNullException(nameof(cpuTracker));
            _serverTracker = serverTracker;
        }

        /// <summary>
        /// Executes a single benchmark step using the provided load generator.
        /// </summary>
        public async Task<(LatencyRecorder latencyRecorder, StepResult result)> ExecuteStepAsync(
            ILoadGenerator loadGenerator,
            int stepIndex,
            int currentStepValue,
            CancellationToken cancellationToken,
            long baselineLatencyMicros = 0)
        {
            var warmupTime = _options.Warmup;
            var measurementTime = _options.Duration;

            loadGenerator.SetBaselineLatency(baselineLatencyMicros);

            if (warmupTime > TimeSpan.Zero)
            {
                await loadGenerator.ExecuteWarmupAsync(warmupTime, cancellationToken);
            }

            _cpuTracker.Reset();
            _cpuTracker.Start();
            _serverTracker?.Start();
            // A start-scrape fault surfaces when the window closes, as the step's absence reason.
            var nodeStart = _nodeExporter?.ScrapeAsync(cancellationToken);

            using var exTracker = FirstChanceExceptionTracker.BeginStep();

            try
            {
                var (latencyRecorder, metrics) = await loadGenerator.ExecuteMeasurementAsync(
                    measurementTime, cancellationToken);

                // The measurement window closes here. The exception dump and the result
                // construction below run outside it, so neither enters the load-host CPU figure.
                _cpuTracker.Stop();
                var nodeWindow = nodeStart == null ? null : await _nodeExporter!.EndWindowAsync(nodeStart, cancellationToken);

                var exSnap = exTracker.Take();
                if (exSnap.Total > 0)
                {
                    var perOp = metrics.OperationsCompleted > 0
                        ? (double)exSnap.Total / metrics.OperationsCompleted
                        : 0.0;
                    Console.Error.WriteLine(
                        $"[Raven.Bench] WARNING: {exSnap.Total} first-chance exceptions during measurement step (concurrency={currentStepValue}, ops={metrics.OperationsCompleted}, ratio={perOp:F2}/op).");
                    foreach (var (type, count) in exSnap.ByType.Take(5))
                        Console.Error.WriteLine($"[Raven.Bench]   {count,10:N0}  {type}");
                    if (exSnap.FirstType != null)
                    {
                        Console.Error.WriteLine($"[Raven.Bench] First exception: {exSnap.FirstType}: {exSnap.FirstMessage}");
                        if (string.IsNullOrEmpty(exSnap.FirstStack) == false)
                            Console.Error.WriteLine(exSnap.FirstStack);
                    }
                    Console.Error.WriteLine("[Raven.Bench] Benchmarks should not throw on the hot path; fix the transport or treat results as untrusted.");
                }

                var result = BuildStepResult(
                    loadGenerator, stepIndex, currentStepValue, latencyRecorder, metrics, nodeWindow);

                if (result.InvalidReason != null)
                    Console.WriteLine(ClientSaturation.ConsoleLine(stepIndex + 1, currentStepValue, _runName, result.InvalidReason));

                return (latencyRecorder, result);
            }
            finally
            {
                _cpuTracker.Stop();
                _serverTracker?.Stop();
            }
        }

        private const string RavenDebugSource = "ravendb-debug";

        private StepResult BuildStepResult(
            ILoadGenerator loadGenerator,
            int stepIndex,
            int currentStepValue,
            LatencyRecorder latencyRecorder,
            LoadGeneratorMetrics metrics,
            NodeExporterWindow? nodeWindow)
        {

            var serverMetrics = _serverTracker?.Current ?? new ServerMetrics();

            var query = metrics.Query;
            var hasQueries = query is { HasQueries: true };
            var topIndexes = hasQueries
                ? query!.IndexUsage
                    .OrderByDescending(kvp => kvp.Value)
                    .Take(5)
                    .Select(kvp => new IndexUsageSummary
                    {
                        IndexName = kvp.Key,
                        UsageCount = kvp.Value,
                        UsagePercent = (double)kvp.Value / query.QueryOperations * 100.0
                    })
                    .ToList()
                : null;

            var clientCpu = _cpuTracker.AverageCpu;

            // node_exporter, when the operator configured it, fills both columns for every product;
            // otherwise the RavenDB debug endpoints fill them for a RavenDB target.
            var serverCpu = nodeWindow != null ? nodeWindow.CpuPercent : serverMetrics.CpuUsagePercent;
            var serverMemory = nodeWindow != null ? nodeWindow.MemoryMB : serverMetrics.MemoryUsageMB;
            var source = nodeWindow != null ? NodeExporterClient.SourceName : RavenDebugSource;

            var result = new StepResult
            {
                Concurrency = loadGenerator.Concurrency,
                Throughput = metrics.Throughput,
                TargetThroughput = loadGenerator.TargetThroughput,
                ErrorRate = metrics.ErrorRate,
                BytesOut = metrics.BytesOut,
                BytesIn = metrics.BytesIn,
                Raw = default,
                Normalized = default,
                P9999 = 0,
                PMax = 0,
                NormalizedP9999 = 0,
                NormalizedPMax = 0,
                SampleCount = metrics.OperationsCompleted,
                CorrectedCount = 0, // Will be set by BenchmarkRunner
                ScheduledOperations = metrics.ScheduledOperations,
                MaxTimestamp = null, // Not tracked in simplified executor
                ClientCpu = clientCpu,
                MeasuredDuration = metrics.Duration,
                InvalidReason = ClientSaturation.MarkingFor(clientCpu),
                ServerCpu = serverCpu,
                ServerMemoryMB = serverMemory,
                ServerCpuSource = serverCpu.HasValue ? source : null,
                ServerMemorySource = serverMemory.HasValue ? source : null,
                ServerMetricsHostWide = serverCpu.HasValue || serverMemory.HasValue ? nodeWindow != null : null,
                ServerMetricsUnavailable = nodeWindow?.Unavailable,
                ServerRequestsPerSec = serverMetrics.RequestsPerSecond,
                ServerIoReadOps = serverMetrics.IoReadOperations.HasValue ? (long?)serverMetrics.IoReadOperations.Value : null,
                ServerIoWriteOps = serverMetrics.IoWriteOperations.HasValue ? (long?)serverMetrics.IoWriteOperations.Value : null,
                ServerIoReadKb = serverMetrics.ReadThroughputKb,
                ServerIoWriteKb = serverMetrics.WriteThroughputKb,
                MachineCpu = serverMetrics.MachineCpu,
                ProcessCpu = serverMetrics.ProcessCpu,
                ManagedMemoryMb = serverMetrics.ManagedMemoryMb,
                UnmanagedMemoryMb = serverMetrics.UnmanagedMemoryMb,
                DirtyMemoryMb = serverMetrics.DirtyMemoryMb,
                Load1Min = serverMetrics.Load1Min,
                Load5Min = serverMetrics.Load5Min,
                Load15Min = serverMetrics.Load15Min,
                SnmpIoReadOpsPerSec = serverMetrics.SnmpIoReadOpsPerSec,
                SnmpIoWriteOpsPerSec = serverMetrics.SnmpIoWriteOpsPerSec,
                SnmpIoReadBytesPerSec = serverMetrics.SnmpIoReadBytesPerSec,
                SnmpIoWriteBytesPerSec = serverMetrics.SnmpIoWriteBytesPerSec,
                ServerSnmpRequestsPerSec = serverMetrics.ServerSnmpRequestsPerSec,
                SnmpErrorsPerSec = serverMetrics.SnmpErrorsPerSec,
                NetworkUtilization = LoadGeneratorExecution.Utilization(metrics.BytesOut, metrics.BytesIn, metrics.Duration, _options.LinkMbps),
                NetworkBytesMeasured = _transport.ReportsWireBytes,
                Reason = metrics.Reason,
                RollingRate = metrics.RollingRate,
                QueryOperations = hasQueries ? query!.QueryOperations : null,
                IndexUsage = hasQueries ? query!.IndexUsage : null,
                TopIndexes = topIndexes,
                MinResultCount = query?.MinResultCount,
                MaxResultCount = query?.MaxResultCount,
                AvgResultCount = query?.AvgResultCount,
                TotalResults = hasQueries ? query!.TotalResults : null,
                StaleQueryCount = hasQueries && query!.StaleQueries > 0 ? query.StaleQueries : null,
                QueryProfile = _options.QueryProfile != QueryProfile.VoronEquality ? _options.QueryProfile : null
            };

            return result;
        }
    }

    /// <summary>
    /// Defines the contract for load generation strategies.
    /// </summary>
    public interface ILoadGenerator
    {
        /// <summary>
        /// The concurrency level for this load generator.
        /// </summary>
        int Concurrency { get; }

        /// <summary>
        /// The target throughput (RPS) for rate-based generators, null for closed-loop.
        /// </summary>
        double? TargetThroughput { get; }

        /// <summary>
        /// Executes warmup operations to stabilize the system.
        /// </summary>
        Task ExecuteWarmupAsync(TimeSpan duration, CancellationToken cancellationToken);

        /// <summary>
        /// Executes the measurement phase and returns collected metrics.
        /// </summary>
        Task<(LatencyRecorder latencyRecorder, LoadGeneratorMetrics metrics)> ExecuteMeasurementAsync(
            TimeSpan duration, CancellationToken cancellationToken);

        /// <summary>
        /// Sets the baseline latency for coordinated omission correction.
        /// </summary>
        void SetBaselineLatency(long baselineLatencyMicros);
    }

    /// <summary>
    /// Metrics collected by load generators during execution.
    /// </summary>
    public sealed class LoadGeneratorMetrics
    {
        public double Throughput { get; init; }
        public double ErrorRate { get; init; }
        public long BytesOut { get; init; }
        public long BytesIn { get; init; }
        public TimeSpan Duration { get; init; }
        public string? Reason { get; init; }
        public RollingRateStats? RollingRate { get; init; }
        public QueryStatsSnapshot? Query { get; init; }
        /// <summary>
        /// Number of operations scheduled (may exceed completed if queueing occurs).
        /// </summary>
        public long ScheduledOperations { get; init; }
        /// <summary>
        /// Number of operations completed (success + errors).
        /// </summary>
        public long OperationsCompleted { get; init; }
    }
}
