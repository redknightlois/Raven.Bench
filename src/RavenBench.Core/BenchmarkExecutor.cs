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
        private readonly ITransport _transport;
        private readonly IWorkload _workload;
        private readonly ProcessCpuTracker _cpuTracker;
        private readonly ServerMetricsTracker? _serverTracker;
        private readonly SnmpOptions? _snmpOptions;

        public BenchmarkExecutor(
            RunOptions options,
            ITransport transport,
            IWorkload workload,
            ProcessCpuTracker cpuTracker,
            ServerMetricsTracker? serverTracker = null,
            SnmpOptions? snmpOptions = null)
        {
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _transport = transport ?? throw new ArgumentNullException(nameof(transport));
            _workload = workload ?? throw new ArgumentNullException(nameof(workload));
            _cpuTracker = cpuTracker ?? throw new ArgumentNullException(nameof(cpuTracker));
            _serverTracker = serverTracker;
            _snmpOptions = snmpOptions;
        }

        /// <summary>
        /// Executes a single benchmark step using the provided load generator.
        /// </summary>
        public async Task<(LatencyRecorder latencyRecorder, StepResult result, WarmupSummary? warmupSummary)> ExecuteStepAsync(
            ILoadGenerator loadGenerator,
            int stepIndex,
            int currentStepValue,
            CancellationToken cancellationToken,
            long baselineLatencyMicros = 0)
        {
            var warmupTime = _options.Warmup;
            var measurementTime = _options.Duration;

            // Set baseline latency for coordinated omission correction
            loadGenerator.SetBaselineLatency(baselineLatencyMicros);

            WarmupSummary? warmupSummary = null;

            // Warmup phase - run until convergence or max iterations
            if (warmupTime > TimeSpan.Zero)
            {
                if (_options.WarmupConverge)
                {
                    warmupSummary = await ExecuteWarmupPhaseAsync(loadGenerator, warmupTime, cancellationToken);
                }
                else
                {
                    // Single warmup iteration, no convergence check
                    var warmupDiag = await loadGenerator.ExecuteWarmupAsync(warmupTime, cancellationToken);
                    warmupSummary = new WarmupSummary(new[] { warmupDiag }, converged: false, WarmupFailureReason.Disabled);
                }
            }

            // Start tracking before measurement
            _cpuTracker.Reset();
            _cpuTracker.Start();
            _serverTracker?.Start();

            try
            {
                // Measurement phase
                var (latencyRecorder, metrics) = await loadGenerator.ExecuteMeasurementAsync(
                    measurementTime, cancellationToken);

                // Build step result
                var result = BuildStepResultAsync(
                    loadGenerator, stepIndex, currentStepValue, latencyRecorder, metrics, cancellationToken);

                return (latencyRecorder, result, warmupSummary);
            }
            finally
            {
                // Stop tracking after measurement
                _cpuTracker.Stop();
                _serverTracker?.Stop();
            }
        }

        private async Task<WarmupSummary> ExecuteWarmupPhaseAsync(
            ILoadGenerator loadGenerator,
            TimeSpan warmupDuration,
            CancellationToken cancellationToken)
        {
            var iterations = new List<WarmupDiagnostics>();
            var maxIterations = _options.WarmupMaxIterations;

            for (int i = 0; i < maxIterations; i++)
            {
                var diagnostics = await loadGenerator.ExecuteWarmupAsync(warmupDuration, cancellationToken);
                iterations.Add(diagnostics);

                // Check convergence after second iteration
                if (i >= 1 && WarmupStabilityHeuristics.HasConverged(iterations))
                {
                    return WarmupStabilityHeuristics.BuildSummary(iterations, requireConvergence: true, maxIterations);
                }
            }

            // Reached max iterations without converging
            return WarmupStabilityHeuristics.BuildSummary(iterations, requireConvergence: true, maxIterations);
        }

        private StepResult BuildStepResultAsync(
            ILoadGenerator loadGenerator,
            int stepIndex,
            int currentStepValue,
            LatencyRecorder latencyRecorder,
            LoadGeneratorMetrics metrics,
            CancellationToken cancellationToken)
        {
            // Don't take snapshot here - let BenchmarkRunner handle all snapshot and percentile logic
            // to avoid double-snapshot issue

            var serverMetrics = _serverTracker?.Current ?? new ServerMetrics();

            var result = new StepResult
            {
                Concurrency = loadGenerator.Concurrency,
                Throughput = metrics.Throughput,
                TargetThroughput = loadGenerator.TargetThroughput,
                ErrorRate = metrics.ErrorRate,
                BytesOut = metrics.BytesOut,
                BytesIn = metrics.BytesIn,
                // Percentiles will be set by BenchmarkRunner after taking snapshot
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
                ClientCpu = _cpuTracker.AverageCpu,
                ServerCpu = serverMetrics.CpuUsagePercent,
                ServerMemoryMB = serverMetrics.MemoryUsageMB,
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
                NetworkUtilization = metrics.NetworkUtilization,
                Reason = metrics.Reason,
                RollingRate = metrics.RollingRate
            };

            // Collect SNMP metrics if configured
            if (_snmpOptions != null)
            {
                // TODO: Implement SNMP metrics collection
                // This would involve calling the SNMP client to get current metrics
            }

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
        Task<WarmupDiagnostics> ExecuteWarmupAsync(TimeSpan duration, CancellationToken cancellationToken);

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
        public double NetworkUtilization { get; init; }
        public string? Reason { get; init; }
        public RollingRateStats? RollingRate { get; init; }
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
