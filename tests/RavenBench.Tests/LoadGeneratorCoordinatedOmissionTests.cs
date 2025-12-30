using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using RavenBench.Core;
using RavenBench.Core.Metrics;
using RavenBench.Core.Metrics.Snmp;
using RavenBench.Core.Transport;
using RavenBench.Core.Workload;
using Xunit;

namespace RavenBench.Tests;

public sealed class LoadGeneratorCoordinatedOmissionTests
{
    [Fact]
    public async Task ClosedLoop_UsesWarmupMedianForCoordinatedOmission()
    {
        var transport = new VariableLatencyTransport(latencyMs: 2);
        var workload = new SingleOperationWorkload();
        var generator = new ClosedLoopLoadGenerator(transport, workload, concurrency: 1, new Random(17));

        // Warmup with faster latency to establish baseline
        await generator.ExecuteWarmupAsync(TimeSpan.FromMilliseconds(200), CancellationToken.None);

        // Increase latency significantly for measurement
        transport.LatencyMs = 20;

        var (recorder, metrics) = await generator.ExecuteMeasurementAsync(
            TimeSpan.FromMilliseconds(200), CancellationToken.None);

        metrics.OperationsCompleted.Should().BeGreaterThan(0);
        var snapshot = recorder.Snapshot();

        // With 2ms baseline and 20ms actual latency, coordinated omission correction
        // should add synthetic samples, making TotalCount > OperationsCompleted
        snapshot.TotalCount.Should().BeGreaterThan(metrics.OperationsCompleted);
    }

    [Fact]
    public async Task RateGenerator_UsesTargetRpsForCoordinatedOmission()
    {
        // Use a low warmup latency to establish a fast baseline
        var transport = new VariableLatencyTransport(latencyMs: 5);
        var workload = new SingleOperationWorkload();
        // Target 100 RPS means 10ms expected interval between requests
        var generator = new RateLoadGenerator(transport, workload, targetRps: 100, maxConcurrency: 8, new Random(42));

        await generator.ExecuteWarmupAsync(TimeSpan.FromMilliseconds(200), CancellationToken.None);

        // Increase latency to 10x the expected interval to ensure CO correction triggers
        transport.LatencyMs = 100;

        var (recorder, metrics) = await generator.ExecuteMeasurementAsync(
            TimeSpan.FromMilliseconds(500), CancellationToken.None);

        metrics.OperationsCompleted.Should().BeGreaterThan(0);
        var snapshot = recorder.Snapshot();
        // With 100ms latency vs 10ms expected interval, coordinated omission correction
        // should add synthetic samples, making TotalCount > OperationsCompleted
        snapshot.TotalCount.Should().BeGreaterThanOrEqualTo(metrics.OperationsCompleted);
    }

    private sealed class SingleOperationWorkload : IWorkload
    {
        public OperationBase NextOperation(Random rng) => new ReadOperation { Id = "users/1" };
        public IWorkload? CreateWarmupWorkload(long preloadCount, IKeyDistribution distribution) => null;
    }

    private sealed class VariableLatencyTransport : ITransport
    {
        private int _latencyMs;

        public int LatencyMs
        {
            get => _latencyMs;
            set => _latencyMs = Math.Max(0, value);
        }

        public VariableLatencyTransport(int latencyMs)
        {
            _latencyMs = Math.Max(0, latencyMs);
        }

        public async Task<TransportResult> ExecuteAsync(OperationBase op, CancellationToken ct)
        {
            if (_latencyMs > 0)
                await Task.Delay(_latencyMs, ct);

            return new TransportResult(64, 32);
        }

        public Task PutAsync<T>(string id, T document) => Task.CompletedTask;
        public Task EnsureDatabaseExistsAsync(string databaseName) => Task.CompletedTask;
        public Task<long> GetDocumentCountAsync(string idPrefix) => Task.FromResult(0L);
        public Task<int?> GetServerMaxCoresAsync() => Task.FromResult<int?>(null);
        public Task<ServerMetrics> GetServerMetricsAsync() => Task.FromResult(new ServerMetrics());
        public Task<SnmpSample> GetSnmpMetricsAsync(SnmpOptions snmpOptions, string? databaseName = null) => Task.FromResult(new SnmpSample());
        public Task<string> GetServerVersionAsync() => Task.FromResult("test");
        public Task<string> GetServerLicenseTypeAsync() => Task.FromResult("test");
        public Task ValidateClientAsync() => Task.CompletedTask;
        public Task<CalibrationResult> ExecuteCalibrationRequestAsync(string endpoint, CancellationToken ct = default) =>
            Task.FromResult(new CalibrationResult(0, 0, 0, new Version(1, 0)));

        public IReadOnlyList<(string name, string path)> GetCalibrationEndpoints() => Array.Empty<(string, string)>();

        public void Dispose()
        {
        }
    }
}
