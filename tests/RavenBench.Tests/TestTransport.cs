using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using RavenBench.Core.Diagnostics;
using RavenBench.Core.Metrics;
using RavenBench.Core.Metrics.Snmp;
using RavenBench.Core.Transport;
using RavenBench.Core.Workload;
using RavenBench.Core;

namespace RavenBench.Tests;

/// <summary>
/// Simple test transport implementation for testing purposes.
/// Provides predictable responses without mocking.
/// </summary>
public sealed class TestTransport : ITransport
{
    private readonly int _baseLatencyMs;
    private readonly ServerMetrics _serverMetrics;

    public TestTransport(int baseLatencyMs = 10)
    {
        _baseLatencyMs = baseLatencyMs;
        _serverMetrics = new ServerMetrics
        {
            CpuUsagePercent = 25.0,
            MemoryUsageMB = 512,
            ActiveConnections = 10,
            RequestsPerSecond = 100.0,
            QueuedRequests = 2,
            IoReadOperations = 50.0,
            IoWriteOperations = 30.0,
            ReadThroughputKb = 1024,
            WriteThroughputKb = 512,
            QueueLength = 1
        };
    }

    public void Dispose() { }

    public async Task<TransportResult> ExecuteAsync(OperationBase op, CancellationToken ct)
    {
        await Task.Delay(_baseLatencyMs, ct);
        return new TransportResult(bytesOut: 200, bytesIn: 150);
    }

    public Task PutAsync<T>(string id, T document) => Task.CompletedTask;

    public Task EnsureDatabaseExistsAsync(string databaseName) => Task.CompletedTask;

    public Task<long> GetDocumentCountAsync(string idPrefix) => Task.FromResult(0L);

    public Task<int?> GetServerMaxCoresAsync() => Task.FromResult<int?>(4);

    public Task<string> GetServerVersionAsync() => Task.FromResult("1.0.0-test");

    public Task<string> GetServerLicenseTypeAsync() => Task.FromResult("Developer");

    public Task ValidateClientAsync() => Task.CompletedTask;


    public Task<ServerMetrics> GetServerMetricsAsync() => Task.FromResult(_serverMetrics);

    public Task<List<string>> SampleDocumentIdsAsync(string idPrefix, int count, int seed)
    {
        // Test implementation: return synthetic keys
        var ids = new List<string>();
        for (int i = 0; i < count; i++)
        {
            ids.Add($"{idPrefix}{i:D8}");
        }
        return Task.FromResult(ids);
    }

    public Task<SnmpSample> GetSnmpMetricsAsync(SnmpOptions snmpOptions, string? databaseName = null)
    {
        var sample = new SnmpSample
        {
            MachineCpu = 50.0,
            ProcessCpu = 30.0,
            ManagedMemoryMb = 1024,
            UnmanagedMemoryMb = 512,
            DirtyMemoryMb = 128,
            Load1Min = 1.5,
            Load5Min = 1.2,
            Load15Min = 1.0
        };
        return Task.FromResult(sample);
    }

    public IReadOnlyList<(string name, string path)> GetCalibrationEndpoints() => new[]
    {
        ("server-version", "/build/version"),
        ("license-status", "/license/status")
    };

    public Task<StartupCalibration> CalibrateAsync(CancellationToken ct = default)
    {
        var calibration = new StartupCalibration
        {
            Endpoints = new[]
            {
                new EndpointCalibration
                {
                    Name = "server-version",
                    Url = "/build/version",
                    TtfbMs = 2.0,
                    ObservedMs = 5.0,
                    BytesDown = 100,
                    HttpVersion = "1.1"
                },
                new EndpointCalibration
                {
                    Name = "license-status",
                    Url = "/license/status",
                    TtfbMs = 3.0,
                    ObservedMs = 8.0,
                    BytesDown = 150,
                    HttpVersion = "1.1"
                }
            }
        };

        return Task.FromResult(calibration);
    }

    public async Task<CalibrationResult> ExecuteCalibrationRequestAsync(string endpoint, CancellationToken ct = default)
    {
        // Simulate network delay
        await Task.Delay(_baseLatencyMs, ct);

        // Return predictable calibration results based on endpoint
        return endpoint switch
        {
            "/build/version" => new CalibrationResult(2.0, 5.0, 100, HttpVersion.Version11),
            "/license/status" => new CalibrationResult(3.0, 8.0, 150, HttpVersion.Version11),
            _ => new CalibrationResult(2.5, 6.0, 125, HttpVersion.Version11)
        };
    }
}
