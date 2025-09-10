using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using RavenBench;
using RavenBench.Metrics;
using RavenBench.Reporting;
using RavenBench.Transport;
using RavenBench.Util;
using RavenBench.Workload;
using Xunit;

namespace RavenBench.Tests;

public class ClosedLoopRampTests
{
    [Fact]
    public async Task Ramp_Collects_Steps_And_Keeps_Invariants()
    {
        var opts = new RunOptions
        {
            Url = "http://localhost:10101",
            Database = "ycsb",
            Writes = 100,
            Distribution = "uniform",
            Compression = "raw:identity",
            Warmup = TimeSpan.FromMilliseconds(50),
            Duration = TimeSpan.FromMilliseconds(100),
            ConcurrencyStart = 2,
            ConcurrencyEnd = 8,
            ConcurrencyFactor = 2,
            OutJson = null,
            OutCsv = null,
        };

        var runner = new BenchmarkRunnerForTest(opts, new MixedWorkload(WorkloadMix.FromWeights(0, 100, 0), new UniformDistribution(), 1024));
        var run = await runner.RunAsyncWithTransport(new StubTransport(baseLatencyMs: 1));

        run.Steps.Count.Should().BeGreaterOrEqualTo(2);
        run.Steps.All(s => s.Throughput >= 0).Should().BeTrue();
        run.Steps.All(s => s.ErrorRate >= 0 && s.ErrorRate <= 1).Should().BeTrue();
    }

    private sealed class BenchmarkRunnerForTest : BenchmarkRunner
    {
        private readonly IWorkload _w;
        public BenchmarkRunnerForTest(RunOptions opts, IWorkload w) : base(opts) { _w = w; }
        public async Task<BenchmarkRun> RunAsyncWithTransport(ITransport transport)
        {
            var steps = new List<StepResult>();
            var concurrency = 2;
            var tracker = new RavenBench.Metrics.ProcessCpuTracker();
            using var serverTracker = new RavenBench.Metrics.ServerMetricsTracker(transport);
            var context = new BenchmarkContext
            {
                Transport = transport,
                Workload = _w,
                CpuTracker = tracker,
                ServerTracker = serverTracker,
                Rng = new Random(42)
            };
            
            while (concurrency <= 8)
            {
                _ = await RunClosedLoopAsync(context, new StepParameters 
                { 
                    Concurrency = concurrency, 
                    Duration = TimeSpan.FromMilliseconds(50), 
                    Record = false 
                });
                var (res, _) = await RunClosedLoopAsync(context, new StepParameters 
                { 
                    Concurrency = concurrency, 
                    Duration = TimeSpan.FromMilliseconds(100), 
                    Record = true 
                });
                steps.Add(res);
                concurrency *= 2;
            }
            return new BenchmarkRun { Steps = steps, MaxNetworkUtilization = 0, ClientCompression = "identity", EffectiveHttpVersion = "1.1" };
        }
    }

    private sealed class StubTransport : ITransport
    {
        private readonly int _latencyMs;
        public StubTransport(int baseLatencyMs) => _latencyMs = baseLatencyMs;
        public void Dispose() { }
        public Task<int?> GetServerMaxCoresAsync() => Task.FromResult<int?>(null);
        public Task<ServerMetrics> GetServerMetricsAsync() => Task.FromResult(new ServerMetrics 
        { 
            CpuUsagePercent = 25.0, 
            MemoryUsageMB = 512, 
            ActiveConnections = 10, 
            RequestsPerSecond = 100.0 
        });
        public Task PutAsync(string id, string json) => Task.CompletedTask;
        public async Task<TransportResult> ExecuteAsync(Operation op, CancellationToken ct)
        {
            await Task.Delay(_latencyMs, ct);
            return new TransportResult(bytesOut: 100, bytesIn: 100);
        }
    }
}
