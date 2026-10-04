using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using RavenBench.Core;
using RavenBench.Core.Metrics;
using RavenBench.Core.Reporting;
using RavenBench.Core.Transport;
using RavenBench.Core.Workload;
using RavenBench.Dataset;
using Xunit;

namespace RavenBench.Tests.Metrics;

public class NodeExporterTests
{
    // Two CPUs; counters in the exposition format node_exporter 1.x writes.
    private static string Scrape(double idle0, double user0, double idle1, double user1, long totalBytes = 8L << 30, long availableBytes = 6L << 30) => $$"""
        # HELP node_cpu_seconds_total Seconds the CPUs spent in each mode.
        # TYPE node_cpu_seconds_total counter
        node_cpu_seconds_total{cpu="0",mode="idle"} {{idle0}}
        node_cpu_seconds_total{cpu="0",mode="user"} {{user0}}
        node_cpu_seconds_total{cpu="0",mode="iowait"} 1.5
        node_cpu_seconds_total{cpu="1",mode="idle"} {{idle1}}
        node_cpu_seconds_total{cpu="1",mode="user"} {{user1}}
        node_cpu_seconds_total{cpu="1",mode="iowait"} 2.5
        # TYPE node_memory_MemTotal_bytes gauge
        node_memory_MemTotal_bytes {{totalBytes}}
        # TYPE node_memory_MemAvailable_bytes gauge
        node_memory_MemAvailable_bytes {{availableBytes}}
        node_load1 0.5
        """;

    [Fact]
    public void Cpu_Is_The_Non_Idle_Share_Of_All_Cpus_Between_Two_Scrapes_In_Percent()
    {
        var start = NodeExporterSample.Parse(Scrape(100, 10, 200, 20));
        // Deltas: cpu0 idle 6 user 4, cpu1 idle 9 user 1 -> non-idle 5 of 20.
        var end = NodeExporterSample.Parse(Scrape(106, 14, 209, 21));

        NodeExporterSample.CpuPercent(start, end).Should().BeApproximately(25.0, 1e-9);
    }

    [Fact]
    public void Memory_Is_Total_Minus_Available_In_MiB()
    {
        NodeExporterSample.Parse(Scrape(1, 1, 1, 1)).UsedMemoryMB.Should().Be(2048);
    }

    [Fact]
    public void Disk_Written_Bytes_Sum_Every_Device_And_Are_Null_Without_The_Series()
    {
        var withDisks = Scrape(1, 1, 1, 1) + """

            node_disk_written_bytes_total{device="sda"} 1000
            node_disk_written_bytes_total{device="nvme0n1"} 234
            """;

        NodeExporterSample.Parse(withDisks).DiskWrittenBytes.Should().Be(1234);
        NodeExporterSample.Parse(Scrape(1, 1, 1, 1)).DiskWrittenBytes.Should().BeNull();
    }

    [Theory]
    [InlineData(NodeExporterSample.CpuSeries)]
    [InlineData(NodeExporterSample.MemTotalSeries)]
    [InlineData(NodeExporterSample.MemAvailableSeries)]
    public void A_Missing_Series_Is_Reported_By_Name(string series)
    {
        var text = string.Join('\n', Scrape(1, 1, 1, 1).Split('\n').Where(l => l.TrimStart().StartsWith(series) == false));

        var act = () => NodeExporterSample.Parse(text);

        act.Should().Throw<NodeExporterException>().WithMessage($"*{series} missing*");
    }

    [Fact]
    public void Zero_Elapsed_Cpu_Time_Is_Refused_Not_Zero()
    {
        var sample = NodeExporterSample.Parse(Scrape(1, 1, 1, 1));

        var act = () => NodeExporterSample.CpuPercent(sample, sample);

        act.Should().Throw<NodeExporterException>().WithMessage("*no CPU time elapsed*");
    }

    [Fact]
    public void A_Counter_Reset_Is_Refused_By_Name()
    {
        var act = () => NodeExporterSample.CpuPercent(NodeExporterSample.Parse(Scrape(100, 10, 200, 20)), NodeExporterSample.Parse(Scrape(1, 10, 200, 20)));

        act.Should().Throw<NodeExporterException>().WithMessage("*node_cpu_seconds_total*reset*");
    }

    [Fact]
    public async Task An_Http_Error_Fails_The_Scrape_By_Source_Name()
    {
        using var client = new NodeExporterClient(new Uri("http://dbhost:9100/metrics"), new QueuedHandler(new QueuedResponse(HttpStatusCode.InternalServerError, "")));

        var act = () => client.ScrapeAsync();

        (await act.Should().ThrowAsync<NodeExporterException>()).WithMessage("node_exporter:*HTTP 500*");
    }

    [Fact]
    public async Task A_Step_Names_Node_Exporter_As_Its_Source_And_Marks_It_Host_Wide()
    {
        using var nodeExporter = new NodeExporterClient(new Uri("http://dbhost:9100/metrics"), new QueuedHandler(
            new QueuedResponse(HttpStatusCode.OK, Scrape(100, 10, 200, 20)),
            new QueuedResponse(HttpStatusCode.OK, Scrape(106, 14, 209, 21))));

        var step = await RunOneStep(nodeExporter);

        step.ServerCpu.Should().BeApproximately(25.0, 1e-9);
        step.ServerMemoryMB.Should().Be(2048);
        step.ServerCpuSource.Should().Be("node_exporter");
        step.ServerMemorySource.Should().Be("node_exporter");
        step.ServerMetricsHostWide.Should().BeTrue();

        using var json = JsonDocument.Parse(JsonSerializer.Serialize(step));
        json.RootElement.GetProperty(nameof(StepResult.ServerCpuSource)).GetString().Should().Be("node_exporter");
        json.RootElement.GetProperty(nameof(StepResult.ServerMetricsHostWide)).GetBoolean().Should().BeTrue();
        json.RootElement.GetProperty(nameof(StepResult.ServerCpuBasis)).GetString().Should().Be("step average over all host CPUs");
        RavenBench.Reporting.CsvMetrics.AllFields.Single(f => f.Name == nameof(StepResult.ServerCpuBasis)).ValueSelector(step).Should().Be(step.ServerCpuBasis);

        var columns = ServerColumnAvailability.FromSteps("PostgreSQL", new[] { step });
        columns.Columns.Should().Contain(new[] { nameof(StepResult.ServerCpu), nameof(StepResult.ServerMemoryMB) });
        columns.Sources[nameof(StepResult.ServerCpu)].Should().Be("node_exporter (host-wide)");
    }

    [Fact]
    public async Task A_Step_Whose_Scrape_Lacks_The_Cpu_Series_States_The_Reason_And_Records_No_Figure()
    {
        using var nodeExporter = new NodeExporterClient(new Uri("http://dbhost:9100/metrics"), new QueuedHandler(
            new QueuedResponse(HttpStatusCode.OK, Scrape(100, 10, 200, 20)),
            new QueuedResponse(HttpStatusCode.OK, "node_memory_MemTotal_bytes 1\nnode_memory_MemAvailable_bytes 1\n")));

        var step = await RunOneStep(nodeExporter);

        step.ServerCpu.Should().BeNull();
        step.ServerMemoryMB.Should().BeNull();
        step.ServerCpuSource.Should().BeNull();
        step.ServerMetricsUnavailable.Should().Be("node_exporter: series node_cpu_seconds_total missing.");
        ServerColumnAvailability.FromSteps("PostgreSQL", new[] { step }).Statement.Should().Be("PostgreSQL has no server column from this harness run.");
    }

    [Fact]
    public async Task A_Recall_Row_Carries_The_Node_Exporter_Columns_In_Its_Json()
    {
        using var nodeExporter = new NodeExporterClient(new Uri("http://dbhost:9100/metrics"), new QueuedHandler(
            new QueuedResponse(HttpStatusCode.OK, Scrape(100, 10, 200, 20)),
            new QueuedResponse(HttpStatusCode.OK, Scrape(106, 14, 209, 21))));
        var metadata = new VectorWorkloadMetadata
        {
            FieldName = "Vector",
            QueryVectors = [[1f, 0f]],
            GroundTruth = new Dictionary<int, string[]> { [0] = ["a", "b"] },
            DocumentIdPrefix = "v/"
        };

        var row = await new RecallMeasurement().MeasureAsync(new TruthTransport(["v/a", "v/b"]), metadata, [2], VectorQuantization.None, effort: null, nodeExporter);

        row.RecallAtK[2].Should().Be(1.0);
        using var json = JsonDocument.Parse(JsonSerializer.Serialize(row));
        json.RootElement.GetProperty(nameof(StepResult.ServerCpu)).GetDouble().Should().BeApproximately(25.0, 1e-9);
        json.RootElement.GetProperty(nameof(StepResult.ServerMemoryMB)).GetInt64().Should().Be(2048);
        json.RootElement.GetProperty(nameof(StepResult.ServerCpuSource)).GetString().Should().Be("node_exporter");
        json.RootElement.GetProperty(nameof(StepResult.ServerMetricsHostWide)).GetBoolean().Should().BeTrue();
    }

    private sealed class TruthTransport(IReadOnlyList<string> ids) : IYcsbTransport
    {
        public string ProductName => "stub";
        public bool ReportsWireBytes => false;
        public string RecordedEndpoint => "stub";
        public Task<TransportResult> ExecuteAsync(OperationBase op, CancellationToken ct) =>
            Task.FromResult(new TransportResult(0, 0, resultCount: ids.Count) { NeighborIds = ids });
        public Task PutAsync<T>(string id, T document) => throw new NotSupportedException();
        public Task EnsureDatabaseExistsAsync(string databaseName) => throw new NotSupportedException();
        public Task<long> GetDocumentCountAsync(string idPrefix) => throw new NotSupportedException();
        public Task<string> GetServerVersionAsync() => throw new NotSupportedException();
        public void Dispose() { }
    }

    private static async Task<StepResult> RunOneStep(NodeExporterClient nodeExporter)
    {
        using var transport = new TestTransport(baseLatencyMs: 1);
        var workload = new MixedProfileWorkload(WorkloadMix.FromWeights(0, 100, 0), new UniformDistribution(), 1024, seed: 42);
        var options = new RunOptions
        {
            Url = "http://localhost:8081",
            Database = "bench",
            Distribution = KeyDistributionKind.Uniform,
            Transport = TransportKind.Raw,
            Compression = CompressionMode.Identity,
            Warmup = TimeSpan.Zero,
            Duration = TimeSpan.FromMilliseconds(100),
            Step = new StepPlan(1, 1, 1),
            Profile = WorkloadProfile.Mixed
        };
        var executor = new BenchmarkExecutor(options, transport, workload, new ProcessCpuTracker(), nodeExporter: nodeExporter);
        var (_, step) = await executor.ExecuteStepAsync(new ClosedLoopLoadGenerator(transport, workload, 1, new Random(42)), 0, 1, CancellationToken.None);
        return step;
    }

    private sealed record QueuedResponse(HttpStatusCode Status, string Body);

    private sealed class QueuedHandler(params QueuedResponse[] responses) : HttpMessageHandler
    {
        private readonly Queue<QueuedResponse> _responses = new(responses);

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var next = _responses.Dequeue();
            return Task.FromResult(new HttpResponseMessage(next.Status) { Content = new StringContent(next.Body) });
        }
    }
}
