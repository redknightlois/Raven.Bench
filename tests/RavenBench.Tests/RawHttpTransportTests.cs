using System;
using System.Diagnostics;
using System.Net;
using System.Net.Http;
using System.Text.Json;
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

public class RawHttpTransportTests
{
    [Fact]
    public void WriteWorkload_Generates_Valid_JSON_Object_Payloads()
    {
        // INVARIANT: WriteWorkload payloads must be valid JSON objects, not double-serialized strings.

        var workload = new WriteWorkload(1024);
        var rng = new Random(42);
        var op = workload.NextOperation(rng);

        op.Should().BeOfType<InsertOperation<string>>();
        var insertOp = (InsertOperation<string>)op;

        // The payload should be valid JSON that parses to an object
        var payload = insertOp.Payload;
        var document = JsonDocument.Parse(payload);
        document.RootElement.ValueKind.Should().Be(JsonValueKind.Object);

        // Ensure it has the expected YCSB fields
        document.RootElement.EnumerateObject().Should().HaveCountGreaterThan(0);
        foreach (var property in document.RootElement.EnumerateObject())
        {
            property.Name.Should().MatchRegex("^field\\d+$");
            property.Value.ValueKind.Should().Be(JsonValueKind.String);
        }
    }

    [Fact]
    public async Task Zstd_Request_Body_Is_Encoded_And_Decodes_To_Original()
    {
        var json = "{\"field0\":\"hello zstd compression\",\"field1\":\"abcabcabcabcabcabc\"}";

        using var content = RawHttpTransport.ZstdJsonContent(System.Text.Encoding.UTF8.GetBytes(json));

        content.Headers.ContentEncoding.Should().Contain("zstd");
        var wire = await content.ReadAsByteArrayAsync();
        using var decompressor = new ZstdSharp.Decompressor();
        System.Text.Encoding.UTF8.GetString(decompressor.Unwrap(wire).ToArray()).Should().Be(json);
    }

    [Fact]
    public void CancelledResult_Is_Not_An_Error_And_Is_Flagged_Cancelled()
    {
        var result = TransportResult.CancelledResult;

        result.Cancelled.Should().BeTrue();
        result.IsSuccess.Should().BeTrue();
        result.ErrorDetails.Should().BeNull();
        result.BytesIn.Should().Be(0);
        result.BytesOut.Should().Be(0);
    }

    [Fact]
    public void TransportResult_Defaults_To_Not_Cancelled()
    {
        var result = new TransportResult(10, 20);

        result.Cancelled.Should().BeFalse();
        result.IsSuccess.Should().BeTrue();
    }

    [Fact]
    public async Task Cancelled_Operations_Are_Not_Recorded_As_Success_Error_Or_Latency()
    {
        using var transport = new CancelledTransport();
        using var latencyRecorder = new LatencyRecorder(recordLatencies: true);
        var counters = new LoadGeneratorCounters();

        var result = await LoadGeneratorExecution.ExecuteOperationAsync(
            transport,
            new ReadOperation { Id = "bench/1" },
            latencyRecorder,
            Stopwatch.GetTimestamp(),
            expectedIntervalMicros: 0,
            CancellationToken.None);

        result.Cancelled.Should().BeTrue();
        result.IsError.Should().BeFalse();

        counters.Record(result);
        counters.OperationsCompleted.Should().Be(0);
        counters.ErrorCount.Should().Be(0);
        counters.BytesIn.Should().Be(0);
        counters.BytesOut.Should().Be(0);

        latencyRecorder.Snapshot().TotalCount.Should().Be(0);
    }

    [Fact]
    public async Task Failed_Operations_Are_Recorded_As_Errors()
    {
        using var transport = new FailingTransport();
        using var latencyRecorder = new LatencyRecorder(recordLatencies: true);
        var counters = new LoadGeneratorCounters();

        var result = await LoadGeneratorExecution.ExecuteOperationAsync(
            transport,
            new ReadOperation { Id = "bench/1" },
            latencyRecorder,
            Stopwatch.GetTimestamp(),
            expectedIntervalMicros: 0,
            CancellationToken.None);

        result.Cancelled.Should().BeFalse();
        result.IsError.Should().BeTrue();

        counters.Record(result);
        counters.OperationsCompleted.Should().Be(1);
        counters.ErrorCount.Should().Be(1);
        latencyRecorder.Snapshot().TotalCount.Should().Be(0);
    }

    [Theory]
    [InlineData("1.0")]
    [InlineData("1.1")]
    [InlineData("2")]
    [InlineData("3")]
    public void ParseHttpVersion_Roundtrips_Known_Versions(string version)
    {
        HttpHelper.FormatHttpVersion(HttpHelper.ParseHttpVersion(version)).Should().Be(version);
    }

    [Fact]
    public void ParseHttpVersion_Throws_On_Unknown_Input()
    {
        var act = () => HttpHelper.ParseHttpVersion("bogus");
        act.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void GetRequestVersionInfo_Throws_On_Unknown_Input()
    {
        var act = () => HttpHelper.GetRequestVersionInfo("bogus");
        act.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void GetRequestVersionInfo_Auto_Allows_Downgrade()
    {
        var (version, policy) = HttpHelper.GetRequestVersionInfo("auto");

        version.Should().Be(HttpVersion.Version30);
        policy.Should().Be(HttpVersionPolicy.RequestVersionOrLower);
    }

    private sealed class CancelledTransport : StubTransport
    {
        public override Task<TransportResult> ExecuteAsync(OperationBase op, CancellationToken ct)
            => Task.FromResult(TransportResult.CancelledResult);
    }

    private sealed class FailingTransport : StubTransport
    {
        public override Task<TransportResult> ExecuteAsync(OperationBase op, CancellationToken ct)
            => Task.FromResult(new TransportResult(0, 0, "boom"));
    }

    private abstract class StubTransport : ITransport
    {
        public bool ReportsWireBytes => true;
        public abstract Task<TransportResult> ExecuteAsync(OperationBase op, CancellationToken ct);
        public Task PutAsync<T>(string id, T document) => Task.CompletedTask;
        public Task EnsureDatabaseExistsAsync(string databaseName) => Task.CompletedTask;
        public Task<long> GetDocumentCountAsync(string idPrefix) => Task.FromResult(0L);
        public Task<int?> GetServerMaxCoresAsync() => Task.FromResult<int?>(null);
        public Task<ServerMetrics> GetServerMetricsAsync() => Task.FromResult(new ServerMetrics());
        public Task<SnmpSample> GetSnmpMetricsAsync(SnmpOptions snmpOptions, string? databaseName = null)
            => Task.FromResult(new SnmpSample());
        public Task<string> GetServerVersionAsync() => Task.FromResult("test");
        public Task<string> GetServerLicenseTypeAsync() => Task.FromResult("test");
        public Task ValidateClientAsync() => Task.CompletedTask;
        public Task<CalibrationResult> ExecuteCalibrationRequestAsync(string endpoint, CancellationToken ct = default)
            => Task.FromResult(new CalibrationResult(0, 0, 0, HttpVersion.Version11));
        public System.Collections.Generic.IReadOnlyList<(string name, string path)> GetCalibrationEndpoints()
            => Array.Empty<(string, string)>();
        public void Dispose() { }
    }
}
