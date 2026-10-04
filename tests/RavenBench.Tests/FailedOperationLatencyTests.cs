using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using RavenBench.Core;
using RavenBench.Core.Metrics;
using RavenBench.Core.Transport;
using RavenBench.Core.Workload;
using Xunit;

namespace RavenBench.Tests;

public sealed class FailedOperationLatencyTests
{
    private static readonly OperationBase Op = new ReadOperation { Id = "users/1" };

    // The latency is scripted through the start timestamp, so no assertion depends on a delay.
    private static Task<WorkItemResult> Run(IYcsbTransport transport, LatencyRecorder recorder, int ageMs, CancellationToken ct = default) =>
        LoadGeneratorExecution.ExecuteOperationAsync(transport, Op, recorder,
            Stopwatch.GetTimestamp() - ageMs * Stopwatch.Frequency / 1000, ct);

    private static async Task<HistogramSnapshot> SlowOperationRun(Func<TransportResult> slowOutcome)
    {
        using var recorder = new LatencyRecorder(recordLatencies: true);
        for (var i = 0; i < 99; i++)
            await Run(new ScriptedTransport(() => new TransportResult(1, 1)), recorder, ageMs: 1);
        await Run(new ScriptedTransport(slowOutcome), recorder, ageMs: 500);
        return recorder.Snapshot();
    }

    [Fact]
    public async Task Failed_Operation_Adds_A_Sample_And_Cannot_Improve_The_Tail()
    {
        var failed = await SlowOperationRun(() => new TransportResult(0, 0, errorDetails: "503"));

        failed.TotalCount.Should().Be(100);
        failed.MaxMicros.Should().BeGreaterThanOrEqualTo(500_000);
        // The scripted 500 ms is a floor, so the tail holds it whatever the schedule adds.
        failed.GetPercentile(99.9).Should().BeGreaterThanOrEqualTo(500_000 * 0.99);
        var step = new RavenBench.Core.Reporting.StepResult();
        System.Text.Json.JsonSerializer.Serialize(step).Should().Contain("\"LatencySamples\":\"succeeded, failed and timed-out");
        RavenBench.Reporting.CsvMetrics.AllFields.Should().Contain(f => f.Name == nameof(step.LatencySamples));
    }

    [Fact]
    public async Task Timed_Out_Operation_Adds_A_Sample()
    {
        using var recorder = new LatencyRecorder(recordLatencies: true);
        var result = await Run(new ScriptedTransport(() => throw new TimeoutException("timed out")), recorder, ageMs: 500);

        result.IsError.Should().BeTrue();
        recorder.Snapshot().TotalCount.Should().Be(1);
    }

    [Fact]
    public async Task Cancelled_Operation_Adds_No_Sample()
    {
        using var recorder = new LatencyRecorder(recordLatencies: true);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        (await Run(new ScriptedTransport(() => throw new OperationCanceledException(cts.Token)), recorder, 500, cts.Token)).Cancelled.Should().BeTrue();
        (await Run(new ScriptedTransport(() => TransportResult.CancelledResult), recorder, 500)).Cancelled.Should().BeTrue();
        recorder.Snapshot().TotalCount.Should().Be(0);
    }

    [Fact]
    public async Task Latency_Above_The_Histogram_Limit_Ends_As_An_Error()
    {
        using var recorder = new LatencyRecorder(recordLatencies: true);
        var result = await Run(new ScriptedTransport(() => new TransportResult(1, 1)), recorder, ageMs: 7_200_000);

        result.IsError.Should().BeTrue();
    }

    private sealed class ScriptedTransport(Func<TransportResult> outcome) : IYcsbTransport
    {
        public string ProductName => "Stub";
        public bool ReportsWireBytes => true;
        public Task<TransportResult> ExecuteAsync(OperationBase op, CancellationToken ct) => Task.FromResult(outcome());
        public Task PutAsync<T>(string id, T document) => Task.CompletedTask;
        public Task EnsureDatabaseExistsAsync(string databaseName) => Task.CompletedTask;
        public Task<long> GetDocumentCountAsync(string idPrefix) => Task.FromResult(0L);
        public Task<string> GetServerVersionAsync() => Task.FromResult("test");
        public void Dispose() { }
    }
}
