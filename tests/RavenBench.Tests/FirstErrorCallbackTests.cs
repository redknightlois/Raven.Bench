using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using RavenBench.Core;
using RavenBench.Core.Metrics;
using RavenBench.Core.Transport;
using RavenBench.Core.Workload;
using Xunit;

namespace RavenBench.Tests;

// The callback and its tracking are static, so these tests run one at a time.
[Collection("FirstErrorCallback")]
public sealed class FirstErrorCallbackTests : IDisposable
{
    private readonly ConcurrentQueue<string> _reported = new();
    private readonly string _tag = Guid.NewGuid().ToString("N");

    public FirstErrorCallbackTests()
    {
        LoadGeneratorExecution.ResetErrorTracking();
        LoadGeneratorExecution.OnFirstError = message =>
        {
            if (message.Contains(_tag, StringComparison.Ordinal))
                _reported.Enqueue(message);
        };
    }

    public void Dispose()
    {
        LoadGeneratorExecution.OnFirstError = null;
        LoadGeneratorExecution.ResetErrorTracking();
    }

    private static async Task Fail(string message)
    {
        using var recorder = new LatencyRecorder(recordLatencies: true);
        await LoadGeneratorExecution.ExecuteOperationAsync(new FailingTransport(message), new ReadOperation { Id = "users/1" }, recorder, Stopwatch.GetTimestamp(), CancellationToken.None);
    }

    [Fact]
    public async Task Each_Distinct_Message_Reaches_The_Callback_Once()
    {
        await Fail($"A {_tag}");
        await Fail($"B {_tag}");
        await Fail($"A {_tag}");
        await Fail($"B {_tag}");

        _reported.Should().Equal($"A {_tag}", $"B {_tag}");
    }

    [Fact]
    public async Task A_Message_Reaches_The_Callback_Again_After_A_Reset()
    {
        await Fail($"A {_tag}");
        LoadGeneratorExecution.ResetErrorTracking();
        await Fail($"A {_tag}");

        _reported.Should().HaveCount(2);
    }

    [Fact]
    public async Task Concurrent_Workers_Failing_With_The_Same_Message_Produce_One_Callback()
    {
        await Task.WhenAll(Enumerable.Range(0, 64).Select(_ => Task.Run(() => Fail($"same {_tag}"))));

        _reported.Should().ContainSingle();
    }

    private sealed class FailingTransport(string message) : IYcsbTransport
    {
        public string ProductName => "Stub";
        public bool ReportsWireBytes => true;
        public string RecordedEndpoint => "stub";
        public Task<TransportResult> ExecuteAsync(OperationBase op, CancellationToken ct) => Task.FromResult(new TransportResult(0, 0, errorDetails: message));
        public Task PutAsync<T>(string id, T document) => Task.CompletedTask;
        public Task EnsureDatabaseExistsAsync(string databaseName) => Task.CompletedTask;
        public Task<long> GetDocumentCountAsync(string idPrefix) => Task.FromResult(0L);
        public Task<string> GetServerVersionAsync() => Task.FromResult("test");
        public void Dispose() { }
    }
}
