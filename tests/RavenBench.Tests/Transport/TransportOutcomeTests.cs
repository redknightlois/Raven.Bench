using System;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using RavenBench.Core;
using RavenBench.Core.Transport;
using RavenBench.Core.Workload;
using Xunit;

namespace RavenBench.Tests.Transport;

/// <summary>One outcome rule for every transport: a missing document and a cancelled run are never successes or errors by accident.</summary>
public class TransportOutcomeTests
{
    /// <summary>Answers every request with 404, the RavenDB answer for an absent document.</summary>
    private sealed class NotFoundHandler : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.NotFound) { Content = new StringContent("") });
        }
    }

    private static RavenClientTransport ClientTransport(HttpMessageHandler handler, bool mapEntities = false) =>
        new("http://127.0.0.1:1", "db", CompressionMode.Identity, HttpVersion.Version11, mapEntities, handler);

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RavenClient_Read_Of_An_Absent_Id_Is_A_NotFound_Failure(bool mapEntities)
    {
        using var handler = new NotFoundHandler();
        using var transport = ClientTransport(handler, mapEntities);

        var result = await transport.ExecuteAsync(new ReadOperation { Id = "users/missing" }, CancellationToken.None);

        result.IsSuccess.Should().BeFalse();
        result.NotFound.Should().BeTrue();
        result.Should().BeEquivalentTo(TransportResult.DocumentNotFound("users/missing"));
    }

    [Fact]
    public async Task RavenClient_Field_Update_Of_An_Absent_Id_Is_A_NotFound_Failure_Without_Byte_Counts()
    {
        using var handler = new NotFoundHandler();
        using var transport = ClientTransport(handler);

        var result = await transport.ExecuteAsync(new UpdateFieldOperation { Id = "users/missing", FieldName = "field0", Value = "v" }, CancellationToken.None);

        result.IsSuccess.Should().BeFalse();
        result.NotFound.Should().BeTrue();
        result.BytesIn.Should().Be(0);
        result.BytesOut.Should().Be(0);
    }

    [Theory]
    [InlineData(CompressionMode.Brotli)]
    [InlineData(CompressionMode.Deflate)]
    public void RavenClient_Refuses_A_Compression_It_Cannot_Apply(CompressionMode compression)
    {
        var act = () => new RavenClientTransport("http://127.0.0.1:1", "db", compression, HttpVersion.Version11);

        act.Should().Throw<NotSupportedException>().WithMessage($"*{compression.ToWireFormat()}*");
    }

    [Fact]
    public async Task Plain_OperationCanceledException_Is_Cancelled_Only_When_The_Run_Token_Is_Cancelled()
    {
        using var cts = new CancellationTokenSource();
        Task<TransportResult> Throwing() => throw new OperationCanceledException();

        var live = await TransportResult.GuardedAsync(Throwing, cts.Token);
        cts.Cancel();
        var cancelled = await TransportResult.GuardedAsync(Throwing, cts.Token);

        live.Cancelled.Should().BeFalse();
        live.IsSuccess.Should().BeFalse();
        cancelled.Cancelled.Should().BeTrue();
        cancelled.IsSuccess.Should().BeTrue();
    }

    [Theory]
    [InlineData("http://127.0.0.1:1", CompressionMode.Identity, "1.1")]
    [InlineData("https://127.0.0.1:1", CompressionMode.Identity, "1.1")]
    [InlineData("http://127.0.0.1:1", CompressionMode.Zstd, "1.1")]
    [InlineData("http://127.0.0.1:1", CompressionMode.Identity, "2")]
    [InlineData("http://127.0.0.1:1", CompressionMode.Gzip, "1.1")]
    public void Only_The_Socket_Path_Reports_Wire_Bytes(string url, CompressionMode compression, string httpVersion)
    {
        using var transport = new RawHttpTransport(url, "db", compression, HttpHelper.ParseHttpVersion(httpVersion));

        transport.ReportsWireBytes.Should().Be(transport.TransportPath == RawHttpTransport.SocketPath);
    }

    [Fact]
    public async Task Every_Pipelined_Sibling_Is_Cancelled_When_The_Run_Token_Cancels()
    {
        const int depth = 4;
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        try
        {
            using var transport = new RawHttpTransport($"http://127.0.0.1:{((IPEndPoint)listener.LocalEndpoint).Port}", "db",
                CompressionMode.Identity, HttpVersion.Version11, pipelineDepth: depth);
            using var cts = new CancellationTokenSource();
            var allSent = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var serverDone = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

            var server = Task.Run(async () =>
            {
                using var client = await listener.AcceptTcpClientAsync();
                var stream = client.GetStream();
                var seen = new StringBuilder();
                var buffer = new byte[4096];
                while (seen.ToString().Split("\r\n\r\n").Length - 1 < depth)
                {
                    var n = await stream.ReadAsync(buffer);
                    if (n == 0)
                        break;
                    seen.Append(Encoding.ASCII.GetString(buffer, 0, n));
                }
                allSent.SetResult();
                // Never answers: the client connection stays open until the test ends.
                await serverDone.Task;
            });

            var requests = Enumerable.Range(0, depth).Select(i => transport.ExecuteAsync(new ReadOperation { Id = $"d/{i}" }, cts.Token)).ToArray();
            await allSent.Task;
            cts.Cancel();
            var results = await Task.WhenAll(requests);
            serverDone.SetResult();
            await server;

            results.Should().OnlyContain(r => r.Cancelled, "a sibling cancelled on a shared connection is still a cancellation, not an error");
            transport.OpenedSocketConnections.Should().Be(1);
        }
        finally
        {
            listener.Stop();
        }
    }
}
