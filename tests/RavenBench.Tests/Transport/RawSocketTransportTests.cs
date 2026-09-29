using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using RavenBench.Cli;
using RavenBench.Core;
using RavenBench.Core.Transport;
using RavenBench.Core.Workload;
using RavenBench.Tests.Infrastructure;
using Xunit;

namespace RavenBench.Tests.Transport;

public class QueryEnvelopeTests
{
    [Theory]
    [InlineData("stale.json")]
    [InlineData("query.json")]
    [InlineData("facet.json")]
    [InlineData("vector.json")]
    [InlineData("stream.json")]
    public void Forward_Reader_Matches_The_Document_Parser_On_Recorded_Responses(string file)
    {
        var bytes = File.ReadAllBytes(Path.Combine(AppContext.BaseDirectory, "Transport", "Recorded", file));
        using var doc = JsonDocument.Parse(bytes);
        var root = doc.RootElement;

        var envelope = QueryEnvelope.Read(bytes);

        envelope.IndexName.Should().Be(root.TryGetProperty("IndexName", out var i) ? i.GetString() : null);
        envelope.ResultCount.Should().Be(root.TryGetProperty("Results", out var r) && r.ValueKind == JsonValueKind.Array ? r.GetArrayLength() : null);
        envelope.IsStale.Should().Be(root.TryGetProperty("IsStale", out var s) ? s.GetBoolean() : null);
    }

    [Fact]
    public void Stale_Recording_Reads_As_Stale()
    {
        var bytes = File.ReadAllBytes(Path.Combine(AppContext.BaseDirectory, "Transport", "Recorded", "stale.json"));
        QueryEnvelope.Read(bytes).IsStale.Should().BeTrue();
    }

    [Fact]
    public void Nested_Fields_Do_Not_Shadow_Top_Level_Ones()
    {
        var envelope = QueryEnvelope.Read("{\"Results\":[{\"IndexName\":\"inner\",\"IsStale\":true}],\"IndexName\":\"outer\"}"u8);

        envelope.Should().Be(new QueryEnvelope("outer", 1, null));
    }

    [Theory]
    [InlineData("[]")]
    [InlineData("{\"Results\":[1,2}")]
    [InlineData("{\"IsStale\":true} {}")]
    [InlineData("{\"Results\":[]")]
    public void Malformed_Bodies_Throw(string body)
    {
        var act = () => QueryEnvelope.Read(Encoding.UTF8.GetBytes(body));
        act.Should().Throw<JsonException>();
    }

    [Theory]
    [InlineData("{\"IsStale\":\"yes\"}")]
    [InlineData("{\"IndexName\":5}")]
    public void Wrong_Types_Throw(string body)
    {
        var act = () => QueryEnvelope.Read(Encoding.UTF8.GetBytes(body));
        act.Should().Throw<InvalidOperationException>();
    }
}

public class RequestBodyTests
{
    private static string Body(OperationBase op)
    {
        using var transport = new RawHttpTransport("http://127.0.0.1:1", "db", CompressionMode.Identity, HttpVersion.Version11);
        return Encoding.UTF8.GetString(transport.Describe(op).Body.Span);
    }

    [Fact]
    public void Query_Body_Equals_The_Serializer_Body()
    {
        var parameters = new Dictionary<string, object?> { ["id"] = "users/1", ["n"] = 42, ["f"] = 1.5, ["b"] = true, ["x"] = null };
        var op = new QueryOperation { QueryText = "from Users where id() = $id", Parameters = parameters };

        Body(op).Should().Be(JsonSerializer.Serialize(new { Query = op.QueryText, QueryParameters = parameters, MetadataOnly = false }));
    }

    [Fact]
    public void Patch_Body_Carries_The_Fixed_Script_And_Arguments()
    {
        using var doc = JsonDocument.Parse(Body(new UpdateFieldOperation { Id = "d/1", FieldName = "field3", Value = "v\"x" }));
        var patch = doc.RootElement.GetProperty("Patch");

        patch.GetProperty("Script").GetString().Should().Be("this[args.field] = args.value;");
        patch.GetProperty("Values").GetProperty("field").GetString().Should().Be("field3");
        patch.GetProperty("Values").GetProperty("value").GetString().Should().Be("v\"x");
    }
}

public class PipelineDepthValidationTests
{
    [Fact]
    public void Depth_Defaults_To_One_On_The_Socket_Path()
    {
        using var transport = new RawHttpTransport("http://127.0.0.1:1", "db", CompressionMode.Identity, HttpVersion.Version11);

        transport.PipelineDepth.Should().Be(1);
        transport.TransportPath.Should().Be(RawHttpTransport.SocketPath);
    }

    [Fact]
    public void Paths_The_Socket_Cannot_Serve_Use_HttpClient_And_Reject_Pipelining()
    {
        using (var zstd = new RawHttpTransport("http://127.0.0.1:1", "db", CompressionMode.Zstd, HttpVersion.Version11))
            zstd.TransportPath.Should().Be(RawHttpTransport.HttpClientPath);

        var act = () => new RawHttpTransport("http://127.0.0.1:1", "db", CompressionMode.Identity, HttpVersion.Version20, pipelineDepth: 2);
        act.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void Cli_Rejects_Invalid_Depth_And_Unsupported_Transport()
    {
        CliParsing.ParsePipelineDepth(4, TransportKind.Raw).Should().Be(4);
        ((Action)(() => CliParsing.ParsePipelineDepth(0, TransportKind.Raw))).Should().Throw<ArgumentException>();
        ((Action)(() => CliParsing.ParsePipelineDepth(2, TransportKind.Client))).Should().Throw<ArgumentException>();
    }
}

/// <summary>Drives the socket path against a scripted HTTP/1.1 server that writes exact bytes.</summary>
public class RawSocketScriptedServerTests
{
    private static RawHttpTransport Transport(TcpListener listener, int depth = 1) =>
        new($"http://127.0.0.1:{((IPEndPoint)listener.LocalEndpoint).Port}", "db", CompressionMode.Identity, HttpVersion.Version11, pipelineDepth: depth);

    private static TcpListener Listen()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        return listener;
    }

    private static ReadOperation Read(int i) => new() { Id = $"d/{i}" };

    /// <summary>Reads until <paramref name="count"/> bodiless request heads arrived; returns the bytes read.</summary>
    private static async Task<int> ReadRequests(NetworkStream stream, int count)
    {
        var seen = new List<byte>();
        var buffer = new byte[4096];
        while (CountHeads(seen) < count)
        {
            var n = await stream.ReadAsync(buffer);
            if (n == 0)
                throw new IOException("client closed");
            seen.AddRange(buffer.Take(n));
        }
        return seen.Count;
    }

    private static int CountHeads(List<byte> bytes) =>
        Encoding.ASCII.GetString(bytes.ToArray()).Split("\r\n\r\n").Length - 1;

    private static byte[] Ok(string body) =>
        Encoding.ASCII.GetBytes($"HTTP/1.1 200 OK\r\nContent-Length: {body.Length}\r\n\r\n{body}");

    [Fact]
    public async Task Depth_Four_Writes_Four_Requests_Before_Any_Response_And_Counts_Wire_Bytes()
    {
        using var listener = Listen();
        using var transport = Transport(listener, depth: 4);
        var responses = Enumerable.Range(0, 4).Select(i => Ok($"{{\"n\":{i}}}")).ToArray();

        var server = Task.Run(async () =>
        {
            using var client = await listener.AcceptTcpClientAsync();
            var stream = client.GetStream();
            var read = await ReadRequests(stream, 4);
            foreach (var r in responses)
                await stream.WriteAsync(r);
            await Task.Delay(200);
            return read;
        });

        var results = await Task.WhenAll(Enumerable.Range(0, 4).Select(i => transport.ExecuteAsync(Read(i), CancellationToken.None)));
        var requestBytes = await server;

        results.Should().OnlyContain(r => r.IsSuccess);
        results.Sum(r => r.BytesIn).Should().Be(responses.Sum(r => r.Length));
        results.Sum(r => r.BytesOut).Should().Be(requestBytes);
        transport.OpenedSocketConnections.Should().Be(1);
    }

    [Fact]
    public async Task Chunked_Response_Split_Into_Single_Bytes_Is_Read_To_Its_End()
    {
        using var listener = Listen();
        using var transport = Transport(listener);
        var wire = Encoding.ASCII.GetBytes("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\na;ext=1\r\n0123456789\r\n0\r\nX-T: 1\r\n\r\n");

        var server = Task.Run(async () =>
        {
            using var client = await listener.AcceptTcpClientAsync();
            client.NoDelay = true;
            var stream = client.GetStream();
            await ReadRequests(stream, 1);
            foreach (var b in wire)
            {
                await stream.WriteAsync(new[] { b });
                await stream.FlushAsync();
            }
            await ReadRequests(stream, 1);
            await stream.WriteAsync(Ok("{}"));
            await Task.Delay(200);
        });

        var first = await transport.ExecuteAsync(Read(1), CancellationToken.None);
        var second = await transport.ExecuteAsync(Read(2), CancellationToken.None);
        await server;

        first.IsSuccess.Should().BeTrue(first.ErrorDetails);
        first.BytesIn.Should().Be(wire.Length);
        second.IsSuccess.Should().BeTrue(second.ErrorDetails);
        transport.OpenedSocketConnections.Should().Be(1);
    }

    [Fact]
    public async Task Close_Mid_Response_Is_An_Error_And_The_Next_Request_Reconnects()
    {
        using var listener = Listen();
        using var transport = Transport(listener);

        var server = Task.Run(async () =>
        {
            using (var first = await listener.AcceptTcpClientAsync())
            {
                var stream = first.GetStream();
                await ReadRequests(stream, 1);
                await stream.WriteAsync(Encoding.ASCII.GetBytes("HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\npartial"));
            }
            using var second = await listener.AcceptTcpClientAsync();
            var s2 = second.GetStream();
            await ReadRequests(s2, 1);
            await s2.WriteAsync(Ok("{}"));
            await Task.Delay(200);
        });

        var broken = await transport.ExecuteAsync(Read(1), CancellationToken.None);
        var next = await transport.ExecuteAsync(Read(2), CancellationToken.None);
        await server;

        broken.IsSuccess.Should().BeFalse();
        next.IsSuccess.Should().BeTrue(next.ErrorDetails);
        transport.OpenedSocketConnections.Should().Be(2);
    }

    [Fact]
    public async Task Connection_Close_Response_Succeeds_And_Is_Not_Reused()
    {
        using var listener = Listen();
        using var transport = Transport(listener);

        var server = Task.Run(async () =>
        {
            for (int i = 0; i < 2; i++)
            {
                using var client = await listener.AcceptTcpClientAsync();
                var stream = client.GetStream();
                await ReadRequests(stream, 1);
                await stream.WriteAsync(Encoding.ASCII.GetBytes("HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 2\r\n\r\n{}"));
            }
        });

        var first = await transport.ExecuteAsync(Read(1), CancellationToken.None);
        var second = await transport.ExecuteAsync(Read(2), CancellationToken.None);
        await server;

        first.IsSuccess.Should().BeTrue(first.ErrorDetails);
        second.IsSuccess.Should().BeTrue(second.ErrorDetails);
        transport.OpenedSocketConnections.Should().Be(2);
    }

    [Theory]
    [InlineData("HTTP/1.1 200 OK\r\nContent-Length: 2\r\nTransfer-Encoding: chunked\r\n\r\n{}")]
    [InlineData("HTTP/1.1 200 OK\r\nContent-Length: 2\r\nContent-Length: 3\r\n\r\n{}")]
    [InlineData("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nzz\r\n")]
    [InlineData("HTTP/1.1 200 OK\r\n\r\n{}")]
    [InlineData("garbage\r\n\r\n")]
    public async Task Malformed_Responses_Fail_The_Request(string response)
    {
        using var listener = Listen();
        using var transport = Transport(listener);

        var server = Task.Run(async () =>
        {
            using var client = await listener.AcceptTcpClientAsync();
            var stream = client.GetStream();
            await ReadRequests(stream, 1);
            await stream.WriteAsync(Encoding.ASCII.GetBytes(response));
            await Task.Delay(200);
        });

        var result = await transport.ExecuteAsync(Read(1), CancellationToken.None);
        await server;

        result.IsSuccess.Should().BeFalse();
    }

    [Fact]
    public async Task Close_With_Pipelined_Requests_In_Flight_Fails_Only_The_Unanswered_Ones()
    {
        using var listener = Listen();
        using var transport = Transport(listener, depth: 4);

        var server = Task.Run(async () =>
        {
            using (var first = await listener.AcceptTcpClientAsync())
            {
                var stream = first.GetStream();
                await ReadRequests(stream, 4);
                await stream.WriteAsync(Ok("{}"));
            }
            using var second = await listener.AcceptTcpClientAsync();
            var s2 = second.GetStream();
            await ReadRequests(s2, 1);
            await s2.WriteAsync(Ok("{}"));
            await Task.Delay(200);
        });

        var batch = await Task.WhenAll(Enumerable.Range(0, 4).Select(i => transport.ExecuteAsync(Read(i), CancellationToken.None)));
        var next = await transport.ExecuteAsync(Read(5), CancellationToken.None);
        await server;

        // The four requests wait on one connect, so their order on the wire is not their call order.
        batch.Count(r => r.IsSuccess).Should().Be(1);
        batch.Where(r => r.IsSuccess == false).Should().HaveCount(3).And.OnlyContain(r => r.Cancelled == false);
        next.IsSuccess.Should().BeTrue(next.ErrorDetails);
        transport.OpenedSocketConnections.Should().Be(2);
    }

    [Fact]
    public async Task A_Server_That_Never_Answers_Times_The_Request_Out()
    {
        using var listener = Listen();
        using var transport = Transport(listener);

        var server = Task.Run(async () =>
        {
            using var client = await listener.AcceptTcpClientAsync();
            await ReadRequests(client.GetStream(), 1);
            await Task.Delay(TimeSpan.FromSeconds(40));
        });

        var started = Stopwatch.StartNew();
        var result = await transport.ExecuteAsync(Read(1), CancellationToken.None);

        result.IsSuccess.Should().BeFalse();
        result.Cancelled.Should().BeFalse();
        result.ErrorDetails.Should().Contain("timed out");
        started.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(33));
    }

    [Fact]
    public async Task Cancelled_Request_Retires_Its_Connection()
    {
        using var listener = Listen();
        using var transport = Transport(listener);
        using var cts = new CancellationTokenSource();

        var server = Task.Run(async () =>
        {
            using (var first = await listener.AcceptTcpClientAsync())
            {
                await ReadRequests(first.GetStream(), 1);
                cts.Cancel();
                using var second = await listener.AcceptTcpClientAsync();
                var stream = second.GetStream();
                await ReadRequests(stream, 1);
                await stream.WriteAsync(Ok("{}"));
                await Task.Delay(200);
            }
        });

        var cancelled = await transport.ExecuteAsync(Read(1), cts.Token);
        var next = await transport.ExecuteAsync(Read(2), CancellationToken.None);
        await server;

        cancelled.Cancelled.Should().BeTrue();
        next.IsSuccess.Should().BeTrue(next.ErrorDetails);
        transport.OpenedSocketConnections.Should().Be(2);
    }

    [Fact]
    public async Task Cancellation_Completes_As_Cancelled()
    {
        using var listener = Listen();
        using var transport = Transport(listener);
        using var cts = new CancellationTokenSource();

        var server = Task.Run(async () =>
        {
            using var client = await listener.AcceptTcpClientAsync();
            await ReadRequests(client.GetStream(), 1);
            cts.Cancel();
            await Task.Delay(200);
        });

        var result = await transport.ExecuteAsync(Read(1), cts.Token);
        await server;

        result.Cancelled.Should().BeTrue();
    }
}

public class RawSocketLiveServerTests : EmbeddedRavenTestBase
{
    [Fact]
    public async Task Every_Raw_Operation_Succeeds_On_The_Socket_Path_Over_One_Connection()
    {
        using var store = GetDocumentStore();
        using var transport = new RawHttpTransport(store.Urls[0], store.Database, CompressionMode.Identity, HttpVersion.Version11);
        transport.TransportPath.Should().Be(RawHttpTransport.SocketPath);

        var doc = "{\"Name\":\"a\",\"Vec\":[0.1,0.2,0.3],\"@metadata\":{\"@collection\":\"Items\"}}";
        var ops = new OperationBase[]
        {
            new InsertOperation<string> { Id = "items/1", Payload = doc },
            new ReadOperation { Id = "items/1" },
            new UpdateFieldOperation { Id = "items/1", FieldName = "Name", Value = "b" },
            new DocumentPatchOperation { Id = "items/1", Script = "this.Name = 'c';" },
            new QueryOperation { QueryText = "from Items where Name = $n", Parameters = new Dictionary<string, object?> { ["n"] = "c" } },
            new StreamQueryOperation { QueryText = "from Items", Parameters = new Dictionary<string, object?>() },
            new AttachmentOperation { DocumentId = "items/1", Name = "a.bin", Kind = AttachmentOperationKind.Put, Payload = new byte[] { 1, 2, 3 } },
            new AttachmentOperation { DocumentId = "items/1", Name = "a.bin", Kind = AttachmentOperationKind.Get },
            new AttachmentOperation { DocumentId = "items/1", Name = "a.bin", Kind = AttachmentOperationKind.Delete },
            new BulkInsertOperation<string> { Documents = [new DocumentToWrite<string> { Id = "items/2", Document = doc }] },
            new VectorSearchOperation { QueryVector = [0.1f, 0.2f, 0.3f], FieldName = "Vec", ExpectedIndex = "Items/ByVec" },
        };

        store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
        {
            Name = "Items/ByVec",
            Maps = { "from i in docs.Items select new { Vec = CreateVector(i.Vec) }" }
        }));

        foreach (var op in ops)
        {
            if (op is VectorSearchOperation)
                WaitForIndexing(store);

            var result = await transport.ExecuteAsync(op, CancellationToken.None);
            result.IsSuccess.Should().BeTrue($"{op.GetType().Name}: {result.ErrorDetails}");
            result.BytesIn.Should().BePositive();
        }

        var query = await transport.ExecuteAsync(ops[4], CancellationToken.None);
        query.ResultCount.Should().Be(1);
        transport.OpenedSocketConnections.Should().Be(1);
    }

    [Fact]
    public async Task Pipelined_Batch_Whose_Middle_Request_Fails_Keeps_Its_Neighbours()
    {
        using var store = GetDocumentStore();
        using var transport = new RawHttpTransport(store.Urls[0], store.Database, CompressionMode.Identity, HttpVersion.Version11, pipelineDepth: 3);
        QueryOperation Query(string rql) => new() { QueryText = rql, Parameters = new Dictionary<string, object?>() };

        var results = await Task.WhenAll(
            transport.ExecuteAsync(Query("from @all_docs"), CancellationToken.None),
            transport.ExecuteAsync(Query("from where where"), CancellationToken.None),
            transport.ExecuteAsync(Query("from @all_docs"), CancellationToken.None));

        results[0].IsSuccess.Should().BeTrue(results[0].ErrorDetails);
        results[1].IsSuccess.Should().BeFalse();
        results[1].ErrorDetails.Should().Contain("ParseException");
        results[2].IsSuccess.Should().BeTrue(results[2].ErrorDetails);
        transport.OpenedSocketConnections.Should().Be(1);
    }
}
