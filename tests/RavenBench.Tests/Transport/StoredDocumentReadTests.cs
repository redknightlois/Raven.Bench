using System;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using RavenBench.Core;
using RavenBench.Core.Transport;
using Xunit;

namespace RavenBench.Tests.Transport;

public class StoredDocumentReadTests
{
    /// <summary>Answers every request on a fresh connection with the given status line and an empty body.</summary>
    private static async Task Serve(TcpListener listener, string status, CancellationToken ct)
    {
        var buffer = new byte[4096];
        while (ct.IsCancellationRequested == false)
        {
            using var client = await listener.AcceptTcpClientAsync(ct);
            var stream = client.GetStream();
            _ = await stream.ReadAsync(buffer, ct);
            await stream.WriteAsync(Encoding.ASCII.GetBytes($"HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"), ct);
        }
    }

    [Fact]
    public async Task A_Missing_Document_Reads_As_Null()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        using var stop = new CancellationTokenSource();
        var server = Serve(listener, "404 Not Found", stop.Token);
        using var transport = new RawHttpTransport($"http://127.0.0.1:{((IPEndPoint)listener.LocalEndpoint).Port}", "db", CompressionMode.Identity, HttpVersion.Version11);

        var stored = await transport.ReadStoredFieldsAsync("users/missing", CancellationToken.None);

        stored.Should().BeNull();
        stop.Cancel();
        listener.Stop();
    }

    [Fact]
    public async Task Any_Other_Failure_Status_Still_Throws()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        using var stop = new CancellationTokenSource();
        var server = Serve(listener, "500 Internal Server Error", stop.Token);
        using var transport = new RawHttpTransport($"http://127.0.0.1:{((IPEndPoint)listener.LocalEndpoint).Port}", "db", CompressionMode.Identity, HttpVersion.Version11);

        var act = () => transport.ReadStoredFieldsAsync("users/1", CancellationToken.None);

        await act.Should().ThrowAsync<HttpRequestException>();
        stop.Cancel();
        listener.Stop();
    }
}
