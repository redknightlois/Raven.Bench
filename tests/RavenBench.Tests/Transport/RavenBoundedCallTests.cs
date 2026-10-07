using System;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using RavenBench.Core;
using RavenBench.Core.Transport;
using Xunit;

namespace RavenBench.Tests.Transport;

public class RavenBoundedCallTests
{
    // Longer than the transport's request deadline, shorter than a hang the suite would notice.
    private static readonly TimeSpan OuterGuard = TimeSpan.FromSeconds(50);

    /// <summary>Accepts every connection and never answers.</summary>
    private static async Task AcceptAndStay(TcpListener listener, CancellationToken stop)
    {
        var held = new System.Collections.Generic.List<TcpClient>();
        try
        {
            while (true)
                held.Add(await listener.AcceptTcpClientAsync(stop));
        }
        catch (OperationCanceledException)
        {
        }
        finally
        {
            held.ForEach(c => c.Dispose());
        }
    }

    [Fact]
    public async Task Every_Admin_Preflight_And_Inspection_Call_Fails_Against_A_Server_That_Never_Answers()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        using var stop = new CancellationTokenSource();
        var server = AcceptAndStay(listener, stop.Token);
        using var transport = new RawHttpTransport($"http://127.0.0.1:{((IPEndPoint)listener.LocalEndpoint).Port}", "db", CompressionMode.Identity, HttpVersion.Version11);

        // The five calls wait out their deadlines side by side, so the test takes one deadline.
        Func<Task>[] calls =
        [
            () => transport.ValidateClientAsync(),
            () => transport.GetServerVersionAsync(),
            () => transport.GetServerMaxCoresAsync(),
            () => transport.ReadStoredFieldsAsync("users/1", CancellationToken.None),
            () => transport.DeleteStoredDocumentAsync("users/1", CancellationToken.None),
        ];
        var outcomes = calls.Select(async call =>
        {
            var attempt = call();
            var winner = await Task.WhenAny(attempt, Task.Delay(OuterGuard));
            return winner == attempt ? attempt.Exception?.GetBaseException() : null;
        }).ToArray();
        var failures = await Task.WhenAll(outcomes);

        failures.Should().OnlyContain(failure => failure != null, "each call fails by its own deadline before the outer guard");
        stop.Cancel();
        await server;
        listener.Stop();
    }

    [Fact]
    public async Task The_Callers_Cancellation_Surfaces_As_Cancellation_Not_As_The_Deadline()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        using var stop = new CancellationTokenSource();
        var server = AcceptAndStay(listener, stop.Token);
        using var transport = new RawHttpTransport($"http://127.0.0.1:{((IPEndPoint)listener.LocalEndpoint).Port}", "db", CompressionMode.Identity, HttpVersion.Version11);
        using var caller = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));

        var act = () => transport.ReadStoredFieldsAsync("users/1", caller.Token);

        await act.Should().ThrowAsync<OperationCanceledException>();
        stop.Cancel();
        await server;
        listener.Stop();
    }
}
