using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using FluentAssertions;
using RavenBench.Core.Metrics.Snmp;
using Xunit;

namespace RavenBench.Tests;

public class SnmpClientTests
{
    [Fact]
    public async Task GetManyAsync_UnreachableEndpoint_ReturnsEmpty()
    {
        var client = new SnmpClient();

        var result = await client.GetManyAsync(new[] { "1.3.6.1.2.1.1.1.0" }, "127.0.0.1", port: 1, timeoutMs: 250);

        result.Should().BeEmpty();
    }

    [Fact]
    public async Task GetManyAsync_HostName_ResolvesAndReturnsEmptyOnFailure()
    {
        var client = new SnmpClient();

        var result = await client.GetManyAsync(new[] { "1.3.6.1.2.1.1.1.0" }, "localhost", port: 1, timeoutMs: 250);

        result.Should().BeEmpty();
    }

    [Fact]
    public async Task GetManyAsync_TimedOutRequest_DisposesItsSocket()
    {
        using var silentAgent = new UdpClient(new IPEndPoint(IPAddress.Loopback, 0));
        var socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);

        var result = await new SnmpClient().GetManyAsync(new[] { "1.3.6.1.4.1.45751.1.1.1.5.1" },
            (IPEndPoint)silentAgent.Client.LocalEndPoint!, socket, timeoutMs: 50);

        result.Should().BeEmpty();
        socket.SafeHandle.IsClosed.Should().BeTrue();
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void A_Failure_After_A_Success_Is_Reported_Again()
    {
        var log = new StringWriter();
        var client = new SnmpClient(log);

        client.RecordOutcome(new TimeoutException("first"));
        client.RecordOutcome(new TimeoutException("repeat"));
        client.RecordOutcome(null);
        client.RecordOutcome(new TimeoutException("second"));

        var reports = log.ToString().Split('\n', StringSplitOptions.RemoveEmptyEntries);
        reports.Should().HaveCount(2);
        reports[0].Should().Contain("first");
        reports[1].Should().Contain("second");
    }
}
