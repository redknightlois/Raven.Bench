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
}
