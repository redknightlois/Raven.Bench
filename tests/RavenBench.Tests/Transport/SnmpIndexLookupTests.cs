using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using RavenBench.Core.Transport;
using Xunit;

namespace RavenBench.Tests.Transport;

public class SnmpIndexLookupTests
{
    private sealed class OidHandler(HttpStatusCode status, string body) : HttpMessageHandler
    {
        public int OidRequests;

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            if (request.RequestUri!.AbsolutePath == "/monitoring/snmp/oids")
                Interlocked.Increment(ref OidRequests);
            return Task.FromResult(new HttpResponseMessage(status) { Content = new StringContent(body) });
        }
    }

    [Theory]
    [InlineData(HttpStatusCode.OK, """{"Databases":{"db":{"@General":[{"OID":"1.3.6.1.4.1.45751.1.1.5.2.7.1.1"}]}}}""")]
    [InlineData(HttpStatusCode.InternalServerError, "")]
    public async Task The_Database_Index_Is_Looked_Up_Once_Across_Polls(HttpStatusCode status, string body)
    {
        using var handler = new OidHandler(status, body);
        using var http = new HttpClient(handler);
        var admin = new TransportAdminClient(http, "http://127.0.0.1:1");

        var first = await admin.GetDatabaseSnmpIndexAsync("db");
        for (int poll = 0; poll < 4; poll++)
            (await admin.GetDatabaseSnmpIndexAsync("db")).Should().Be(first);

        handler.OidRequests.Should().Be(1);
        (first is null).Should().Be(status != HttpStatusCode.OK);
    }
}
