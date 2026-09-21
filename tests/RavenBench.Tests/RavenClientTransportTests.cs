using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using Raven.Client.Documents.Session;
using Raven.Embedded;
using Raven.TestDriver;
using RavenBench.Core;
using RavenBench.Core.Transport;
using RavenBench.Core.Workload;
using Sparrow.Json;
using Xunit;

namespace RavenBench.Tests;

public class RavenClientTransportTests : RavenTestDriver
{
    static RavenClientTransportTests()
    {
        ConfigureServer(new TestServerOptions
        {
            Licensing = new ServerOptions.LicensingOptions
            {
                ThrowOnInvalidOrMissingLicense = false
            }
        });
    }

    [Fact]
    public async Task Bulk_Batch_Stores_Every_Document_With_Its_Payload_Unchanged()
    {
        // INVARIANT: one bulk operation is one batch request that writes the payload bytes as the
        // document body, so a document read back carries the fields the workload generated.

        using var store = GetDocumentStore();
        using var transport = new RavenClientTransport(store.Urls[0], store.Database, CompressionMode.Identity, HttpVersion.Version11);

        var documents = Enumerable.Range(1, 250)
            .Select(i => new DocumentToWrite<string> { Id = $"bench/{i}", Document = $"{{\"field0\":\"value {i}\"}}" })
            .ToList();

        var result = await transport.ExecuteAsync(new BulkInsertOperation<string> { Documents = documents }, CancellationToken.None);

        result.IsSuccess.Should().BeTrue(result.ErrorDetails);
        (await transport.GetDocumentCountAsync("bench/")).Should().Be(documents.Count);

        using var session = store.OpenAsyncSession(new SessionOptions { NoTracking = true });
        foreach (var id in new[] { "bench/1", "bench/137", "bench/250" })
        {
            var stored = await session.LoadAsync<BlittableJsonReaderObject>(id);
            stored.Should().NotBeNull();
            stored.TryGet("field0", out string field0).Should().BeTrue();
            field0.Should().Be($"value {id["bench/".Length..]}");
        }
    }
}
