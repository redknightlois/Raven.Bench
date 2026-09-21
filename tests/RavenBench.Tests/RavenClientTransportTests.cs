using System;
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

    [Fact]
    public async Task Mapped_Client_Writes_The_Fields_The_Record_Carries()
    {
        // INVARIANT: the mapped mode differs from the unmapped one only in crossing the object
        // mapper, so the document on the server holds the record's own ten fields under the wire
        // names, not the names or shape the mapper chose.

        using var store = GetDocumentStore();
        using var mapped = new RavenClientTransport(store.Urls[0], store.Database, CompressionMode.Identity, HttpVersion.Version11, mapEntities: true);

        var record = PayloadGenerator.GenerateRecord(1024, new Random(42));
        var result = await mapped.ExecuteAsync(new InsertOperation<YcsbRecord> { Id = "bench/1", Payload = record }, CancellationToken.None);

        result.IsSuccess.Should().BeTrue(result.ErrorDetails);

        using var session = store.OpenAsyncSession(new SessionOptions { NoTracking = true });
        var stored = await session.LoadAsync<BlittableJsonReaderObject>("bench/1");
        for (int i = 0; i < YcsbRecord.FieldCount; i++)
        {
            stored.TryGet($"field{i}", out string written).Should().BeTrue();
            written.Should().Be(record.GetField(i));
        }

        var readBack = await mapped.ExecuteAsync(new ReadOperation { Id = "bench/1" }, CancellationToken.None);
        readBack.IsSuccess.Should().BeTrue(readBack.ErrorDetails);
        readBack.BytesIn.Should().Be(record.EstimateJsonSize());
    }
}
