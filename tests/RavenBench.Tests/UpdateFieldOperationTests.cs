using System;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using Raven.Embedded;
using Raven.TestDriver;
using RavenBench.Core;
using RavenBench.Core.Transport;
using RavenBench.Core.Workload;
using Xunit;

namespace RavenBench.Tests;

public class UpdateFieldOperationTests
{
    private const int DocumentSize = 1024;

    [Fact]
    public void Update_Value_Has_The_Width_Of_The_Field_It_Replaces()
    {
        // INVARIANT: a run of updates must not drift the document size, or the read row stops
        // measuring the document the load phase wrote.
        var workload = new MixedProfileWorkload(
            WorkloadMix.FromWeights(0, 0, 100), new UniformDistribution(), DocumentSize, seed: 7, initialKeyspace: 10);
        var rng = new Random(1);

        for (int i = 0; i < 20; i++)
        {
            var op = workload.NextOperation(rng).Should().BeOfType<UpdateFieldOperation>().Subject;

            op.Value.Length.Should().Be(PayloadGenerator.FieldWidth(DocumentSize));
            op.FieldName.Should().BeOneOf(Enumerable.Range(0, PayloadGenerator.FieldCount).Select(PayloadGenerator.FieldName));
        }
    }
}

/// <summary>
/// Proves the field update against a real server: a transport that served it by rewriting the
/// whole document would fail the byte-identical check on the nine fields it must not touch.
/// </summary>
public class UpdateFieldOperationTransportTests : RavenTestDriver
{
    private const int DocumentSize = 1024;

    static UpdateFieldOperationTransportTests()
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
    public async Task Update_Changes_The_Named_Field_And_Leaves_The_Other_Nine_Byte_Identical()
    {
        using var store = GetDocumentStore();
        using var transport = new RawHttpTransport(store.Urls[0], store.Database, CompressionMode.Identity, HttpVersion.Version11);

        const int seed = 42;
        var id = BenchIds.IdFor(1);
        var seeded = PayloadGenerator.Generate(seed, id, DocumentSize);
        await transport.PutAsync(id, seeded);

        const int updatedField = 3;
        var newValue = PayloadGenerator.GenerateFieldValue(PayloadGenerator.FieldWidth(DocumentSize), new Random(7));
        var result = await transport.ExecuteAsync(
            new UpdateFieldOperation { Id = id, FieldName = PayloadGenerator.FieldName(updatedField), Value = newValue },
            CancellationToken.None);
        result.IsSuccess.Should().BeTrue(result.ErrorDetails);

        // Read back over plain HTTP, so the comparison depends on neither the transport under test
        // nor the client library.
        using var http = new HttpClient();
        var body = await http.GetStringAsync($"{store.Urls[0]}/databases/{store.Database}/docs?id={Uri.EscapeDataString(id)}");
        using var response = JsonDocument.Parse(body);
        var readBack = response.RootElement.GetProperty("Results")[0];
        using var expected = JsonDocument.Parse(seeded);

        for (int i = 0; i < PayloadGenerator.FieldCount; i++)
        {
            var field = PayloadGenerator.FieldName(i);
            readBack.GetProperty(field).GetString().Should().Be(
                i == updatedField ? newValue : expected.RootElement.GetProperty(field).GetString(),
                $"only {PayloadGenerator.FieldName(updatedField)} may differ from the seeded document");
        }
    }
}
