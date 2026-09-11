using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using FluentAssertions;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using RavenBench.Core;
using RavenBench.Core.Transport;
using RavenBench.Core.Workload;
using RavenBench.Tests.Infrastructure;
using Xunit;

namespace RavenBench.Tests.Ycsb;

/// <summary>
/// Pins the typed-operation-to-BSON translation without a live server: the read filter names
/// <c>_id</c>, the insert carries the id and the ten generated fields at the top level, the field
/// update is a one-field <c>$set</c> (never a whole-document replace), and every write path runs
/// under the journaled write concern.
/// </summary>
public class MongoYcsbTransportMappingTests
{
    private static readonly RenderArgs<BsonDocument> RenderArguments =
        new(BsonSerializer.LookupSerializer<BsonDocument>(), BsonSerializer.SerializerRegistry);

    private static BsonDocument Render(FilterDefinition<BsonDocument> filter) =>
        filter.Render(RenderArguments).AsBsonDocument;

    private static BsonDocument Render(UpdateDefinition<BsonDocument> update) =>
        update.Render(RenderArguments).AsBsonDocument;

    [Fact]
    public void Read_Filter_Names_The_Id_Field_And_The_Requested_Id()
    {
        var id = BenchIds.IdFor(7);

        var rendered = Render(MongoYcsbTransport.ReadFilter(id));

        rendered.ElementCount.Should().Be(1);
        rendered["_id"].AsString.Should().Be(id);
    }

    [Fact]
    public void Insert_Carries_The_Id_And_The_Ten_Generated_Fields_At_The_Top_Level()
    {
        var id = BenchIds.IdFor(7);
        var payload = PayloadGenerator.Generate(seed: 42, id, sizeBytes: 1024);

        var document = MongoYcsbTransport.ToStoredDocument(id, payload);

        document["_id"].AsString.Should().Be(id);
        document.ElementCount.Should().Be(PayloadGenerator.FieldCount + 1);

        using var expected = JsonDocument.Parse(payload);
        foreach (var field in expected.RootElement.EnumerateObject())
        {
            document.Contains(field.Name).Should().BeTrue("every generated field stays at the top level");
            document[field.Name].AsString.Should().Be(field.Value.GetString());
        }
    }

    [Fact]
    public void Field_Update_Is_A_One_Field_Set()
    {
        var rendered = Render(MongoYcsbTransport.FieldUpdate("field3", "replacement"));

        // A whole-document replace would render a document, not an update operator.
        rendered.ElementCount.Should().Be(1);
        var set = rendered["$set"].AsBsonDocument;
        set.ElementCount.Should().Be(1);
        set["field3"].AsString.Should().Be("replacement");
    }

    [Fact]
    public void Every_Write_Path_Uses_The_Journaled_Write_Concern()
    {
        MongoYcsbTransport.DurableWriteConcern.Journal.Should().BeTrue();

        using var transport = new MongoYcsbTransport(MongoTestEndpoints.MongoConnectionString, "unused", MongoYcsbTransport.MongoDbTarget);
        transport.AppliedWriteConcern.Journal.Should().BeTrue();
    }

    [Fact]
    public void Product_Name_Comes_From_The_Target()
    {
        using var mongo = new MongoYcsbTransport(MongoTestEndpoints.MongoConnectionString, "unused", MongoYcsbTransport.MongoDbTarget);
        using var documentDb = new MongoYcsbTransport(MongoTestEndpoints.MongoConnectionString, "unused", MongoYcsbTransport.DocumentDbTarget);

        mongo.ProductName.Should().Be(MongoYcsbTransport.MongoDbProductName);
        documentDb.ProductName.Should().Be(MongoYcsbTransport.DocumentDbProductName);
        mongo.ProductName.Should().NotBe(documentDb.ProductName);
    }

    [Fact]
    public void Wire_Bytes_Are_Not_Reported()
    {
        using var transport = new MongoYcsbTransport(MongoTestEndpoints.MongoConnectionString, "unused", MongoYcsbTransport.MongoDbTarget);

        transport.ReportsWireBytes.Should().BeFalse();
    }

    [Fact]
    public void An_Unsupported_Operation_Throws_A_Specific_Exception()
    {
        using var transport = new MongoYcsbTransport(MongoTestEndpoints.MongoConnectionString, "unused", MongoYcsbTransport.MongoDbTarget);
        var query = new QueryOperation { QueryText = "from @all_docs", Parameters = new Dictionary<string, object?>() };

        Action act = () => transport.ExecuteAsync(query, CancellationToken.None);

        act.Should().Throw<NotSupportedException>().WithMessage("*QueryOperation*");
    }

    [Fact]
    public void An_Unknown_Target_Is_Rejected_Naming_The_Value()
    {
        Action act = () => new MongoYcsbTransport(MongoTestEndpoints.MongoConnectionString, "unused", "postgresql");

        act.Should().Throw<ArgumentException>().WithMessage("*postgresql*");
    }

    [Fact]
    public void A_Missing_Connection_String_Is_Rejected()
    {
        Action act = () => new MongoYcsbTransport(" ", "unused", MongoYcsbTransport.MongoDbTarget);

        act.Should().Throw<ArgumentException>().WithMessage("*connection string*");
    }

    [Fact]
    public void A_Missing_Database_Name_Is_Rejected()
    {
        Action act = () => new MongoYcsbTransport(MongoTestEndpoints.MongoConnectionString, " ", MongoYcsbTransport.MongoDbTarget);

        act.Should().Throw<ArgumentException>().WithMessage("*database name*");
    }

    [Fact]
    public void Redaction_Removes_The_Password_And_Keeps_The_User_Host_Port_And_Options()
    {
        var redacted = MongoYcsbTransport.RedactConnectionString(MongoTestEndpoints.DocumentDbConnectionString);

        redacted.Should().Be("mongodb://bench:***@localhost:10260/?tls=true&tlsAllowInvalidCertificates=true");
        redacted.Should().NotContain("bench:bench");
    }

    [Theory]
    [InlineData("mongodb://localhost:27017")]
    [InlineData("mongodb://user@localhost:27017")]
    [InlineData("mongodb://localhost:27017/?appName=bench@home")]
    public void Redaction_Leaves_A_String_Without_A_Password_Unchanged(string connectionString)
    {
        MongoYcsbTransport.RedactConnectionString(connectionString).Should().Be(connectionString);
    }

    [Theory]
    [InlineData("mongodb://localhost:10260/?tls=true&tlsAllowInvalidCertificates=true", true)]
    [InlineData("mongodb://bench:bench@localhost:10260/?tlsAllowInvalidCertificates=true", true)]
    [InlineData("mongodb://localhost:10260/?tls=true", false)]
    [InlineData("mongodb://localhost:27017", false)]
    [InlineData("mongodb://localhost:27017/?tlsInsecure=false&tlsAllowInvalidCertificates=true", false)]
    public void Normalize_Translates_The_Shell_Insecure_Certificate_Option_To_The_Driver_Option(string connectionString, bool expectInsecure)
    {
        var normalized = MongoYcsbTransport.NormalizeConnectionString(connectionString);

        if (expectInsecure)
            normalized.Should().Contain("tlsInsecure=true");
        else
            normalized.Should().Be(connectionString);
    }
}
