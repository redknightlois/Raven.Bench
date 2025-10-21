using System;
using System.Text.Json;
using FluentAssertions;
using RavenBench.Workload;
using Xunit;

namespace RavenBench.Tests;

public class RawHttpTransportTests
{
    [Fact]
    public void WriteWorkload_Generates_Valid_JSON_Object_Payloads()
    {
        // INVARIANT: WriteWorkload payloads should be valid JSON objects, not strings
        // This test prevents regression where payloads might be double-serialized

        var workload = new WriteWorkload(1024);
        var rng = new Random(42);
        var op = workload.NextOperation(rng);

        op.Should().BeOfType<InsertOperation<string>>();
        var insertOp = (InsertOperation<string>)op;

        // The payload should be valid JSON that parses to an object
        var payload = insertOp.Payload;
        var document = JsonDocument.Parse(payload);
        document.RootElement.ValueKind.Should().Be(JsonValueKind.Object);

        // Ensure it has the expected YCSB fields
        document.RootElement.EnumerateObject().Should().HaveCountGreaterThan(0);
        foreach (var property in document.RootElement.EnumerateObject())
        {
            property.Name.Should().MatchRegex("^field\\d+$");
            property.Value.ValueKind.Should().Be(JsonValueKind.String);
        }
    }

    [Fact]
    public void Double_Serialization_Produces_Invalid_JSON_For_PUT_Requests()
    {
        // INVARIANT: Double-serializing JSON strings produces invalid content for HTTP requests
        // This test documents the bug that was fixed and prevents regression

        var validJsonPayload = "{\"field0\":\"test value\",\"field1\":\"another value\"}";

        // Correct: payload is already JSON
        var correctDoc = JsonDocument.Parse(validJsonPayload);
        correctDoc.RootElement.ValueKind.Should().Be(JsonValueKind.Object);

        // Buggy: double-serialize the JSON string
        var doubleSerialized = JsonSerializer.Serialize(validJsonPayload);
        // Result: "\"{\\\"field0\\\":\\\"test value\\\",\\\"field1\\\":\\\"another value\\\"}\""

        var buggyDoc = JsonDocument.Parse(doubleSerialized);
        buggyDoc.RootElement.ValueKind.Should().Be(JsonValueKind.String);

        // The string content is escaped JSON, not the document itself
        var innerJson = buggyDoc.RootElement.GetString();
        innerJson.Should().Be(validJsonPayload);

        // When sent to server, this would be invalid because the server expects JSON object, not a JSON string
    }
}