using System;
using System.Linq;
using System.Text;
using System.Text.Json;
using FluentAssertions;
using RavenBench.Core;
using RavenBench.Core.Workload;
using Xunit;

namespace RavenBench.Tests;

public class PayloadGeneratorTests
{
    private const int DocumentSize = 1024;

    [Fact]
    public void Generate_Produces_Ten_String_Fields_Named_And_Ordered_By_Ordinal()
    {
        // INVARIANT: the record is the YCSB record; the same ten wire keys go to every product.
        var payload = PayloadGenerator.Generate(seed: 42, documentId: BenchIds.IdFor(1), sizeBytes: DocumentSize);

        using var document = JsonDocument.Parse(payload);
        document.RootElement.ValueKind.Should().Be(JsonValueKind.Object);

        var properties = document.RootElement.EnumerateObject().ToArray();
        properties.Should().HaveCount(PayloadGenerator.FieldCount);
        for (int i = 0; i < PayloadGenerator.FieldCount; i++)
        {
            properties[i].Name.Should().Be(PayloadGenerator.FieldName(i));
            properties[i].Value.ValueKind.Should().Be(JsonValueKind.String);
        }
    }

    [Fact]
    public void Generate_Gives_Every_Field_The_Same_Width()
    {
        // INVARIANT: ten fields of one field length, or the row is not comparable to YCSB's.
        var payload = PayloadGenerator.Generate(seed: 42, documentId: BenchIds.IdFor(1), sizeBytes: DocumentSize);

        using var document = JsonDocument.Parse(payload);
        document.RootElement.EnumerateObject()
            .Select(p => p.Value.GetString()!.Length)
            .Should().AllBeEquivalentTo(PayloadGenerator.FieldWidth(DocumentSize));
    }

    [Theory]
    [InlineData(200)]
    [InlineData(512)]
    [InlineData(1024)]
    [InlineData(2048)]
    [InlineData(16384)]
    public void Generate_Lands_Within_One_Field_Count_Of_The_Requested_Size(int sizeBytes)
    {
        // INVARIANT: equal width beats an exact size, so the shortfall from rounding the
        // available bytes across ten fields is all the document may lose.
        var payload = PayloadGenerator.Generate(seed: 42, documentId: BenchIds.IdFor(1), sizeBytes: sizeBytes);

        var actualBytes = Encoding.UTF8.GetByteCount(payload);
        actualBytes.Should().BeLessOrEqualTo(sizeBytes);
        actualBytes.Should().BeGreaterThan(sizeBytes - PayloadGenerator.FieldCount);
    }

    [Fact]
    public void Generate_Grows_Monotonically_With_The_Requested_Size()
    {
        var sizes = new[] { 200, 512, 1024, 2048, 16384 };

        var lengths = sizes.Select(size =>
            Encoding.UTF8.GetByteCount(PayloadGenerator.Generate(42, BenchIds.IdFor(1), size))).ToArray();

        lengths.Should().BeInAscendingOrder().And.OnlyHaveUniqueItems();
    }

    [Fact]
    public void Generate_Below_The_Json_Overhead_Floors_At_One_Character_Per_Field()
    {
        // INVARIANT: a size too small for ten real fields is a documented floor, not a silent
        // truncation to a record the loader cannot round-trip.
        var payload = PayloadGenerator.Generate(seed: 42, documentId: BenchIds.IdFor(1), sizeBytes: 1);

        using var document = JsonDocument.Parse(payload);
        document.RootElement.EnumerateObject().Should().HaveCount(PayloadGenerator.FieldCount)
            .And.OnlyContain(p => p.Value.GetString()!.Length == 1);
    }

    [Fact]
    public void Generate_Is_A_Pure_Function_Of_Seed_Id_And_Size()
    {
        var first = PayloadGenerator.Generate(seed: 123, documentId: BenchIds.IdFor(42), sizeBytes: DocumentSize);
        var second = PayloadGenerator.Generate(seed: 123, documentId: BenchIds.IdFor(42), sizeBytes: DocumentSize);

        second.Should().Be(first);
    }

    [Fact]
    public void Generate_Does_Not_Depend_On_What_Was_Generated_Before_It()
    {
        // INVARIANT: no cache and no shared draw position; the id alone decides the content.
        var isolated = PayloadGenerator.Generate(123, BenchIds.IdFor(42), DocumentSize);

        for (long i = 1; i <= 50; i++)
            PayloadGenerator.Generate(123, BenchIds.IdFor(i), DocumentSize);

        PayloadGenerator.Generate(123, BenchIds.IdFor(42), DocumentSize).Should().Be(isolated);
    }

    [Fact]
    public void Generate_Gives_Each_Id_Its_Own_Content_Under_One_Seed()
    {
        // INVARIANT: content depends on the document id. A derivation that ignored the id would
        // load every product with the same document at every key.
        var documents = Enumerable.Range(1, 200)
            .Select(i => PayloadGenerator.Generate(7, BenchIds.IdFor(i), DocumentSize))
            .ToArray();

        documents.Should().OnlyHaveUniqueItems();
    }

    [Fact]
    public void Generate_Gives_Each_Seed_Its_Own_Content_At_The_Same_Id()
    {
        for (long i = 1; i <= 50; i++)
        {
            var id = BenchIds.IdFor(i);
            PayloadGenerator.Generate(1, id, DocumentSize)
                .Should().NotBe(PayloadGenerator.Generate(2, id, DocumentSize));
        }
    }

    [Fact]
    public void Generate_Does_Not_Alias_The_Seed_With_The_Document_Id()
    {
        // INVARIANT: the seed and the id are mixed, not added. An additive derivation makes
        // (seed + 1, id) draw exactly what (seed, id + 1) draws.
        for (int seed = 1; seed <= 20; seed++)
        {
            for (long i = 1; i <= 20; i++)
            {
                PayloadGenerator.Generate(seed + 1, BenchIds.IdFor(i), DocumentSize)
                    .Should().NotBe(PayloadGenerator.Generate(seed, BenchIds.IdFor(i + 1), DocumentSize));
            }
        }
    }
}
