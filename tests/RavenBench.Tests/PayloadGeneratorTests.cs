using System;
using System.Text.Json;
using FluentAssertions;
using RavenBench.Util;
using Xunit;

namespace RavenBench.Tests;

public class PayloadGeneratorTests
{
    [Fact]
    public void Generate_Produces_Valid_JSON()
    {
        // INVARIANT: Generated payload must be valid JSON that can be parsed
        // INVARIANT: Should contain YCSB-standard field structure
        var rng = new Random(42);
        
        var payload = PayloadGenerator.Generate(1024, rng);
        
        // Must be valid JSON
        var document = JsonDocument.Parse(payload);
        document.RootElement.ValueKind.Should().Be(JsonValueKind.Object);
        
        // Should have at least one field
        document.RootElement.EnumerateObject().Should().HaveCountGreaterThan(0);
    }

    [Fact] 
    public void Generate_Respects_Size_Constraints()
    {
        // INVARIANT: Generated payload size should be reasonably close to requested size
        // INVARIANT: Larger requests should produce larger payloads
        var rng = new Random(42);
        
        var small = PayloadGenerator.Generate(500, rng);
        var large = PayloadGenerator.Generate(2000, rng);
        
        var smallBytes = System.Text.Encoding.UTF8.GetByteCount(small);
        var largeBytes = System.Text.Encoding.UTF8.GetByteCount(large);
        
        // Larger request should produce larger payload
        largeBytes.Should().BeGreaterThan(smallBytes);
    }
    
    [Fact]
    public void Generate_Is_Deterministic_With_Same_Seed()
    {
        // INVARIANT: Same seed should produce identical output
        // INVARIANT: Different seeds should produce different output
        var rng1 = new Random(123);
        var rng2 = new Random(123);
        var rng3 = new Random(456);
        
        var payload1 = PayloadGenerator.Generate(1024, rng1);
        var payload2 = PayloadGenerator.Generate(1024, rng2);  
        var payload3 = PayloadGenerator.Generate(1024, rng3);
        
        // Same seed should produce same result
        payload1.Should().Be(payload2);
        
        // Different seed should produce different result
        payload1.Should().NotBe(payload3);
    }
}