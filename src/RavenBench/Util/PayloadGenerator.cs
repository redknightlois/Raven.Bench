using System.Text;
using System.Text.Json;

namespace RavenBench.Util;

public static class PayloadGenerator
{
    private const string Alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    
    public static string Generate(int sizeBytes, Random rng)
    {
        // Generate YCSB-compatible JSON document with 10 fields (field0-field9)
        // Each field contains up to 100 characters to match YCSB standard format
        var document = new YcsbDocument();
        
        var fieldSize = Math.Min(100, Math.Max(10, sizeBytes / 12)); // Distribute size across fields
        var remainingBytes = sizeBytes;
        
        // Fill fields 0-9 with random data
        for (int i = 0; i < 10 && remainingBytes > 50; i++)
        {
            var currentFieldSize = Math.Min(fieldSize, remainingBytes / (10 - i));
            document.SetField(i, GenerateRandomString(currentFieldSize, rng));
            remainingBytes -= currentFieldSize + 20; // Account for JSON overhead
        }
        
        // Serialize to JSON string
        return JsonSerializer.Serialize(document, new JsonSerializerOptions 
        { 
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase 
        });
    }
    
    private static string GenerateRandomString(int length, Random rng) =>
        string.Create(length, rng, static (span, random) =>
        {
            for (int i = 0; i < span.Length; i++)
            {
                span[i] = Alphabet[random.Next(Alphabet.Length)];
            }
        });
    
    private sealed class YcsbDocument
    {
        public string Field0 { get; set; } = string.Empty;
        public string Field1 { get; set; } = string.Empty;
        public string Field2 { get; set; } = string.Empty;
        public string Field3 { get; set; } = string.Empty;
        public string Field4 { get; set; } = string.Empty;
        public string Field5 { get; set; } = string.Empty;
        public string Field6 { get; set; } = string.Empty;
        public string Field7 { get; set; } = string.Empty;
        public string Field8 { get; set; } = string.Empty;
        public string Field9 { get; set; } = string.Empty;
        
        public void SetField(int index, string value)
        {
            switch (index)
            {
                case 0: Field0 = value; break;
                case 1: Field1 = value; break;
                case 2: Field2 = value; break;
                case 3: Field3 = value; break;
                case 4: Field4 = value; break;
                case 5: Field5 = value; break;
                case 6: Field6 = value; break;
                case 7: Field7 = value; break;
                case 8: Field8 = value; break;
                case 9: Field9 = value; break;
            }
        }
    }
}

