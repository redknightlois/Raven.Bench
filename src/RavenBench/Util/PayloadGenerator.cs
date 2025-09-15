using System.Text;
using System.Text.Json;
using System.Collections.Concurrent;

namespace RavenBench.Util;

public static class PayloadGenerator
{
    private const string Alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    private static readonly ConcurrentDictionary<int, string[]> _payloadCache = new();
    private const int CacheSize = 1000; // Pre-generate 1000 payloads per size
    
    public static string Generate(int sizeBytes, Random rng)
    {
        // Use cached payloads to avoid constant allocation
        var cachedPayloads = _payloadCache.GetOrAdd(sizeBytes, size => GeneratePayloadCache(size));
        return cachedPayloads[rng.Next(cachedPayloads.Length)];
    }
    
    private static string[] GeneratePayloadCache(int sizeBytes)
    {
        var payloads = new string[CacheSize];
        var rng = new Random(42); // Fixed seed for reproducible payloads
        
        for (int i = 0; i < CacheSize; i++)
        {
            payloads[i] = GenerateUncached(sizeBytes, rng);
        }
        
        return payloads;
    }
    
    private static string GenerateUncached(int sizeBytes, Random rng)
    {
        // Generate YCSB-compatible JSON document with 10 fields (field0-field9)
        // Calculate accurate JSON overhead for: {"field0":"value","field1":"value",...,"field9":"value"}
        const int jsonOverhead = 2 +       // Opening and closing braces: {}
                                 9 +       // 9 commas between fields
                                 10 * 2 +  // 10 sets of quotes around values: ""
                                 10 * 1 +  // 10 colons and quotes around field names: ":"
                                 10 * 8;   // Field names: "field0" through "field9" total chars

        var document = new YcsbDocument();

        // Ensure we have enough space for the JSON structure
        if (sizeBytes <= jsonOverhead)
        {
            // For very small sizes, create minimal document with empty or very short fields
            for (int i = 0; i < 10; i++)
            {
                document.SetField(i, "x"); // 1 character per field
            }
        }
        else
        {
            var availableContentSize = sizeBytes - jsonOverhead;
            var fieldsToFill = Math.Min(10, Math.Max(1, availableContentSize / 10));
            var fieldSize = availableContentSize / fieldsToFill;

            // Fill the calculated number of fields
            for (int i = 0; i < fieldsToFill; i++)
            {
                var currentFieldSize = fieldSize;
                // For the last field, use any remaining space
                if (i == fieldsToFill - 1)
                {
                    var usedContent = i * fieldSize;
                    currentFieldSize = availableContentSize - usedContent;
                }

                document.SetField(i, GenerateRandomString(Math.Max(1, currentFieldSize), rng));
            }

            // Fill remaining fields with minimal content if needed
            for (int i = fieldsToFill; i < 10; i++)
            {
                document.SetField(i, "");
            }
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

