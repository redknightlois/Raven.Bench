using System.Text;
using System.Text.Json;
using System.Collections.Concurrent;
using RavenBench.Core.Workload;

namespace RavenBench.Core;

public static class PayloadGenerator
{
    private const string Alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    private static readonly ConcurrentDictionary<int, string[]> _payloadCache = new();
    private static readonly ConcurrentDictionary<int, YcsbRecord[]> _recordCache = new();
    private const int CacheSize = 1000; // Pre-generate 1000 payloads per size
    
    public static string Generate(int sizeBytes, Random rng)
    {
        // Use cached payloads to avoid constant allocation
        var cachedPayloads = _payloadCache.GetOrAdd(sizeBytes, size => GeneratePayloadCache(size));
        return cachedPayloads[rng.Next(cachedPayloads.Length)];
    }
    
    /// <summary>
    /// The same document as <see cref="Generate"/>, as an entity for a transport that goes through
    /// an object mapper. Both forms hold the same ten fields, so the two transports store the same
    /// record and only the mapper separates them.
    /// </summary>
    public static YcsbRecord GenerateRecord(int sizeBytes, Random rng)
    {
        var cachedRecords = _recordCache.GetOrAdd(sizeBytes, size => GenerateRecordCache(size));
        return cachedRecords[rng.Next(cachedRecords.Length)];
    }

    /// <summary>
    /// The insert operation for one document, in the form the transport driving the run consumes.
    /// </summary>
    public static OperationBase InsertOperationFor(PayloadKind kind, int sizeBytes, Random rng, string documentId) => kind switch
    {
        PayloadKind.Entity => new InsertOperation<YcsbRecord> { Id = documentId, Payload = GenerateRecord(sizeBytes, rng) },
        _ => new InsertOperation<string> { Id = documentId, Payload = Generate(sizeBytes, rng) }
    };

    /// <summary>
    /// The whole-document update for one document, in the form the transport driving the run consumes.
    /// </summary>
    public static OperationBase UpdateOperationFor(PayloadKind kind, int sizeBytes, Random rng, string documentId) => kind switch
    {
        PayloadKind.Entity => new UpdateOperation<YcsbRecord> { Id = documentId, Payload = GenerateRecord(sizeBytes, rng) },
        _ => new UpdateOperation<string> { Id = documentId, Payload = Generate(sizeBytes, rng) }
    };

    private static string[] GeneratePayloadCache(int sizeBytes)
    {
        var records = _recordCache.GetOrAdd(sizeBytes, size => GenerateRecordCache(size));
        var payloads = new string[CacheSize];

        for (int i = 0; i < CacheSize; i++)
        {
            payloads[i] = Serialize(records[i]);
        }

        return payloads;
    }

    private static YcsbRecord[] GenerateRecordCache(int sizeBytes)
    {
        var records = new YcsbRecord[CacheSize];
        var rng = new Random(42); // Fixed seed for reproducible payloads

        for (int i = 0; i < CacheSize; i++)
        {
            records[i] = GenerateUncached(sizeBytes, rng);
        }

        return records;
    }
    
    private static YcsbRecord GenerateUncached(int sizeBytes, Random rng)
    {
        // Generate YCSB-compatible JSON document with 10 fields (field0-field9)
        // Calculate accurate JSON overhead for: {"field0":"value","field1":"value",...,"field9":"value"}
        const int jsonOverhead = 2 +       // Opening and closing braces: {}
                                 9 +       // 9 commas between fields
                                 10 * 2 +  // 10 sets of quotes around values: ""
                                 10 * 1 +  // 10 colons and quotes around field names: ":"
                                 10 * 8;   // Field names: "field0" through "field9" total chars

        var document = new YcsbRecord();

        if (sizeBytes <= jsonOverhead)
        {
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

            for (int i = 0; i < fieldsToFill; i++)
            {
                var currentFieldSize = fieldSize;
                if (i == fieldsToFill - 1)
                {
                    var usedContent = i * fieldSize;
                    currentFieldSize = availableContentSize - usedContent;
                }

                document.SetField(i, GenerateRandomString(Math.Max(1, currentFieldSize), rng));
            }

            for (int i = fieldsToFill; i < 10; i++)
            {
                document.SetField(i, "");
            }
        }

        return document;
    }

    private static string Serialize(YcsbRecord record) => JsonSerializer.Serialize(record);

    
    private static string GenerateRandomString(int length, Random rng) =>
        string.Create(length, rng, static (span, random) =>
        {
            for (int i = 0; i < span.Length; i++)
            {
                span[i] = Alphabet[random.Next(Alphabet.Length)];
            }
        });
    
}
