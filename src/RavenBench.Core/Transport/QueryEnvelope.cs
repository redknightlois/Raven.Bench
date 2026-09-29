using System.Text.Json;

namespace RavenBench.Core.Transport;

/// <summary>The metadata fields of a /queries response; each is null when the response does not carry it.</summary>
internal readonly record struct QueryEnvelope(string? IndexName, int? ResultCount, bool? IsStale)
{
    /// <summary>
    /// Reads the top-level IndexName, Results length and IsStale in one forward pass without building
    /// a document. The whole body is still validated: malformed JSON throws <see cref="JsonException"/>,
    /// and a field of the wrong type throws <see cref="InvalidOperationException"/>, as a JsonDocument read does.
    /// </summary>
    public static QueryEnvelope Read(ReadOnlySpan<byte> json)
    {
        var reader = new Utf8JsonReader(json, isFinalBlock: true, state: default);
        if (reader.Read() == false || reader.TokenType != JsonTokenType.StartObject)
            throw new JsonException("The query response is not a JSON object.");

        string? indexName = null;
        int? resultCount = null;
        bool? isStale = null;

        while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
        {
            if (reader.ValueTextEquals("IndexName"u8))
            {
                reader.Read();
                indexName = reader.GetString();
            }
            else if (reader.ValueTextEquals("Results"u8))
            {
                reader.Read();
                resultCount = reader.TokenType == JsonTokenType.StartArray ? CountElements(ref reader) : null;
                reader.Skip();
            }
            else if (reader.ValueTextEquals("IsStale"u8))
            {
                reader.Read();
                isStale = reader.GetBoolean();
            }
            else
            {
                reader.Read();
                reader.Skip();
            }
        }

        // Past the root object only whitespace may follow; anything else throws here.
        if (reader.Read())
            throw new JsonException("The query response has content after its root object.");

        return new QueryEnvelope(indexName, resultCount, isStale);
    }

    // Leaves the reader on the array's EndArray token.
    private static int CountElements(ref Utf8JsonReader reader)
    {
        var count = 0;
        while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
        {
            count++;
            reader.Skip();
        }
        return count;
    }
}
