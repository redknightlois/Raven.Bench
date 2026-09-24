using System.Text.Json;

namespace RavenBench.Core.Transport;

/// <summary>The metadata fields of a /queries response; each is null when the response does not carry it or was not asked for.</summary>
internal readonly record struct QueryEnvelope(string? IndexName, int? ResultCount, bool? IsStale, IReadOnlyList<string>? Ids = null)
{
    /// <summary>
    /// Reads the top-level IndexName, Results length and IsStale in one forward pass without building
    /// a document. The whole body is still validated: malformed JSON throws <see cref="JsonException"/>,
    /// and a field of the wrong type throws <see cref="InvalidOperationException"/>, as a JsonDocument read does.
    /// With <paramref name="readIds"/>, it also keeps each result's @metadata.@id in order, and a result without one throws <see cref="JsonException"/>.
    /// </summary>
    public static QueryEnvelope Read(ReadOnlySpan<byte> json, bool readIds = false)
    {
        var reader = new Utf8JsonReader(json, isFinalBlock: true, state: default);
        if (reader.Read() == false || reader.TokenType != JsonTokenType.StartObject)
            throw new JsonException("The query response is not a JSON object.");

        string? indexName = null;
        int? resultCount = null;
        bool? isStale = null;
        List<string>? ids = null;

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
                if (reader.TokenType != JsonTokenType.StartArray)
                    reader.Skip();
                else if (readIds)
                    resultCount = (ids = ReadIds(ref reader)).Count;
                else
                    resultCount = CountElements(ref reader);
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

        if (readIds && ids == null)
            throw new JsonException("The vector search response carries no Results array.");

        return new QueryEnvelope(indexName, resultCount, isStale, ids);
    }

    // Leaves the reader on the array's EndArray token.
    private static List<string> ReadIds(ref Utf8JsonReader reader)
    {
        var ids = new List<string>();
        while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
            ids.Add(ReadMetadataId(ref reader) ?? throw new JsonException("A vector search result carries no @metadata.@id."));
        return ids;
    }

    // Reads one result object and leaves the reader on its EndObject token.
    private static string? ReadMetadataId(ref Utf8JsonReader reader)
    {
        string? id = null;
        if (reader.TokenType != JsonTokenType.StartObject)
        {
            reader.Skip();
            return null;
        }
        while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
        {
            var metadata = reader.ValueTextEquals("@metadata"u8);
            reader.Read();
            if (metadata && reader.TokenType == JsonTokenType.StartObject)
            {
                while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
                {
                    var isId = reader.ValueTextEquals("@id"u8);
                    reader.Read();
                    if (isId && reader.TokenType == JsonTokenType.String)
                        id = reader.GetString();
                    else
                        reader.Skip();
                }
            }
            else
            {
                reader.Skip();
            }
        }
        return id;
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
