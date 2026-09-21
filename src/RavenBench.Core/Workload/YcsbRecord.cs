namespace RavenBench.Core.Workload;

/// <summary>
/// How a workload hands a document to a transport: as the JSON bytes to write, or as an entity for
/// a transport that serialises it through an object mapper.
/// </summary>
public enum PayloadKind
{
    Json,
    Entity
}

/// <summary>
/// The YCSB record as a mapped entity. The properties carry the wire field names, so a document
/// written through an object mapper holds the same keys as one written as raw JSON, with no
/// serializer configuration between the two.
/// </summary>
public sealed class YcsbRecord
{
    public const int FieldCount = 10;

    public string field0 { get; set; } = string.Empty;
    public string field1 { get; set; } = string.Empty;
    public string field2 { get; set; } = string.Empty;
    public string field3 { get; set; } = string.Empty;
    public string field4 { get; set; } = string.Empty;
    public string field5 { get; set; } = string.Empty;
    public string field6 { get; set; } = string.Empty;
    public string field7 { get; set; } = string.Empty;
    public string field8 { get; set; } = string.Empty;
    public string field9 { get; set; } = string.Empty;

    public void SetField(int index, string value)
    {
        switch (index)
        {
            case 0: field0 = value; break;
            case 1: field1 = value; break;
            case 2: field2 = value; break;
            case 3: field3 = value; break;
            case 4: field4 = value; break;
            case 5: field5 = value; break;
            case 6: field6 = value; break;
            case 7: field7 = value; break;
            case 8: field8 = value; break;
            case 9: field9 = value; break;
            default: throw new ArgumentOutOfRangeException(nameof(index), index, null);
        }
    }

    public string GetField(int index) => index switch
    {
        0 => field0,
        1 => field1,
        2 => field2,
        3 => field3,
        4 => field4,
        5 => field5,
        6 => field6,
        7 => field7,
        8 => field8,
        9 => field9,
        _ => throw new ArgumentOutOfRangeException(nameof(index), index, null)
    };

    /// <summary>
    /// The serialised size of this record, which the mapper never exposes as bytes: the braces, the
    /// commas, and for each field its name, the colon and the two pairs of quotes.
    /// </summary>
    public long EstimateJsonSize()
    {
        long size = 2 + (FieldCount - 1);
        for (int i = 0; i < FieldCount; i++)
            size += $"field{i}".Length + 5 + GetField(i).Length;

        return size;
    }
}
