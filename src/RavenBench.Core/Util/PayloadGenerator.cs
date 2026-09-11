using System.Text;

namespace RavenBench.Core;

/// <summary>
/// Generates the YCSB record: ten equal-width string fields named after their ordinal position.
/// The content of a document is a pure function of (seed, document id, requested size), so the
/// same triple yields byte-identical output in any process, at any concurrency, and whatever was
/// generated before it. Every loader and every workload must obtain a document from here, and no
/// caller may pass a <see cref="Random"/> whose position in a stream could change the result.
/// </summary>
public static class PayloadGenerator
{
    private const string Alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    /// <summary>
    /// Number of string fields in the record. Ten fields of one width is what the reference YCSB
    /// tool stores, and a comparable row depends on storing the same record.
    /// </summary>
    public const int FieldCount = 10;

    // {"field0":"..",..,"field9":".."}: two braces, the commas between fields, and for each field
    // a colon, two quotes around the key and two around the value, plus the key itself.
    private static readonly int JsonOverheadBytes =
        2 + (FieldCount - 1) + Enumerable.Range(0, FieldCount).Sum(i => FieldName(i).Length + 5);

    /// <summary>
    /// The wire key of one field. The same names are used on every product.
    /// </summary>
    public static string FieldName(int index) => $"field{index}";

    /// <summary>
    /// The width shared by all ten fields at this document size. Equal width wins over hitting the
    /// requested size exactly, so the serialised document lands within <see cref="FieldCount"/>
    /// bytes below the requested size. Below the JSON overhead no positive share is left and the
    /// width floors at one character per field, which keeps the record ten equal-width fields at
    /// every size instead of truncating the field count.
    /// </summary>
    public static int FieldWidth(int sizeBytes) => Math.Max(1, (sizeBytes - JsonOverheadBytes) / FieldCount);

    /// <summary>
    /// Builds the document that the run seeded with <paramref name="seed"/> stores at
    /// <paramref name="documentId"/>.
    /// </summary>
    public static string Generate(int seed, string documentId, int sizeBytes)
    {
        int width = FieldWidth(sizeBytes);
        var rng = DocumentRandom(seed, documentId);

        var json = new StringBuilder(JsonOverheadBytes + FieldCount * width);
        json.Append('{');
        for (int i = 0; i < FieldCount; i++)
        {
            if (i > 0)
                json.Append(',');
            json.Append('"').Append(FieldName(i)).Append("\":\"").Append(GenerateFieldValue(width, rng)).Append('"');
        }

        return json.Append('}').ToString();
    }

    /// <summary>
    /// Draws one field's worth of characters from the caller's own source. The alphabet needs no
    /// JSON escaping, so a field's character count is also its byte count on the wire.
    /// </summary>
    public static string GenerateFieldValue(int width, Random rng) =>
        string.Create(width, rng, static (span, random) =>
        {
            for (int i = 0; i < span.Length; i++)
                span[i] = Alphabet[random.Next(Alphabet.Length)];
        });

    /// <summary>
    /// Mixes the run seed and the document id into one document source with FNV-1a. Addition would
    /// alias, making (seed + 1, id) draw what (seed, id + 1) draws, and <c>string.GetHashCode</c>
    /// is randomised per process, which would break reproducibility across runs.
    /// </summary>
    private static Random DocumentRandom(int seed, string documentId)
    {
        const ulong offsetBasis = 14695981039346656037UL;
        const ulong prime = 1099511628211UL;

        ulong hash = offsetBasis;
        for (int shift = 0; shift < 32; shift += 8)
            hash = (hash ^ (byte)(seed >> shift)) * prime;
        foreach (var c in documentId)
            hash = (hash ^ c) * prime;

        return new Random(unchecked((int)(hash ^ (hash >> 32))));
    }
}
