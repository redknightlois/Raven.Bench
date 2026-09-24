using System.Buffers.Binary;
using Apex.PgClient;

namespace RavenBench.Core.Transport;

/// <summary>
/// The binary wire format of the pgvector <c>vector</c> type: a big-endian int16 dimension count, an
/// int16 reserved field that is always zero, then one big-endian float32 per dimension.
/// </summary>
public static class PgVectorCodec
{
    /// <summary>The type name the server resolves to the extension's oid.</summary>
    public const string TypeName = "vector";

    /// <summary>The largest dimension count pgvector stores in a <c>vector</c>.</summary>
    public const int MaxDimensions = 16000;

    private const int HeaderSize = 4;

    public static byte[] Encode(float[] vector)
    {
        ArgumentNullException.ThrowIfNull(vector);
        if (vector.Length is 0 or > MaxDimensions)
            throw new ArgumentOutOfRangeException(nameof(vector), vector.Length, $"A pgvector vector holds 1 to {MaxDimensions} dimensions.");

        var bytes = new byte[HeaderSize + sizeof(float) * vector.Length];
        BinaryPrimitives.WriteInt16BigEndian(bytes, (short)vector.Length);
        for (int i = 0; i < vector.Length; i++)
            BinaryPrimitives.WriteSingleBigEndian(bytes.AsSpan(HeaderSize + sizeof(float) * i), vector[i]);
        return bytes;
    }

    public static float[] Decode(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length < HeaderSize)
            throw new PgVectorFormatException($"A binary vector needs a {HeaderSize}-byte header; the buffer holds {bytes.Length} bytes.");

        int dimensions = BinaryPrimitives.ReadInt16BigEndian(bytes);
        int reserved = BinaryPrimitives.ReadInt16BigEndian(bytes[2..]);
        if (dimensions is < 1 or > MaxDimensions)
            throw new PgVectorFormatException($"A binary vector declares {dimensions} dimensions; pgvector allows 1 to {MaxDimensions}.");
        if (reserved != 0)
            throw new PgVectorFormatException($"A binary vector carries {reserved} in its reserved field; pgvector writes 0.");
        if (bytes.Length != HeaderSize + sizeof(float) * dimensions)
            throw new PgVectorFormatException($"A binary vector declares {dimensions} dimensions, which need {HeaderSize + sizeof(float) * dimensions} bytes; the buffer holds {bytes.Length}.");

        var vector = new float[dimensions];
        for (int i = 0; i < dimensions; i++)
            vector[i] = BinaryPrimitives.ReadSingleBigEndian(bytes[(HeaderSize + sizeof(float) * i)..]);
        return vector;
    }

    /// <summary>
    /// Registers the codec for the server's <c>vector</c> oid. The oid is assigned when the extension
    /// is created, so it is read from the server, never assumed.
    /// </summary>
    public static PgTypeRegistry Register(uint oid)
    {
        var registry = new PgTypeRegistry();
        registry.Register<PgVector>(new PgType(oid, TypeName), v => Encode(v.Values), m => new PgVector(Decode(m.Span)));
        return registry;
    }
}

/// <summary>
/// A pgvector value. The wrapper keeps the registered codec apart from the driver's own <c>real[]</c> mapping of float arrays.
/// </summary>
public sealed record PgVector(float[] Values);

/// <summary>Thrown when bytes do not hold a well-formed binary pgvector value.</summary>
public sealed class PgVectorFormatException(string message) : FormatException(message);
