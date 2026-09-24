using System;
using RavenBench.Core.Transport;
using Xunit;

namespace RavenBench.Tests.Transport;

public class PgVectorCodecTests
{
    // dims=3, reserved=0, then 1.0f, -2.5f, 0.0f big-endian.
    private static readonly byte[] Wire =
    [
        0x00, 0x03, 0x00, 0x00,
        0x3F, 0x80, 0x00, 0x00,
        0xC0, 0x20, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00
    ];

    [Fact]
    public void Encode_Writes_The_Pgvector_Binary_Layout()
    {
        Assert.Equal(Wire, PgVectorCodec.Encode([1f, -2.5f, 0f]));
    }

    [Fact]
    public void Decode_Reads_Hand_Built_Bytes_Including_A_Negative_And_A_Zero()
    {
        Assert.Equal(new[] { 1f, -2.5f, 0f }, PgVectorCodec.Decode(Wire));
    }

    [Fact]
    public void A_Round_Trip_Is_Bitwise_Identical()
    {
        float[] vector = [float.Epsilon, -0f, 3.4e38f, -1e-20f];
        var back = PgVectorCodec.Decode(PgVectorCodec.Encode(vector));
        Assert.Equal(vector.Length, back.Length);
        for (int i = 0; i < vector.Length; i++)
            Assert.Equal(BitConverter.SingleToInt32Bits(vector[i]), BitConverter.SingleToInt32Bits(back[i]));
    }

    [Fact]
    public void A_Dimension_Count_That_Disagrees_With_The_Length_Throws()
    {
        var bytes = (byte[])Wire.Clone();
        bytes[1] = 0x04;
        var ex = Assert.Throws<PgVectorFormatException>(() => PgVectorCodec.Decode(bytes));
        Assert.Contains("4 dimensions", ex.Message);

        Assert.Throws<PgVectorFormatException>(() => PgVectorCodec.Decode(Wire.AsSpan(0, Wire.Length - 1)));
    }

    [Fact]
    public void A_Short_Header_A_Nonzero_Reserved_Field_Or_A_Zero_Dimension_Count_Throws()
    {
        Assert.Throws<PgVectorFormatException>(() => PgVectorCodec.Decode(new byte[] { 0x00, 0x01 }));

        var reserved = (byte[])Wire.Clone();
        reserved[3] = 0x01;
        Assert.Throws<PgVectorFormatException>(() => PgVectorCodec.Decode(reserved));

        Assert.Throws<PgVectorFormatException>(() => PgVectorCodec.Decode(new byte[] { 0x00, 0x00, 0x00, 0x00 }));
    }

    [Fact]
    public void Encode_Refuses_An_Empty_Vector()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => PgVectorCodec.Encode([]));
    }
}
