namespace RavenBench.Core;

/// <summary>
/// Mixes a run seed and a token into a second seed with FNV-1a. Addition would alias, making
/// (seed + 1, token) draw what (seed, next token) draws, and <c>string.GetHashCode</c> is
/// randomised per process, which would break reproducibility across runs.
/// </summary>
public static class SeedMixer
{
    public static int Derive(int seed, string token)
    {
        const ulong offsetBasis = 14695981039346656037UL;
        const ulong prime = 1099511628211UL;

        ulong hash = offsetBasis;
        for (int shift = 0; shift < 32; shift += 8)
            hash = (hash ^ (byte)(seed >> shift)) * prime;
        foreach (var c in token)
            hash = (hash ^ c) * prime;

        return unchecked((int)(hash ^ (hash >> 32)));
    }
}
