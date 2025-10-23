namespace RavenBench.Core.Workload;

/// <summary>
/// Represents a workload mix with read, write, and update percentages that sum to 100.
/// Provides weight-based normalization for flexible input formats.
/// </summary>
public readonly struct WorkloadMix
{
    public int ReadPercent { get; }
    public int WritePercent { get; }
    public int UpdatePercent { get; }

    private WorkloadMix(int r, int w, int u)
    {
        if (r + w + u != 100) throw new ArgumentException("Mix must sum to 100");
        ReadPercent = r; WritePercent = w; UpdatePercent = u;
    }

    public static WorkloadMix FromWeights(double read, double write, double update)
    {
        if (read < 0 || write < 0 || update < 0)
            throw new ArgumentOutOfRangeException("weights must be non-negative");

        // If values look like percents (sum approx 100), accept directly
        var sum = read + write + update;
        if (sum <= 0)
            throw new ArgumentException("at least one weight must be > 0");

        // Normalize to percents and distribute remainder based on fractional parts
        var readPercent = read / sum * 100.0;
        var writePercent = write / sum * 100.0;
        var updatePercent = update / sum * 100.0;

        // Floor each percentage and calculate remainder to distribute
        var readInt = (int)Math.Floor(readPercent);
        var writeInt = (int)Math.Floor(writePercent);
        var updateInt = (int)Math.Floor(updatePercent);
        var remainder = 100 - (readInt + writeInt + updateInt);

        // Distribute remainder based on largest fractional parts
        var fractionalParts = new[]
        {
            (readPercent - readInt, 0), // read
            (writePercent - writeInt, 1), // write  
            (updatePercent - updateInt, 2) // update
        };
        
        Array.Sort(fractionalParts, (a, b) => b.Item1.CompareTo(a.Item1));
        
        for (int i = 0; i < remainder; i++)
        {
            switch (fractionalParts[i % 3].Item2)
            {
                case 0: readInt++; break;
                case 1: writeInt++; break;
                case 2: updateInt++; break;
            }
        }

        return new WorkloadMix(readInt, writeInt, updateInt);
    }
}
