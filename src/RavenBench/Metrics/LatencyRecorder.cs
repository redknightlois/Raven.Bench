namespace RavenBench.Metrics;

public sealed class LatencyRecorder
{
    private readonly bool _record;
    private readonly int _maxSamples;
    private long _count;
    
    // Lock-free reservoir sampling using concurrent array and atomic operations
    private readonly long[] _reservoir;
    private readonly ThreadLocal<Random> _rng = new(() => new Random(unchecked(Environment.TickCount * 397 ^ Thread.CurrentThread.ManagedThreadId)));

    public LatencyRecorder(bool recordLatencies, int maxSamples = 100_000)
    {
        _record = recordLatencies;
        _maxSamples = Math.Max(1024, maxSamples);
        _reservoir = new long[_maxSamples];
    }

    public void Record(long micros)
    {
        if (!_record) return;
        
        var n = Interlocked.Increment(ref _count);
        
        if (n <= _maxSamples)
        {
            // Fill the reservoir initially
            _reservoir[n - 1] = micros;
        }
        else
        {
            // Reservoir sampling: replace with probability maxSamples/n
            var randomIndex = _rng.Value!.NextInt64(n);
            if (randomIndex < _maxSamples)
            {
                // Use volatile write to ensure visibility across threads
                Volatile.Write(ref _reservoir[randomIndex], micros);
            }
        }
    }

    public double GetPercentile(int p)
    {
        if (!_record) return 0;
        
        var currentCount = Volatile.Read(ref _count);
        if (currentCount == 0) return 0;
        
        // Copy samples for sorting (up to filled capacity)
        var sampleCount = Math.Min((int)currentCount, _maxSamples);
        var samples = new long[sampleCount];
        
        for (int i = 0; i < sampleCount; i++)
        {
            // Yes, volatile read is needed: multiple worker threads write via Volatile.Write(), 
            // and we need to ensure visibility of the most recent values when reading for percentile calculation
            samples[i] = Volatile.Read(ref _reservoir[i]);
        }
        
        Array.Sort(samples);
        var rank = Math.Clamp((int)Math.Ceiling((p / 100.0) * samples.Length) - 1, 0, samples.Length - 1);
        return samples[rank];
    }
}
