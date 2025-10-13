namespace RavenBench.Workload;

public sealed class ReadWorkload : IWorkload
{
    private readonly IKeyDistribution _distribution;
    private long _maxKey;

    public ReadWorkload(IKeyDistribution distribution, long initialKeyspace)
    {
        if (initialKeyspace <= 0)
            throw new InvalidOperationException("Read profile requires preloaded documents. Use --preload to seed data.");

        _distribution = distribution;
        _maxKey = initialKeyspace;
    }

    public OperationBase NextOperation(Random rng)
    {
        var k = _distribution.NextKey(rng, (int)Math.Min(_maxKey, int.MaxValue));
        return new ReadOperation { Id = IdFor(k) };
    }

    private static string IdFor(long i) => $"bench/{i:D8}";
}

