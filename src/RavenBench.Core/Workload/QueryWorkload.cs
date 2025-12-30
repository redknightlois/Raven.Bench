namespace RavenBench.Core.Workload;

public sealed class QueryWorkload : IWorkload
{
    private readonly IKeyDistribution _distribution;
    private long _maxKey;

    public QueryWorkload(IKeyDistribution distribution, long initialKeyspace)
    {
        if (initialKeyspace <= 0)
            throw new InvalidOperationException("Query profile requires preloaded documents. Use --preload to seed data.");

        _distribution = distribution;
        _maxKey = initialKeyspace;
    }

    public OperationBase NextOperation(Random rng)
    {
        var k = _distribution.NextKey(rng, (int)Math.Min(_maxKey, int.MaxValue));
        var docId = IdFor(k);
        return new QueryOperation
        {
            QueryText = "from @all_docs where id() = $id",
            Parameters = new Dictionary<string, object?> { ["id"] = docId }
        };
    }

    public IWorkload? CreateWarmupWorkload(long preloadCount, IKeyDistribution distribution)
    {
        // QueryWorkload is already read-only, use it directly for warmup
        return null;
    }

    private static string IdFor(long i) => $"bench/{i:D8}";
}
