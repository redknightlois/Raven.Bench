namespace RavenBench.Core.Workload;

/// <summary>
/// Server-side JavaScript patches against single preloaded documents.
/// Exercises the JS engine and the write path without shipping the document over the wire.
/// </summary>
public sealed class PatchWorkload : IWorkload
{
    private const string Script = "this.Patched = (this.Patched || 0) + 1;";

    private readonly IKeyDistribution _distribution;
    private readonly long _maxKey;

    public PatchWorkload(IKeyDistribution distribution, long initialKeyspace)
    {
        if (initialKeyspace <= 0)
            throw new InvalidOperationException("Patch profile requires preloaded documents. Use --preload to seed data.");

        _distribution = distribution;
        _maxKey = initialKeyspace;
    }

    public OperationBase NextOperation(Random rng)
    {
        var k = _distribution.NextKey(rng, (int)Math.Min(_maxKey, int.MaxValue));
        return new DocumentPatchOperation
        {
            Id = BenchIds.IdFor(k),
            Script = Script
        };
    }
}
