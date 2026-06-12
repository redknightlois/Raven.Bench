namespace RavenBench.Core.Workload;

/// <summary>
/// Attachment operations against preloaded documents (blob IO path).
/// Attachment names are deterministic per document so Get/Delete target what Put created.
/// </summary>
public sealed class AttachmentWorkload : IWorkload
{
    private readonly IKeyDistribution _distribution;
    private readonly long _maxKey;
    private readonly AttachmentOperationKind _kind;
    private readonly byte[] _payload;

    public AttachmentWorkload(IKeyDistribution distribution, long initialKeyspace, AttachmentOperationKind kind, int attachmentSizeBytes, int seed)
    {
        if (initialKeyspace <= 0)
            throw new InvalidOperationException("Attachments profile requires preloaded documents. Use --preload to seed data.");

        _distribution = distribution;
        _maxKey = initialKeyspace;
        _kind = kind;
        _payload = new byte[attachmentSizeBytes];
        new Random(seed).NextBytes(_payload);
    }

    public static string NameFor(string documentId) => $"a_{documentId}";

    public OperationBase NextOperation(Random rng)
    {
        var k = _distribution.NextKey(rng, (int)Math.Min(_maxKey, int.MaxValue));
        var docId = BenchIds.IdFor(k);

        return new AttachmentOperation
        {
            DocumentId = docId,
            Name = NameFor(docId),
            Kind = _kind,
            Payload = _kind == AttachmentOperationKind.Put ? _payload : null
        };
    }
}
