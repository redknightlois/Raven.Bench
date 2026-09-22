namespace RavenBench.Core.Transport;

/// <summary>
/// Optional transport capability: reads one stored document's ycsb fields back and deletes it.
/// Not part of <see cref="IYcsbTransport"/>, whose results carry byte counts and not content.
/// </summary>
public interface IInspectsStoredDocuments
{
    /// <summary>
    /// The ycsb field names and values stored under the id, or null if the product says there is
    /// no such document. The product's own id field is not included, and key order is not kept.
    /// </summary>
    Task<IReadOnlyDictionary<string, string>?> ReadStoredFieldsAsync(string id, CancellationToken ct);

    /// <summary>
    /// Deletes the document with this id. Deleting an id the product does not have is not an
    /// error.
    /// </summary>
    Task DeleteStoredDocumentAsync(string id, CancellationToken ct);
}
