namespace RavenBench.Core.Transport;

/// <summary>
/// An optional capability of a transport whose product reports the on-disk size of what it stores.
/// It is not part of <see cref="IYcsbTransport"/>: a product that exposes no size does not implement
/// this, so it never has to fake one. The caller tests for the capability before it reads a size.
/// </summary>
public interface IReportsStorageSize
{
    /// <summary>
    /// The name of the product statistic the figure is read from, as that product spells it. The
    /// result carries it beside the figure, so a reader knows which statistic the number is.
    /// </summary>
    string StorageSizeMetricName { get; }

    /// <summary>
    /// The on-disk size the product reports, in bytes. It is a measurement the product makes, never
    /// an estimate from a document count. Read after a load completes, outside a measurement window.
    /// </summary>
    Task<long> GetStorageSizeBytesAsync();
}
