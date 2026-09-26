using System.Net;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using RavenBench.Core;
using RavenBench.Core.Aggregate;
using RavenBench.Core.Transport;
using RavenBench.Core.Workload;
using RavenBench.Ycsb;

namespace RavenBench.Aggregate;

/// <summary>
/// One product under the aggregate parity check: how it loads the sample and waits until every
/// shape is queryable, how it runs a grouped aggregate, and how it deletes what it wrote.
/// </summary>
public sealed record AggregateParityProduct(
    string Name,
    string Endpoint,
    Func<IReadOnlyList<AggregateDocument>, CancellationToken, Task> LoadAsync,
    Func<GroupedAggregateOperation, CancellationToken, Task<TransportResult>> QueryAsync,
    Func<Task> CleanupAsync);

/// <summary>One (shape, product) pair. A pair agrees only when it compared at least one group and found no difference.</summary>
public sealed record AggregateParityPair(string Shape, string Product, int Compared, string? FirstDifference, string? Failure)
{
    public bool Agreed => Failure is null && FirstDifference is null && Compared > 0;
}

public sealed record AggregateParityReport(int SampleSize, int Seed, string ReferenceProduct, IReadOnlyList<string> Products,
    IReadOnlyList<AggregateParityPair> Pairs, TimeSpan NonStaleTimeout)
{
    public int ExitCode => Pairs.All(p => p.Agreed) ? 0 : 1;
}

/// <summary>
/// Over a seeded sample, every aggregate shape must return the same ordered groups and the same
/// exact values on every product. The first product is the reference: it is compared against the
/// result the shared ordering computes from the sample, and every other product against the
/// reference's result.
/// </summary>
public sealed class AggregateParityCheck
{
    public const int DefaultSampleSize = 1_000;
    public const int CategoryCardinality = 20;
    public const int RegionCardinality = 200;
    public const int DocumentSizeBytes = 256;

    /// <summary>The count shape cuts at 5 of 20 categories, so the sample exercises a cut inside the group set.</summary>
    public const int CountTopN = 5;

    /// <summary>The plan's top 100 for the region shapes.</summary>
    public const int RegionTopN = 100;

    /// <summary>How long the check waits for RavenDB to report every aggregate index non-stale.</summary>
    public static readonly TimeSpan NonStaleTimeout = TimeSpan.FromMinutes(2);

    private const int LoadBatchSize = 500;

    private readonly int _seed;
    private readonly int _sampleSize;

    public AggregateParityCheck(int seed, int sampleSize)
    {
        if (sampleSize < 1)
            throw new ArgumentOutOfRangeException(nameof(sampleSize), sampleSize, "The aggregate parity sample needs at least one document.");
        _seed = seed;
        _sampleSize = sampleSize;
    }

    /// <summary>The operation of each shape over the parity sample; the filtered shape takes the hottest category.</summary>
    public static IReadOnlyList<(string Shape, GroupedAggregateOperation Operation)> Operations() =>
    [
        (AggregateShapes.CountByCategory, AggregateShapes.Create(AggregateShapes.CountByCategory, CountTopN)),
        (AggregateShapes.SumByRegion, AggregateShapes.Create(AggregateShapes.SumByRegion, RegionTopN)),
        (AggregateShapes.FilteredGroup, AggregateShapes.Create(AggregateShapes.FilteredGroup, RegionTopN, AggregateDataSet.CategoryKey(1, CategoryCardinality)))
    ];

    public IReadOnlyList<AggregateDocument> Sample() => new AggregateDataSet(new AggregateDataSpec(_seed, _sampleSize, DocumentSizeBytes,
        CategoryCardinality, RegionCardinality, GroupDistribution.Parse(GroupDistribution.Zipfian, 0.99))).Generate().ToList();

    public async Task<AggregateParityReport> RunAsync(IReadOnlyList<AggregateParityProduct> products, CancellationToken ct)
    {
        if (products.Count == 0)
            throw new ArgumentException("The aggregate parity check needs at least one product.", nameof(products));

        var sample = Sample();
        var operations = Operations();
        var truth = operations.ToDictionary(o => o.Shape, o => (IReadOnlyList<AggregateGroup>?)AggregateOrdering.Compute(o.Operation, sample));
        var reference = products[0].Name;
        IReadOnlyDictionary<string, IReadOnlyList<AggregateGroup>?>? referenceResults = null;
        var pairs = new List<AggregateParityPair>();

        foreach (var product in products)
        {
            var (results, failure) = await ObserveAsync(product, sample, operations, ct);
            var expected = referenceResults ?? truth;
            foreach (var (shape, _) in operations)
            {
                var actual = results.GetValueOrDefault(shape);
                if (failure is not null || actual is null)
                    pairs.Add(new AggregateParityPair(shape, product.Name, 0, null, failure ?? "no result"));
                else if (expected[shape] is not { } want)
                    pairs.Add(new AggregateParityPair(shape, product.Name, 0, null, $"the reference {reference} returned no result to compare with"));
                else
                    pairs.Add(new AggregateParityPair(shape, product.Name, Math.Min(want.Count, actual.Count), Compare(product.Name, shape, want, actual), null));
            }
            if (product.Name == reference)
                referenceResults = operations.ToDictionary(o => o.Shape, o => failure is null ? results.GetValueOrDefault(o.Shape) : null);
        }

        return new AggregateParityReport(_sampleSize, _seed, reference, products.Select(p => p.Name).ToList(), pairs, NonStaleTimeout);
    }

    /// <summary>
    /// The first difference between two ordered group lists, or null when they are equal. The
    /// message names the product, the shape, the position, the expected group and the actual group.
    /// </summary>
    public static string? Compare(string product, string shape, IReadOnlyList<AggregateGroup> expected, IReadOnlyList<AggregateGroup> actual)
    {
        for (int i = 0; i < Math.Max(expected.Count, actual.Count); i++)
        {
            AggregateGroup? want = i < expected.Count ? expected[i] : null;
            AggregateGroup? got = i < actual.Count ? actual[i] : null;
            if (want is { } w && got is { } g && AggregateOrdering.KeyComparer.Equals(w.Key, g.Key) && w.Value == g.Value)
                continue;
            return $"{product} {shape} position {i}: expected {Describe(want)}, actual {Describe(got)}";
        }
        return null;
    }

    private static string Describe(AggregateGroup? group) => group is { } g ? $"({g.Key}, {g.Value})" : "(none)";

    private static async Task<(Dictionary<string, IReadOnlyList<AggregateGroup>?> Results, string? Failure)> ObserveAsync(
        AggregateParityProduct product, IReadOnlyList<AggregateDocument> sample, IReadOnlyList<(string Shape, GroupedAggregateOperation Operation)> operations, CancellationToken ct)
    {
        var results = new Dictionary<string, IReadOnlyList<AggregateGroup>?>();
        string? failure = null;
        try
        {
            await product.LoadAsync(sample, ct);
            foreach (var (shape, operation) in operations)
            {
                var result = await product.QueryAsync(operation, ct);
                if (result.IsSuccess == false)
                    throw new InvalidOperationException($"the {shape} query failed: {result.ErrorDetails}");
                if (result.IsStale is null)
                    throw new InvalidDataException($"the {shape} result records no stale flag");
                if (result.IsStale == true)
                    throw new InvalidOperationException($"the {shape} result is stale after the index reported non-stale");
                results[shape] = result.Groups ?? throw new InvalidDataException($"the {shape} result carries no groups");
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            failure = $"{product.Name} at {product.Endpoint}: {ex.GetType().Name}: {ex.Message}";
        }

        try
        {
            await product.CleanupAsync();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            failure = (failure is null ? "" : failure + "; ") + $"cleanup of {product.Name} failed, the sample may be left behind: {ex.GetType().Name}: {ex.Message}";
        }
        return (results, failure);
    }

    /// <summary>
    /// RavenDB over raw HTTP. The sample goes into the named database, which the check creates when
    /// it is missing; the check waits for the map-reduce indexes to report non-stale, and afterwards
    /// deletes the database it created, or the indexes and the sample documents it wrote.
    /// </summary>
    public static AggregateParityProduct RavenDb(string url, string database)
    {
        var transport = new RawHttpTransport(url, database, CompressionMode.Identity, HttpVersion.Version11);
        bool created = false;
        return new AggregateParityProduct(
            YcsbRunner.RavendbTarget,
            transport.RecordedEndpoint,
            async (sample, ct) =>
            {
                using (var store = HttpHelper.Create(url, database, HttpVersion.Version11))
                {
                    if (await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(database), ct) is null)
                    {
                        await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(database)), ct);
                        created = true;
                    }
                }
                await LoadInBatchesAsync(transport, sample, ct);
                await transport.EnsureAggregateIndexesAsync(ct);
                await transport.WaitForNonStaleAggregateIndexesAsync(NonStaleTimeout, ct);
            },
            transport.ExecuteAsync,
            async () =>
            {
                transport.Dispose();
                using var store = HttpHelper.Create(url, database, HttpVersion.Version11);
                if (created)
                {
                    await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(database, hardDelete: true));
                    return;
                }
                foreach (var index in AggregateShapes.RavenDbIndexes())
                    await store.Maintenance.SendAsync(new DeleteIndexOperation(index.GetProperty("Name").GetString()!));
                var delete = await store.Operations.SendAsync(new DeleteByQueryOperation(new IndexQuery { Query = $"from '{AggregateDocument.RavenDbCollection}'" }));
                await delete.WaitForCompletionAsync(NonStaleTimeout);
            });
    }

    /// <summary>
    /// MongoDB under either target: the collection is created, the target's indexes are created
    /// (none for the plain target), the sample is inserted, and the collection is dropped afterwards.
    /// </summary>
    public static AggregateParityProduct Mongo(MongoYcsbTransport transport) => new(
        transport.Target,
        transport.RecordedEndpoint,
        async (sample, ct) =>
        {
            await transport.EnsureAggregateIndexesAsync(ct);
            await LoadInBatchesAsync(transport, sample, ct);
        },
        transport.ExecuteAsync,
        () => transport.DropAggregateCollectionAsync(CancellationToken.None));

    internal static async Task LoadInBatchesAsync(IYcsbTransport transport, IReadOnlyList<AggregateDocument> sample, CancellationToken ct)
    {
        foreach (var batch in sample.Chunk(LoadBatchSize))
        {
            var result = await transport.ExecuteAsync(new BulkInsertOperation<AggregateDocument>
            {
                Documents = batch.Select(d => new DocumentToWrite<AggregateDocument> { Id = d.Id, Document = d }).ToList()
            }, ct);
            if (result.IsSuccess == false)
                throw new InvalidOperationException($"The {transport.ProductName} sample load failed: {result.ErrorDetails}");
        }
    }
}
