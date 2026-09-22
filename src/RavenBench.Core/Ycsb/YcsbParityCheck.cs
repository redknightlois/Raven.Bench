using System.Text.Json;
using RavenBench.Core.Transport;
using RavenBench.Core.Workload;

namespace RavenBench.Core.Ycsb;

/// <summary>The typed operations the parity check compares, as the report names them.</summary>
public static class YcsbParityOperations
{
    public const string ReadById = "read-by-id";
    public const string Insert = "insert";
    public const string UpdateField = "update-field";

    /// <summary>The order the report lists the operations in.</summary>
    public static readonly string[] All = { ReadById, Insert, UpdateField };
}

/// <summary>One product the check addresses: what it is called, where it is, and how it is driven.</summary>
public sealed record YcsbParityProduct(string Name, string Endpoint, string Database, IYcsbTransport Transport);

/// <summary>
/// One (operation, product) pair of the report. A pair that the check could not evaluate carries
/// the failure that stopped it, so an unreachable product is named rather than counted as agreeing.
/// </summary>
public sealed record YcsbParityPair(string Operation, string Product, int Compared, int Mismatches, string? FirstDifference, string? Failure)
{
    public bool Agreed => Failure is null && Mismatches == 0 && Compared > 0;
}

/// <summary>
/// What the check found: every (operation, product) pair, the sample it covered, the reference the
/// products were compared against, and what the check left on each product.
/// </summary>
public sealed record YcsbParityReport(
    int SampleSize,
    string ReferenceProduct,
    IReadOnlyList<string> Products,
    IReadOnlyList<YcsbParityPair> Pairs,
    IReadOnlyList<string> LeftBehind)
{
    /// <summary>Exit status of a check whose every pair agreed.</summary>
    public const int AgreedExitCode = 0;

    /// <summary>Exit status of a check with any disagreement or any product it could not reach.</summary>
    public const int DisagreedExitCode = 1;

    public bool Agreed => Pairs.Count > 0 && Pairs.All(p => p.Agreed);

    public int ExitCode => Agreed ? AgreedExitCode : DisagreedExitCode;
}

/// <summary>
/// The four-product parity check: over a sample of seeded documents, every typed operation must
/// leave the same state on every product. The first product supplied is the reference. Only the
/// product's own id field and key order may differ, because field values are compared parsed and
/// never as raw text. This is not a benchmark: it writes no summary and reports no throughput or
/// latency, and it deletes the sample it wrote.
/// </summary>
public sealed class YcsbParityCheck
{
    /// <summary>The plan's acceptance sample: a thousand documents.</summary>
    public const int DefaultSampleSize = 1000;

    // The id the read must not find. It sits one past the sample, so no product has it.
    private const int MissingIdOffset = 1;

    // Replacement values come from a document generated under this derived seed, so a replacement
    // is as wide as the field it replaces and the document size does not drift.
    private const string UpdateSeedToken = "parity-update";

    private readonly int _seed;
    private readonly int _documentSizeBytes;
    private readonly int _sampleSize;

    public YcsbParityCheck(int seed, int documentSizeBytes, int sampleSize)
    {
        if (sampleSize < 1)
            throw new ArgumentOutOfRangeException(nameof(sampleSize), sampleSize, "The parity sample must hold at least one document.");

        _seed = seed;
        _documentSizeBytes = documentSizeBytes;
        _sampleSize = sampleSize;
    }

    /// <summary>
    /// Runs every operation against every product and reports each pair. A product that fails is
    /// named and the rest are still checked, so the report never stops at the first mismatch.
    /// </summary>
    public async Task<YcsbParityReport> RunAsync(IReadOnlyList<YcsbParityProduct> products, CancellationToken ct)
    {
        if (products.Count == 0)
            throw new ArgumentException("The parity check needs at least one product.", nameof(products));

        var observations = new Dictionary<string, ProductObservation>(products.Count);
        var leftBehind = new List<string>(products.Count);

        foreach (var product in products)
        {
            var observation = await ObserveAsync(product, ct).ConfigureAwait(false);
            observations[product.Name] = observation;
            leftBehind.Add(observation.LeftBehind);
        }

        var reference = products[0];
        var seeded = SeededExpectation();
        var pairs = new List<YcsbParityPair>(products.Count * YcsbParityOperations.All.Length);

        foreach (var operation in YcsbParityOperations.All)
        {
            // The reference is compared against what the seed says each operation must leave, and
            // every other product against what the reference stored.
            foreach (var product in products)
            {
                var expectation = product.Name == reference.Name ? seeded : observations[reference.Name];
                pairs.Add(Compare(operation, product.Name, observations[product.Name], expectation));
            }
        }

        return new YcsbParityReport(_sampleSize, reference.Name, products.Select(p => p.Name).ToList(), pairs, leftBehind);
    }

    /// <summary>The state one product left after each typed operation ran over the whole sample.</summary>
    private sealed record ProductObservation(
        string? Failure,
        Dictionary<string, IReadOnlyDictionary<string, string>?> AfterInsert,
        Dictionary<string, IReadOnlyDictionary<string, string>?> AfterUpdate,
        Dictionary<string, string> ReadOutcomes,
        string LeftBehind);

    private async Task<ProductObservation> ObserveAsync(YcsbParityProduct product, CancellationToken ct)
    {
        var afterInsert = new Dictionary<string, IReadOnlyDictionary<string, string>?>(_sampleSize);
        var afterUpdate = new Dictionary<string, IReadOnlyDictionary<string, string>?>(_sampleSize);
        var readOutcomes = new Dictionary<string, string>(_sampleSize + 1);

        if (product.Transport is not IInspectsStoredDocuments inspector)
        {
            return new ProductObservation(
                $"{product.Name} at {product.Endpoint} exposes no read-back surface, so its stored state cannot be compared.",
                afterInsert, afterUpdate, readOutcomes, $"{product.Name}: nothing was written.");
        }

        try
        {
            await product.Transport.EnsureDatabaseExistsAsync(product.Database).ConfigureAwait(false);

            for (int i = 1; i <= _sampleSize; i++)
            {
                var id = BenchIds.IdFor(i);
                var insert = await product.Transport
                    .ExecuteAsync(new InsertOperation<string> { Id = id, Payload = Document(id) }, ct)
                    .ConfigureAwait(false);

                if (insert.IsSuccess == false)
                    throw new InvalidOperationException($"the single insert of '{id}' failed: {insert.ErrorDetails}");

                afterInsert[id] = Snapshot(await inspector.ReadStoredFieldsAsync(id, ct).ConfigureAwait(false));

                var read = await product.Transport.ExecuteAsync(new ReadOperation { Id = id }, ct).ConfigureAwait(false);
                readOutcomes[id] = read.IsSuccess ? "found" : "not-found";

                var update = UpdateFor(i, id);
                var updated = await product.Transport.ExecuteAsync(update, ct).ConfigureAwait(false);
                if (updated.IsSuccess == false)
                    throw new InvalidOperationException($"the one-field update of '{id}' failed: {updated.ErrorDetails}");

                afterUpdate[id] = Snapshot(await inspector.ReadStoredFieldsAsync(id, ct).ConfigureAwait(false));
            }

            var missingId = BenchIds.IdFor(_sampleSize + MissingIdOffset);
            var missing = await product.Transport.ExecuteAsync(new ReadOperation { Id = missingId }, ct).ConfigureAwait(false);
            readOutcomes[missingId] = missing.IsSuccess ? "found" : "not-found";

            for (int i = 1; i <= _sampleSize; i++)
                await inspector.DeleteStoredDocumentAsync(BenchIds.IdFor(i), ct).ConfigureAwait(false);

            return new ProductObservation(null, afterInsert, afterUpdate, readOutcomes,
                $"{product.Name}: the {_sampleSize}-document sample was removed from database '{product.Database}'.");
        }
        catch (Exception ex)
        {
            return new ProductObservation(
                $"{product.Name} at {product.Endpoint} could not be checked: {ex.Message}",
                afterInsert, afterUpdate, readOutcomes,
                $"{product.Name}: the sample may remain in database '{product.Database}'; the check stopped on a failure.");
        }
    }

    private YcsbParityPair Compare(string operation, string product, ProductObservation observed, ProductObservation reference)
    {
        if (observed.Failure is not null)
            return new YcsbParityPair(operation, product, 0, 0, null, observed.Failure);

        if (reference.Failure is not null)
            return new YcsbParityPair(operation, product, 0, 0, null, $"the reference product could not be checked: {reference.Failure}");

        return operation switch
        {
            YcsbParityOperations.ReadById => CompareOutcomes(operation, product, observed.ReadOutcomes, reference.ReadOutcomes),
            YcsbParityOperations.Insert => CompareFields(operation, product, observed.AfterInsert, reference.AfterInsert),
            YcsbParityOperations.UpdateField => CompareFields(operation, product, observed.AfterUpdate, reference.AfterUpdate),
            _ => throw new ArgumentOutOfRangeException(nameof(operation), operation, "Unknown parity operation.")
        };
    }

    private static YcsbParityPair CompareOutcomes(string operation, string product, Dictionary<string, string> observed, Dictionary<string, string> reference)
    {
        int mismatches = 0;
        string? first = null;

        foreach (var (id, expected) in reference)
        {
            var actual = observed.TryGetValue(id, out var value) ? value : "missing from this product's run";
            if (actual == expected)
                continue;

            mismatches++;
            first ??= $"'{id}' reads {actual}, the reference reads {expected}";
        }

        return new YcsbParityPair(operation, product, reference.Count, mismatches, first, null);
    }

    private static YcsbParityPair CompareFields(
        string operation,
        string product,
        Dictionary<string, IReadOnlyDictionary<string, string>?> observed,
        Dictionary<string, IReadOnlyDictionary<string, string>?> reference)
    {
        int mismatches = 0;
        string? first = null;

        foreach (var (id, expected) in reference)
        {
            var actual = observed.TryGetValue(id, out var value) ? value : null;
            var difference = DescribeDifference(id, expected, actual);
            if (difference is null)
                continue;

            mismatches++;
            first ??= difference;
        }

        return new YcsbParityPair(operation, product, reference.Count, mismatches, first, null);
    }

    /// <summary>
    /// The one comparison rule the check and the gated test share: the field names and values must
    /// agree, and the product's own id field and key order do not count.
    /// </summary>
    public static string? DescribeDifference(string id, IReadOnlyDictionary<string, string>? expected, IReadOnlyDictionary<string, string>? actual)
    {
        if (expected is null)
            return actual is null ? null : $"'{id}' is absent from the reference but present here";
        if (actual is null)
            return $"'{id}' is absent from this product";

        for (int i = 0; i < PayloadGenerator.FieldCount; i++)
        {
            var name = PayloadGenerator.FieldName(i);
            var hasExpected = expected.TryGetValue(name, out var expectedValue);
            var hasActual = actual.TryGetValue(name, out var actualValue);

            if (hasExpected == false || hasActual == false)
                return $"'{id}' field {name} is missing from {(hasExpected ? "this product" : "the reference")}";
            if (expectedValue != actualValue)
                return $"'{id}' field {name} differs";
        }

        return null;
    }

    /// <summary>The seeded fields of one sample document, as every product stores them.</summary>
    public static IReadOnlyDictionary<string, string> ExpectedFields(int seed, string id, int documentSizeBytes)
    {
        using var document = JsonDocument.Parse(PayloadGenerator.Generate(seed, id, documentSizeBytes));
        var fields = new Dictionary<string, string>(PayloadGenerator.FieldCount);
        for (int i = 0; i < PayloadGenerator.FieldCount; i++)
        {
            var name = PayloadGenerator.FieldName(i);
            fields[name] = document.RootElement.GetProperty(name).GetString()!;
        }

        return fields;
    }

    /// <summary>
    /// What the seed says every product must hold after each operation: the seeded document after
    /// the insert, the same document with one field changed after the update, and no document for
    /// the one id outside the sample.
    /// </summary>
    private ProductObservation SeededExpectation()
    {
        var afterInsert = new Dictionary<string, IReadOnlyDictionary<string, string>?>(_sampleSize);
        var afterUpdate = new Dictionary<string, IReadOnlyDictionary<string, string>?>(_sampleSize);
        var readOutcomes = new Dictionary<string, string>(_sampleSize + 1);

        for (int i = 1; i <= _sampleSize; i++)
        {
            var id = BenchIds.IdFor(i);
            var stored = ExpectedFields(_seed, id, _documentSizeBytes);
            var update = UpdateFor(i, id);
            var updated = new Dictionary<string, string>(stored) { [update.FieldName] = update.Value };

            afterInsert[id] = stored;
            afterUpdate[id] = updated;
            readOutcomes[id] = "found";
        }

        readOutcomes[BenchIds.IdFor(_sampleSize + MissingIdOffset)] = "not-found";

        return new ProductObservation(null, afterInsert, afterUpdate, readOutcomes, "the seeded expectation writes nothing.");
    }

    // Copied: a read-back may hand out a live view that a later operation would change.
    private static IReadOnlyDictionary<string, string>? Snapshot(IReadOnlyDictionary<string, string>? fields) =>
        fields is null ? null : new Dictionary<string, string>(fields);

    private string Document(string id) => PayloadGenerator.Generate(_seed, id, _documentSizeBytes);

    /// <summary>
    /// The one-field update for an id: the field follows the id's position in the sample and the
    /// new value comes from a document under a derived seed, so it is the same on every product.
    /// </summary>
    private UpdateFieldOperation UpdateFor(int ordinal, string id)
    {
        var index = ordinal % PayloadGenerator.FieldCount;
        var name = PayloadGenerator.FieldName(index);
        var replacement = ExpectedFields(SeedMixer.Derive(_seed, UpdateSeedToken), id, _documentSizeBytes)[name];

        return new UpdateFieldOperation { Id = id, FieldName = name, Value = replacement };
    }
}
