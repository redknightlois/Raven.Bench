using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using RavenBench.Core;
using RavenBench.Core.Transport;
using RavenBench.Core.Workload;
using RavenBench.Core.Ycsb;
using Xunit;

namespace RavenBench.Tests.Ycsb;

/// <summary>
/// Pins the shape of the parity report: every pair of operation and product is reported, one
/// disagreement hides no other pair, a product the check cannot reach is named with its endpoint,
/// and the exit status follows the report. The live four-product run stays behind its gate.
/// </summary>
public class YcsbParityCheckTests
{
    private const int Seed = 42;
    private const int DocumentSize = 1024;
    private const int Sample = 5;

    [Fact]
    public async Task Every_Operation_And_Product_Pair_Is_Reported_When_All_Agree()
    {
        var report = await RunAsync(Product("reference"), Product("second"));

        report.Pairs.Should().HaveCount(YcsbParityOperations.All.Length * 2);
        report.Pairs.Select(p => (p.Operation, p.Product)).Should().OnlyHaveUniqueItems();
        report.Pairs.Should().OnlyContain(p => p.Agreed);
        report.ReferenceProduct.Should().Be("reference");
        report.Agreed.Should().BeTrue();
    }

    [Fact]
    public async Task A_Product_That_Disagrees_On_One_Operation_Is_Named_And_The_Other_Pairs_Still_Report()
    {
        var report = await RunAsync(Product("reference"), Product("laggard", ignoreUpdates: true));

        var failed = report.Pairs.Single(p => p.Product == "laggard" && p.Operation == YcsbParityOperations.UpdateField);
        failed.Agreed.Should().BeFalse();
        failed.Mismatches.Should().Be(Sample);
        failed.FirstDifference.Should().NotBeNullOrWhiteSpace();

        // The report does not stop at the first mismatch.
        report.Pairs.Where(p => p.Product == "laggard" && p.Operation != YcsbParityOperations.UpdateField)
            .Should().OnlyContain(p => p.Agreed);
        report.Pairs.Where(p => p.Product == "reference").Should().OnlyContain(p => p.Agreed);
        report.Pairs.Should().HaveCount(YcsbParityOperations.All.Length * 2);
    }

    [Fact]
    public async Task A_Product_The_Check_Cannot_Reach_Is_Named_With_Its_Endpoint_On_Every_Operation()
    {
        var unreachable = new YcsbParityProduct("offline", "mongodb://offline:27017", "ycsb", new UnreachableProduct());

        var report = await RunAsync(Product("reference"), unreachable);

        var pairs = report.Pairs.Where(p => p.Product == "offline").ToList();
        pairs.Should().HaveCount(YcsbParityOperations.All.Length);
        pairs.Should().OnlyContain(p => p.Agreed == false);
        pairs.Should().OnlyContain(p => p.Failure!.Contains("offline") && p.Failure.Contains("mongodb://offline:27017"));
        report.LeftBehind.Should().Contain(s => s.Contains("offline"));
    }

    [Fact]
    public async Task The_Exit_Status_Follows_The_Report()
    {
        var agreed = await RunAsync(Product("reference"), Product("second"));
        var disagreed = await RunAsync(Product("reference"), Product("laggard", ignoreUpdates: true));
        var unreachable = await RunAsync(Product("reference"), new YcsbParityProduct("offline", "tcp://offline", "ycsb", new UnreachableProduct()));

        agreed.ExitCode.Should().Be(YcsbParityReport.AgreedExitCode);
        disagreed.ExitCode.Should().NotBe(agreed.ExitCode);
        unreachable.ExitCode.Should().NotBe(agreed.ExitCode);
    }

    [Fact]
    public async Task The_Check_Removes_Its_Own_Sample_From_Every_Product()
    {
        var reference = new InMemoryProduct();
        var second = new InMemoryProduct();

        await RunAsync(
            new YcsbParityProduct("reference", "memory://reference", "ycsb", reference),
            new YcsbParityProduct("second", "memory://second", "ycsb", second));

        reference.Stored.Should().BeEmpty();
        second.Stored.Should().BeEmpty();
    }

    [Fact]
    public async Task A_Transport_Error_On_A_Read_Is_Not_Read_As_Not_Found()
    {
        var broken = new YcsbParityProduct("broken", "memory://broken", "ycsb", new InMemoryProduct(missingReadError: "connection reset by peer"));

        var report = await RunAsync(Product("reference"), broken);

        var read = report.Pairs.Single(p => p.Operation == YcsbParityOperations.ReadById && p.Product == "broken");
        read.Agreed.Should().BeFalse();
        read.Failure.Should().Contain("connection reset by peer");
        report.ExitCode.Should().NotBe(YcsbParityReport.AgreedExitCode);
    }

    [Fact]
    public async Task A_Run_That_Fails_Partway_Leaves_A_Clean_Sample_For_The_Next_Run()
    {
        var product = new YcsbParityProduct("flaky", "memory://flaky", "ycsb", new InMemoryProduct(failFirstUpdate: true));

        var first = await RunAsync(product);
        var second = await RunAsync(product);

        first.Agreed.Should().BeFalse();
        first.Pairs.Should().OnlyContain(p => p.Failure!.Contains("the connection dropped during the update"));
        second.Agreed.Should().BeTrue();
    }

    [Fact]
    public async Task The_Check_Deletes_Only_The_Documents_It_Inserted()
    {
        var transport = new InMemoryProduct();
        var foreign = new Dictionary<string, Dictionary<string, string>>();
        foreach (var id in Enumerable.Range(1, Sample).Select(i => BenchIds.IdFor(i)))
        {
            foreign[id] = new Dictionary<string, string> { ["field0"] = "not written by the check" };
            transport.Preload(id, foreign[id]);
        }

        var report = await RunAsync(new YcsbParityProduct("seeded", "memory://seeded", "ycsb", transport));

        transport.Stored.Should().BeEquivalentTo(foreign);
        report.LeftBehind.Should().ContainSingle().Which.Should().Contain("the 0 documents this check inserted");
    }

    private static Task<YcsbParityReport> RunAsync(params YcsbParityProduct[] products) =>
        new YcsbParityCheck(Seed, DocumentSize, Sample).RunAsync(products, CancellationToken.None);

    private static YcsbParityProduct Product(string name, bool ignoreUpdates = false) =>
        new(name, $"memory://{name}", "ycsb", new InMemoryProduct(ignoreUpdates));

    /// <summary>
    /// A product that stores the ycsb documents in memory, optionally dropping the one-field update,
    /// answering a read of an absent id with a transport error, or failing its first update once.
    /// An insert of an id it already holds overwrites it, as a RavenDB put does.
    /// </summary>
    private sealed class InMemoryProduct(bool ignoreUpdates = false, string? missingReadError = null, bool failFirstUpdate = false) : IYcsbTransport, IInspectsStoredDocuments
    {
        private readonly Dictionary<string, Dictionary<string, string>> _stored = new();
        private bool _updateFailed;

        public IReadOnlyDictionary<string, Dictionary<string, string>> Stored => _stored;

        public void Preload(string id, Dictionary<string, string> document) => _stored[id] = new Dictionary<string, string>(document);

        public string ProductName => "InMemory";
        public bool ReportsWireBytes => false;
        public string RecordedEndpoint => "stub";

        public Task<TransportResult> ExecuteAsync(OperationBase op, CancellationToken ct)
        {
            switch (op)
            {
                case InsertOperation<string> insert:
                    _stored[insert.Id] = new Dictionary<string, string>(YcsbParityCheck.ExpectedFields(Seed, insert.Id, DocumentSize));
                    return Task.FromResult(new TransportResult(0, 0));
                case ReadOperation read:
                    return Task.FromResult(_stored.ContainsKey(read.Id)
                        ? new TransportResult(0, 0)
                        : missingReadError is null ? TransportResult.DocumentNotFound(read.Id) : new TransportResult(0, 0, missingReadError));
                case UpdateFieldOperation update:
                    if (failFirstUpdate && _updateFailed == false)
                    {
                        _updateFailed = true;
                        throw new InvalidOperationException("the connection dropped during the update");
                    }
                    if (_stored.TryGetValue(update.Id, out var document) == false)
                        return Task.FromResult(TransportResult.DocumentNotFound(update.Id));
                    if (ignoreUpdates == false)
                        document[update.FieldName] = update.Value;
                    return Task.FromResult(new TransportResult(0, 0));
                default:
                    throw new NotSupportedException($"The in-memory product cannot execute {op.GetType().Name}.");
            }
        }

        public Task<IReadOnlyDictionary<string, string>?> ReadStoredFieldsAsync(string id, CancellationToken ct) =>
            Task.FromResult(_stored.TryGetValue(id, out var document) ? (IReadOnlyDictionary<string, string>?)document : null);

        public Task DeleteStoredDocumentAsync(string id, CancellationToken ct)
        {
            _stored.Remove(id);
            return Task.CompletedTask;
        }

        public Task PutAsync<T>(string id, T document) => throw new NotSupportedException();
        public Task EnsureDatabaseExistsAsync(string databaseName) => Task.CompletedTask;
        public Task<long> GetDocumentCountAsync(string idPrefix) => Task.FromResult((long)_stored.Count);
        public Task<string> GetServerVersionAsync() => Task.FromResult("in-memory");
        public void Dispose() { }
    }

    /// <summary>A product whose endpoint does not answer; every operation it is asked for fails.</summary>
    private sealed class UnreachableProduct : IYcsbTransport, IInspectsStoredDocuments
    {
        public string ProductName => "Unreachable";
        public bool ReportsWireBytes => false;
        public string RecordedEndpoint => "stub";

        public Task<TransportResult> ExecuteAsync(OperationBase op, CancellationToken ct) =>
            throw new InvalidOperationException("the endpoint refused the connection");

        public Task<IReadOnlyDictionary<string, string>?> ReadStoredFieldsAsync(string id, CancellationToken ct) =>
            throw new InvalidOperationException("the endpoint refused the connection");

        public Task DeleteStoredDocumentAsync(string id, CancellationToken ct) =>
            throw new InvalidOperationException("the endpoint refused the connection");

        public Task PutAsync<T>(string id, T document) => throw new NotSupportedException();
        public Task EnsureDatabaseExistsAsync(string databaseName) =>
            throw new InvalidOperationException("the endpoint refused the connection");
        public Task<long> GetDocumentCountAsync(string idPrefix) => throw new NotSupportedException();
        public Task<string> GetServerVersionAsync() => throw new NotSupportedException();
        public void Dispose() { }
    }
}
