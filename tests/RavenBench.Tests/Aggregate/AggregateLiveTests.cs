using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using MongoDB.Bson;
using RavenBench.Aggregate;
using RavenBench.Core.Aggregate;
using RavenBench.Core.Transport;
using RavenBench.Core.Workload;
using RavenBench.Tests.Infrastructure;
using Xunit;
using static RavenBench.Tests.Aggregate.AggregateUnitTests;

namespace RavenBench.Tests.Aggregate;

/// <summary>
/// The edge contracts the products disagree on by default, shown on RavenDB (8081) and MongoDB
/// (27017) over one hand-built set. Each test uses its own database and deletes it.
/// </summary>
[Collection(LiveServers.Name)]
public class AggregateLiveTests
{
    private const string RavenUrl = "http://localhost:8081";

    // Category "B" and "a" tie at 2 and differ in case: ordinal puts "B" first, culture order puts "a" first.
    // Region counts: r10 has 10 documents, r09 has 9, so a string sort of the value would put r09 first.
    // Region sums: r01 and r02 tie at 50 at the cut of a top 2 behind r03.
    private static readonly AggregateDocument[] EdgeSet = BuildEdgeSet();

    private static AggregateDocument[] BuildEdgeSet()
    {
        var docs = new List<AggregateDocument>
        {
            Doc("a1", "a", "r01", 25), Doc("a2", "a", "r01", 25),
            Doc("b1", "B", "r02", 20), Doc("b2", "B", "r02", 30),
            Doc("c1", "c", "r03", 60)
        };
        for (int i = 0; i < 10; i++)
            docs.Add(Doc("x" + i, "x", "r10", 0));
        for (int i = 0; i < 9; i++)
            docs.Add(Doc("y" + i, "y", "r09", 0));
        return docs.ToArray();
    }

    private static IEnumerable<GroupedAggregateOperation> EdgeOperations()
    {
        yield return new() { GroupBy = "category", Kind = AggregateKind.Count, TopN = 10 };
        yield return new() { GroupBy = "category", Kind = AggregateKind.Count, TopN = 3 };
        yield return new() { GroupBy = "category", Kind = AggregateKind.Count, TopN = 1 };
        yield return new() { GroupBy = "region", Kind = AggregateKind.Sum, SumField = "amount", TopN = 2 };
        yield return new() { GroupBy = "region", Kind = AggregateKind.Sum, SumField = "amount", TopN = 1_000 };
        yield return new() { GroupBy = "region", Kind = AggregateKind.Sum, SumField = "amount", TopN = 100, Filter = new EqualityFilter("category", "B") };
        yield return new() { GroupBy = "region", Kind = AggregateKind.Sum, SumField = "amount", TopN = 100, Filter = new EqualityFilter("category", "missing") };
        yield return new() { GroupBy = "category", Kind = AggregateKind.Count, TopN = 100, Filter = new RangeFilter("category", "c", "y") };
    }

    [RequiresRavenDbFact(8081)]
    public async Task RavenDb_Holds_The_Edge_Contracts()
    {
        var product = AggregateParityCheck.RavenDb(RavenUrl, "agg_it_" + Guid.NewGuid().ToString("N")[..8]);
        await AssertEdgeContracts(product);
    }

    [RequiresMongoFact]
    public async Task Mongo_Holds_The_Edge_Contracts_On_Both_Targets()
    {
        var database = "agg_it_" + Guid.NewGuid().ToString("N")[..8];
        foreach (var target in new[] { MongoYcsbTransport.MongoDbTarget, MongoYcsbTransport.MongoDbIndexedTarget })
        {
            using var transport = new MongoYcsbTransport(MongoTestEndpoints.MongoConnectionString, database, target);
            await AssertEdgeContracts(AggregateParityCheck.Mongo(transport));
        }
    }

    private static async Task AssertEdgeContracts(AggregateParityProduct product)
    {
        try
        {
            await product.LoadAsync(EdgeSet, CancellationToken.None);
            foreach (var op in EdgeOperations())
            {
                var result = await product.QueryAsync(op, CancellationToken.None);
                result.IsSuccess.Should().BeTrue(result.ErrorDetails);
                result.IsStale.Should().BeFalse("RavenDB answers after its indexes report non-stale, and MongoDB never marks a result stale");
                result.Groups.Should().Equal(AggregateOrdering.Compute(op, EdgeSet), $"{product.Name} serves {op.IndexName} top {op.TopN} with filter {op.Filter}");
                result.ResultCount.Should().Be(result.Groups!.Count);
            }

            // Spelled out once so a wrong shared expectation cannot hide a wrong product answer.
            var ties = await product.QueryAsync(new() { GroupBy = "category", Kind = AggregateKind.Count, TopN = 4 }, CancellationToken.None);
            ties.Groups.Should().Equal(new("x", 10), new("y", 9), new("B", 2), new("a", 2));
            var range = await product.QueryAsync(new() { GroupBy = "category", Kind = AggregateKind.Count, TopN = 10, Filter = new RangeFilter("category", "c", "y") }, CancellationToken.None);
            range.Groups!.Select(g => g.Key).Should().Equal("x", "c");
        }
        finally
        {
            await product.CleanupAsync();
        }
    }

    [RequiresMongoFact]
    public async Task Only_The_Indexed_Target_Creates_The_Repository_Indexes_And_A_Second_Load_Succeeds()
    {
        var database = "agg_it_" + Guid.NewGuid().ToString("N")[..8];
        using var plain = new MongoYcsbTransport(MongoTestEndpoints.MongoConnectionString, database, MongoYcsbTransport.MongoDbTarget);
        using var indexed = new MongoYcsbTransport(MongoTestEndpoints.MongoConnectionString, database, MongoYcsbTransport.MongoDbIndexedTarget);
        try
        {
            await plain.EnsureAggregateIndexesAsync(CancellationToken.None);
            (await plain.ListAggregateIndexNamesAsync(CancellationToken.None)).Should().Equal("_id_");

            await indexed.EnsureAggregateIndexesAsync(CancellationToken.None);
            await indexed.EnsureAggregateIndexesAsync(CancellationToken.None);
            var expected = AggregateShapes.MongoIndexes().Select(i => i.GetProperty("name").GetString()).Append("_id_");
            (await indexed.ListAggregateIndexNamesAsync(CancellationToken.None)).Should().BeEquivalentTo(expected);
        }
        finally
        {
            await plain.DropAggregateCollectionAsync(CancellationToken.None);
        }
    }

    [RequiresMongoFact]
    public async Task Parity_Fails_With_The_First_Difference_When_A_Product_Holds_An_Edited_Document()
    {
        var database = "agg_it_" + Guid.NewGuid().ToString("N")[..8];
        using var reference = new MongoYcsbTransport(MongoTestEndpoints.MongoConnectionString, database, MongoYcsbTransport.MongoDbTarget);
        using var edited = new MongoYcsbTransport(MongoTestEndpoints.MongoConnectionString, database, MongoYcsbTransport.MongoDbIndexedTarget);
        var healthy = AggregateParityCheck.Mongo(edited);
        var tampered = healthy with
        {
            LoadAsync = async (sample, ct) =>
            {
                await healthy.LoadAsync(sample, ct);
                var hottest = AggregateDataSet.CategoryKey(1, AggregateParityCheck.CategoryCardinality);
                var victim = sample.First(d => d.Category == hottest);
                await edited.Documents.Database.GetCollection<BsonDocument>(AggregateDocument.MongoCollection)
                    .UpdateOneAsync(new BsonDocument("_id", victim.Id), new BsonDocument("$set", new BsonDocument("amount", new BsonInt64(victim.Amount + 1_000_000))), cancellationToken: ct);
            }
        };

        var report = await new AggregateParityCheck(42, 300).RunAsync([AggregateParityCheck.Mongo(reference), tampered], CancellationToken.None);

        report.ExitCode.Should().Be(1);
        report.Pairs.Where(p => p.Product == MongoYcsbTransport.MongoDbTarget).Should().OnlyContain(p => p.Agreed);
        var failed = report.Pairs.Where(p => p.Agreed == false).ToList();
        failed.Should().NotBeEmpty();
        failed.Should().OnlyContain(p => p.Product == MongoYcsbTransport.MongoDbIndexedTarget && p.FirstDifference!.StartsWith($"{MongoYcsbTransport.MongoDbIndexedTarget} {p.Shape} position "));
        (await edited.ListAggregateIndexNamesAsync(CancellationToken.None)).Should().BeEmpty("the check dropped what it wrote");
    }
}
