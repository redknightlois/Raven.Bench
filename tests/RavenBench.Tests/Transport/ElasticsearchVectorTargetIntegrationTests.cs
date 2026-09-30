using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using RavenBench.Core.Transport;
using RavenBench.Core.Workload;
using RavenBench.Tests.Infrastructure;
using RavenBench.VectorBench;
using Xunit;

namespace RavenBench.Tests.Transport;

/// <summary>
/// State-based tests for the Elasticsearch target against live clusters. Each test loads its own index
/// and deletes it when it finishes.
/// </summary>
[Collection(LiveServers.Name)]
public class ElasticsearchVectorTargetIntegrationTests
{
    // The server defaults a field to bbq_hnsw from 384 dimensions up, so the probe has a BBQ default to find.
    private const int Dimensions = 384;
    private const int K = 5;

    [RequiresElasticsearchFact("trial")]
    public Task A_Load_Creates_A_Fresh_One_Shard_Index_With_Request_Durability_That_Answers_At_Once() => WithTarget("hnsw", async (target, transport) =>
    {
        var set = Set(300, seed: 1);
        await target.LoadAsync(set.ToAsyncEnumerable(), CancellationToken.None);

        var settings = await transport.ReadIndexSettingsAsync(CancellationToken.None);
        settings["index.number_of_shards"].Should().Be("1");
        settings["index.number_of_replicas"].Should().Be("0");
        settings["index.translog.durability"].Should().Be("request");
        foreach (var v in set.Take(20))
            (await Search(transport, v.Vector, K)).Should().HaveCount(K).And.StartWith(v.Id, "a vector copied from the set finds its own dataset id first");

        await target.CleanupAsync();
        var second = Set(100, seed: 2).Select(v => v with { Id = "b" + v.Id }).ToList();
        await target.LoadAsync(second.ToAsyncEnumerable(), CancellationToken.None);
        (await transport.GetDocumentCountAsync("")).Should().Be(second.Count, "the second load starts from a fresh index");
    });

    [RequiresElasticsearchFact("trial")]
    public Task A_Filtered_Search_Returns_Only_Labelled_Ids_And_K_Of_Them() => WithTarget("bbq_hnsw", async (target, transport) =>
    {
        var set = Set(400, seed: 3);
        await target.LoadAsync(set.ToAsyncEnumerable(), CancellationToken.None);
        var labelled = set.Where(v => v.Label == "in").Select(v => v.Id).ToHashSet();

        foreach (var query in Set(10, seed: 4))
        {
            var result = await transport.ExecuteAsync(new VectorSearchOperation
            {
                QueryVector = query.Vector, FieldName = target.FieldName, TopK = K, Filter = new VectorFilter(target.FilterField, "in"), Effort = target.Effort(50)
            }, CancellationToken.None);
            result.IsSuccess.Should().BeTrue(result.ErrorDetails);
            result.NeighborIds.Should().HaveCount(K).And.OnlyContain(id => labelled.Contains(id));
        }
    });

    [RequiresElasticsearchFact("trial")]
    public Task An_Inserted_Document_Is_Found_After_A_Refresh() => WithTarget("hnsw", async (target, transport) =>
    {
        await target.LoadAsync(Set(100, seed: 5).ToAsyncEnumerable(), CancellationToken.None);
        var inserted = Set(1, seed: 6).Single() with { Id = "inserted" };
        (await transport.ExecuteAsync(target.InsertOperation(inserted), CancellationToken.None)).IsSuccess.Should().BeTrue();
        await transport.RefreshAsync(CancellationToken.None);
        (await Search(transport, inserted.Vector, 1)).Should().Equal("inserted");
        target.InsertVisibility.Should().Contain("index.refresh_interval is 1s", "the image default refresh interval as the server reports it");
    });

    [RequiresElasticsearchFact("trial")]
    public Task Bbq_Hnsw_Records_The_Server_Options_The_Probed_Default_Kind_And_The_Licence() => RecordsSettings("bbq_hnsw");

    [RequiresElasticsearchFact("trial")]
    public Task Bbq_Disk_Loads_Under_The_Trial_And_Records_Its_Options() => RecordsSettings("bbq_disk");

    [RequiresElasticsearchFact("basic")]
    public async Task Bbq_Disk_Under_The_Basic_Licence_Is_Refused_By_Name_Before_Any_Write()
    {
        var index = "c1-it-" + Guid.NewGuid().ToString("N")[..8];
        using var transport = new ElasticsearchVectorTransport(ElasticsearchTestEndpoints.Basic.ToString(), index, VectorMetric.Cosine, Dimensions, "bbq_disk");
        using var target = new ElasticsearchVectorTarget(transport);
        var enumerated = false;
        async IAsyncEnumerable<LabelledVector> Vectors()
        {
            enumerated = true;
            await Task.CompletedTask;
            yield return Set(1, seed: 7).Single();
        }

        var act = () => target.LoadAsync(Vectors(), CancellationToken.None);
        (await act.Should().ThrowAsync<ElasticsearchLicenceException>()).WithMessage("*'basic'*xpack.license.self_generated.type=trial*");
        enumerated.Should().BeFalse("no vector is read, so no _bulk request is sent");
        var exists = () => transport.GetDocumentCountAsync("");
        await exists.Should().ThrowAsync<ElasticsearchRequestException>().WithMessage("*404*", "no index was created");
    }

    private Task RecordsSettings(string kind) => WithTarget(kind, async (target, transport) =>
    {
        await target.LoadAsync(Set(300, seed: 8).ToAsyncEnumerable(), CancellationToken.None);
        var settings = await target.ReportedSettingsAsync(CancellationToken.None);

        settings["index_kind.sent"].Should().Be(kind);
        settings["index_kind.vendor_default"].Should().Be("bbq_hnsw", "the 9.5 server resolves an unset cosine field of this width to bbq_hnsw");
        settings["mapping.index_options.type"].Should().Be(kind);
        settings["mapping.index_options.type.source"].Should().Be(BuildSetting.SetByBenchmark);
        var defaults = settings.Keys.Where(k => k.StartsWith("mapping.index_options.") && k.EndsWith(".source") && k != "mapping.index_options.type.source").ToList();
        defaults.Should().NotBeEmpty("the server reports the options it defaulted");
        defaults.Should().OnlyContain(k => settings[k].StartsWith("default: "));
        settings["index.translog.durability"].Should().Be("request");
        settings["index.refresh_interval"].Should().NotBeNullOrEmpty();
        long.Parse(settings["segments.count"]).Should().BePositive();
        long.Parse(settings["jvm.mem.heap_max_in_bytes"]).Should().BePositive();
        settings["license.type"].Should().Be("trial");
        settings["license.status"].Should().Be("active");
        settings["license.expiry_date"].Should().NotBe("none");
        var size = await target.StoredSizeAsync();
        size.Bytes.Should().BePositive();
        size.Metric.Should().Contain("store.size_in_bytes");
    });

    private static async Task<IReadOnlyList<string>> Search(ElasticsearchVectorTransport transport, float[] vector, int k)
    {
        var result = await transport.ExecuteAsync(new VectorSearchOperation
        {
            QueryVector = vector, FieldName = ElasticsearchVectorTransport.VectorField, TopK = k, Effort = new SearchEffort(transport.Kind.Knob, transport.Kind.Knob == ElasticsearchIndexKind.VisitKnob ? 100 : 200)
        }, CancellationToken.None);
        result.IsSuccess.Should().BeTrue(result.ErrorDetails);
        return result.NeighborIds!;
    }

    private static async Task WithTarget(string kind, Func<ElasticsearchVectorTarget, ElasticsearchVectorTransport, Task> body)
    {
        var index = "c1-it-" + Guid.NewGuid().ToString("N")[..8];
        using var transport = new ElasticsearchVectorTransport(ElasticsearchTestEndpoints.Trial.ToString(), index, VectorMetric.Cosine, Dimensions, kind);
        using var target = new ElasticsearchVectorTarget(transport);
        try
        {
            await body(target, transport);
        }
        finally
        {
            await target.CleanupAsync();
        }
    }

    private static List<LabelledVector> Set(int count, int seed)
    {
        var random = new Random(seed);
        return Enumerable.Range(0, count).Select(i => new LabelledVector(
            i.ToString(), Enumerable.Range(0, Dimensions).Select(_ => (float)(random.NextDouble() * 2 - 1)).ToArray(), i % 3 == 0 ? "in" : "out")).ToList();
    }
}
