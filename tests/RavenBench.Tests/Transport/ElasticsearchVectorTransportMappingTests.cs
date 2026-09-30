using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using FluentAssertions;
using RavenBench.Core.Transport;
using RavenBench.Core.Vector;
using RavenBench.Core.Workload;
using RavenBench.Core.Diagnostics;
using RavenBench.VectorBench;
using Xunit;

namespace RavenBench.Tests.Transport;

public class ElasticsearchVectorTransportMappingTests
{
    private static ElasticsearchIndexKind Kind(string name) => ElasticsearchIndexKind.Named(name);

    [Theory]
    [InlineData(VectorMetric.Cosine, "cosine")]
    [InlineData(VectorMetric.L2, "l2_norm")]
    [InlineData(VectorMetric.Dot, "max_inner_product")]
    public void The_Mapping_Similarity_Matches_The_Set_Metric(VectorMetric metric, string similarity)
    {
        using var body = JsonDocument.Parse(ElasticsearchVectorTransport.IndexBody(metric, 8, Kind("hnsw")));
        body.RootElement.GetProperty("mappings").GetProperty("properties").GetProperty("embedding").GetProperty("similarity").GetString().Should().Be(similarity);
    }

    [Theory]
    [InlineData("hnsw")]
    [InlineData("bbq_hnsw")]
    [InlineData("bbq_disk")]
    public void The_Mapping_Sends_Only_The_Kind_With_One_Shard_No_Replicas_And_Request_Durability(string kind)
    {
        using var body = JsonDocument.Parse(ElasticsearchVectorTransport.IndexBody(VectorMetric.Cosine, 8, Kind(kind)));
        var options = body.RootElement.GetProperty("mappings").GetProperty("properties").GetProperty("embedding").GetProperty("index_options");
        options.EnumerateObject().Select(p => p.Name).Should().Equal("type");
        options.GetProperty("type").GetString().Should().Be(kind);
        var settings = body.RootElement.GetProperty("settings");
        settings.GetProperty("number_of_shards").GetInt32().Should().Be(1);
        settings.GetProperty("number_of_replicas").GetInt32().Should().Be(0);
        settings.GetProperty("index.translog.durability").GetString().Should().Be("request");
    }

    [Fact]
    public void The_Default_Probe_Mapping_Carries_No_Index_Options()
    {
        using var body = JsonDocument.Parse(ElasticsearchVectorTransport.IndexBody(VectorMetric.Cosine, 8, kind: null));
        body.RootElement.GetProperty("mappings").GetProperty("properties").GetProperty("embedding").TryGetProperty("index_options", out _).Should().BeFalse();
    }

    [Fact]
    public void An_Unknown_Kind_Or_Metric_Is_Refused_By_Name_Before_Any_Request()
    {
        // Port 1 answers nothing, so a request would fail with a connection error rather than these.
        FluentActions.Invoking(() => new ElasticsearchVectorTransport("http://localhost:1", "i", VectorMetric.Cosine, 8, "int8_hnsw"))
            .Should().Throw<ArgumentException>().WithMessage("*int8_hnsw*");
        FluentActions.Invoking(() => new ElasticsearchVectorTransport("http://localhost:1", "i", (VectorMetric)99, 8, "hnsw"))
            .Should().Throw<UnsupportedVectorMetricException>().Where(e => e.Metric == (VectorMetric)99);
    }

    [Theory]
    [InlineData("hnsw", "num_candidates")]
    [InlineData("bbq_hnsw", "num_candidates")]
    [InlineData("bbq_disk", "visit_percentage")]
    public void Each_Effort_Setting_Changes_Only_The_Kinds_Own_Knob_And_Returns_Ids_Only(string kind, string knob)
    {
        var bodies = new[] { 1, 15, 150 }.Select(v => JsonDocument.Parse(ElasticsearchVectorTransport.SearchBody(Search(new SearchEffort(knob, v)), Kind(kind)))).ToList();
        foreach (var (body, value) in bodies.Zip(new[] { 1, 15, 150 }))
        {
            body.RootElement.GetProperty("_source").GetBoolean().Should().BeFalse();
            var knn = body.RootElement.GetProperty("knn");
            knn.GetProperty(knob).GetInt32().Should().Be(value);
            knn.EnumerateObject().Select(p => p.Name).Should().BeEquivalentTo("field", "query_vector", "k", knob);
        }
        FluentActions.Invoking(() => ElasticsearchVectorTransport.SearchBody(Search(SearchEffort.RavenDb(10)), Kind(kind)))
            .Should().Throw<NotSupportedException>().WithMessage($"*numberOfCandidates*{knob}*");
    }

    [Fact]
    public void Visit_Percentage_Takes_A_Fraction_And_Num_Candidates_Refuses_One()
    {
        using var body = JsonDocument.Parse(ElasticsearchVectorTransport.SearchBody(Search(new SearchEffort("visit_percentage", 0.5)), Kind("bbq_disk")));
        body.RootElement.GetProperty("knn").GetProperty("visit_percentage").GetDouble().Should().Be(0.5);
        FluentActions.Invoking(() => ElasticsearchVectorTransport.SearchBody(Search(new SearchEffort("num_candidates", 0.5)), Kind("hnsw")))
            .Should().Throw<NotSupportedException>().WithMessage("*num_candidates*integer*0.5*");
    }

    [Fact]
    public void The_Label_Filter_Sits_Inside_The_Knn_Clause()
    {
        var search = new VectorSearchOperation { QueryVector = [1f, 0f], FieldName = "embedding", TopK = 10, Filter = new VectorFilter("label", "in") };
        using var body = JsonDocument.Parse(ElasticsearchVectorTransport.SearchBody(search, Kind("hnsw")));
        body.RootElement.GetProperty("knn").GetProperty("filter").GetProperty("term").GetProperty("label").GetString().Should().Be("in");
        body.RootElement.TryGetProperty("query", out _).Should().BeFalse("a top-level query would filter after the search");
        body.RootElement.TryGetProperty("post_filter", out _).Should().BeFalse();
    }

    [Fact]
    public void A_Search_Response_Parses_To_Its_Ids_In_Order()
    {
        ElasticsearchVectorTransport.SearchIds("""{"hits":{"hits":[{"_id":"7"},{"_id":"3"}]}}""").Should().Equal("7", "3");
        ElasticsearchVectorTransport.SearchIds("{}").Should().BeEmpty();
    }

    [Fact]
    public void A_Bulk_Body_Is_Ndjson_Pairs_Of_Action_And_Document()
    {
        var body = ElasticsearchVectorTransport.BulkBody([new() { Id = "a", Document = new VectorRow([0.5f, -1f], "in") }]);
        var lines = body.Split('\n', StringSplitOptions.RemoveEmptyEntries);
        lines.Should().HaveCount(2);
        JsonDocument.Parse(lines[0]).RootElement.GetProperty("index").GetProperty("_id").GetString().Should().Be("a");
        var document = JsonDocument.Parse(lines[1]).RootElement;
        document.GetProperty("embedding").EnumerateArray().Select(e => e.GetSingle()).Should().Equal(0.5f, -1f);
        document.GetProperty("label").GetString().Should().Be("in");
        body.Should().EndWith("\n");
    }

    [Fact]
    public void A_Bulk_Response_With_Errors_Throws_Naming_The_Failed_Item_And_Its_Reason()
    {
        const string response = """{"errors":true,"items":[{"index":{"_id":"1","status":201}},{"index":{"_id":"2","status":403,"error":{"type":"security_exception","reason":"current license is non-compliant for [bbq_disk]"}}}]}""";
        FluentActions.Invoking(() => ElasticsearchVectorTransport.RequireBulkSuccess(response))
            .Should().Throw<ElasticsearchBulkException>().WithMessage("*1 of 2*'2'*non-compliant for [bbq_disk]*");
        ElasticsearchVectorTransport.RequireBulkSuccess("""{"errors":false,"items":[]}""");
    }

    [Theory]
    [InlineData("basic", "active", false)]
    [InlineData("trial", "active", true)]
    [InlineData("enterprise", "active", true)]
    [InlineData("trial", "expired", false)]
    public void Only_An_Active_Trial_Or_Enterprise_Licence_Allows_Bbq_Disk(string type, string status, bool allowed)
    {
        var licence = new ElasticsearchLicence(type, status, "none");
        licence.Allows(Kind("bbq_disk")).Should().Be(allowed);
        licence.Allows(Kind("bbq_hnsw")).Should().BeTrue();
        new ElasticsearchLicenceException("bbq_disk", licence).Message.Should().Contain($"'{type}'").And.Contain("xpack.license.self_generated.type=trial");
    }

    [Fact]
    public void The_Server_Options_Flatten_To_Dotted_Keys()
    {
        using var mapping = JsonDocument.Parse("""{"type":"dense_vector","index_options":{"type":"bbq_hnsw","m":16,"rescore_vector":{"oversample":3.0}}}""");
        var flat = new Dictionary<string, string>();
        ElasticsearchVectorTransport.Flatten(mapping.RootElement, "", flat);
        flat.Should().Contain("index_options.type", "bbq_hnsw").And.Contain("index_options.m", "16").And.Contain("index_options.rescore_vector.oversample", "3.0");
    }

    [Fact]
    public void Every_Row_Label_Names_Its_Quantization_And_No_Two_Configurations_Share_One()
    {
        var scenario = VectorScenario.Load(System.IO.Path.Combine(RepositoryRootLocator.Find(), "benchmarks", "vector", "scenario.json"));
        var labels = new List<string>();
        foreach (var type in VectorScenario.RavenDbEmbeddingTypes)
            using (var t = VectorRunner.BuildTarget("ravendb", "http://localhost:1", "x", VectorMetric.Cosine, 8, 1, scenario with { RavenDbEmbeddingType = type }))
                labels.Add("ravendb " + t.VectorStorage);
        foreach (var kind in ElasticsearchIndexKind.All)
            using (var t = VectorRunner.BuildTarget("elasticsearch", "http://localhost:1", "x", VectorMetric.Cosine, 8, 1, scenario with { ElasticsearchIndexKind = kind.Name }))
                labels.Add("elasticsearch " + t.VectorStorage);
        labels.Add("pgvector float32, unquantized");

        labels.Should().OnlyHaveUniqueItems().And.OnlyContain(l => l.Contains("unquantized") || l.Contains(", quantized"));
        labels.Should().Contain("ravendb float32, unquantized", "the Single row keeps its storage label");
    }

    private static VectorSearchOperation Search(SearchEffort effort) => new() { QueryVector = [1f, 0f], FieldName = "embedding", TopK = 10, Effort = effort };
}
