using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using RavenBench.Core.Transport;
using RavenBench.Core.Workload;
using RavenBench.Dataset.Vectors;
using Xunit;

namespace RavenBench.Tests.Transport;

public class PgVectorTransportMappingTests
{
    private const string Unreachable = "postgresql://bench:bench@127.0.0.1:9/bench";

    [Theory]
    [InlineData(VectorMetric.Cosine, "vector_cosine_ops", "<=>")]
    [InlineData(VectorMetric.L2, "vector_l2_ops", "<->")]
    [InlineData(VectorMetric.Dot, "vector_ip_ops", "<#>")]
    public void Each_Metric_Maps_To_Its_Operator_Class_And_Its_Query_Operator(VectorMetric metric, string operatorClass, string op)
    {
        Assert.Equal(operatorClass, PgVectorMetrics.OperatorClass(metric));
        Assert.Contains($"embedding {op} $1", PgVectorTransport.SearchSql(metric, null));
    }

    [Fact]
    public void A_Metric_Without_A_Mapping_Is_Refused_By_Name_Before_Any_Connection()
    {
        var ex = Assert.Throws<UnsupportedVectorMetricException>(() => new PgVectorTransport(Unreachable, "bench", 1, (VectorMetric)99, 3));
        Assert.Equal(PgVectorTransport.Target, ex.Target);
    }

    [Fact]
    public void The_Filter_Value_Is_A_Bound_Parameter_Never_Sql_Text()
    {
        var sql = PgVectorTransport.SearchSql(VectorMetric.Cosine, new VectorFilter("label", "o'neil"));
        Assert.Contains("WHERE label = $3", sql);
        Assert.DoesNotContain("neil", sql);
    }

    [Fact]
    public async Task PgVector_Refuses_The_RavenDB_Knob_And_RavenDB_Refuses_The_PgVector_Knob()
    {
        using var transport = new PgVectorTransport(Unreachable, "bench", 1, VectorMetric.Cosine, 3);
        var result = await transport.ExecuteAsync(new VectorSearchOperation
        {
            QueryVector = [1f, 0f, 0f],
            FieldName = "embedding",
            Effort = SearchEffort.RavenDb(16)
        }, CancellationToken.None);
        Assert.False(result.IsSuccess);
        Assert.Contains(SearchEffort.RavenDbKnob, result.ErrorDetails);

        var raven = new VectorSearchOperation { QueryVector = [1f], FieldName = "Vector", Effort = SearchEffort.PgVector(40) };
        var ex = Assert.Throws<NotSupportedException>(() => raven.ToRqlQuery());
        Assert.Contains(SearchEffort.PgVectorKnob, ex.Message);
    }

    [Fact]
    public void A_Plan_That_Uses_The_Hnsw_Index_Fails_The_Exact_Search_By_Index_Name()
    {
        var plan = new[] { "Limit", "  ->  Index Scan using vectors_embedding_hnsw on vectors", "        Order By: (embedding <=> $1)" };
        var ex = Assert.Throws<ExactSearchUsedIndexException>(() => PgVectorTransport.RequireNoIndex(plan, PgVectorTransport.IndexName));
        Assert.Equal(PgVectorTransport.IndexName, ex.IndexName);

        PgVectorTransport.RequireNoIndex(["Limit", "  ->  Sort", "        ->  Seq Scan on vectors"], PgVectorTransport.IndexName);
    }
}
