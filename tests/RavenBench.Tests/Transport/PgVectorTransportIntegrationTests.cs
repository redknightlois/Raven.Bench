using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Apex.PgClient;
using RavenBench.Core.Transport;
using RavenBench.Core.Workload;
using RavenBench.Dataset.Vectors;
using RavenBench.Tests.Infrastructure;
using Xunit;

namespace RavenBench.Tests.Transport;

/// <summary>
/// State-based tests for the pgvector transport against the live container. Each test works in its
/// own schema, with <c>public</c> behind it so the extension's type and operators resolve, and drops
/// the schema when it finishes.
/// </summary>
public class PgVectorTransportIntegrationTests
{
    private const int Dimensions = 8;
    private const int K = 5;

    [RequiresPostgreSqlFact]
    public Task The_Server_Reads_The_Codec_Bytes_As_The_Vector_Written() => WithTransport(1, async transport =>
    {
        float[] vector = [1f, -2.5f, 0f, 0.125f, -0f, 3f, 1e-3f, -7f];
        await transport.PutAsync("v/1", new VectorRow(vector, null));

        var (dims, text, back) = await transport.ReadWithWorkerAsync(async (connection, vectorType) =>
        {
            // The typed path asks for binary results, so the embedding column arrives through the codec.
            var rows = await connection.QueryTypedAsync("SELECT vector_dims(embedding)::int8, embedding::text, embedding FROM vectors WHERE id = 'v/1'", PgParameters.Empty, CancellationToken.None);
            return (rows[0].Get<long>(0), rows[0].Get<string>(1), rows[0].Get<PgVector>(2).Values);
        });

        Assert.Equal(vector.Length, dims);
        Assert.Equal("[1,-2.5,0,0.125,-0,3,0.001,-7]", text);
        Assert.Equal(vector, back);
    });

    [RequiresPostgreSqlFact]
    public Task Search_With_And_Without_A_Filter_Returns_The_Brute_Force_Truth() => WithLoadedSet(async (transport, set) =>
    {
        var queries = Seeded(20, seed: 7);
        var truth = await BruteForceTruth.ComputeAsync(queries, set.ToAsyncEnumerable(), VectorMetric.Cosine, K);
        var even = set.Where(v => int.Parse(v.Id) % 2 == 0).ToList();
        var filteredTruth = await BruteForceTruth.ComputeAsync(queries, even.ToAsyncEnumerable(), VectorMetric.Cosine, K);

        // ef_search far above the set size makes the HNSW search visit the whole graph.
        var effort = SearchEffort.PgVector(400);
        for (int q = 0; q < queries.Length; q++)
        {
            var plain = await SearchAsync(transport, queries[q], effort, null);
            Assert.Equal(truth[q], plain);

            var filtered = await SearchAsync(transport, queries[q], effort, new VectorFilter("label", "even"));
            Assert.Equal(filteredTruth[q], filtered);
        }
    });

    [RequiresPostgreSqlFact]
    public Task A_Filter_Value_With_A_Quote_Returns_Its_Rows() => WithTransport(1, async transport =>
    {
        await transport.PutAsync("a", new VectorRow(Seeded(1, 1)[0], "o'neil"));
        await transport.PutAsync("b", new VectorRow(Seeded(1, 2)[0], "other"));

        var ids = await SearchAsync(transport, Seeded(1, 3)[0], null, new VectorFilter("label", "o'neil"));
        Assert.Equal(new[] { "a" }, ids);
    });

    [RequiresPostgreSqlFact]
    public Task Each_Query_On_One_Worker_Runs_Under_Its_Own_Effort() => WithTransport(1, async transport =>
    {
        await transport.PutAsync("a", new VectorRow(Seeded(1, 1)[0], null));
        var serverValue = await ShowEffortAsync(transport);

        foreach (var value in new[] { 17, 93 })
        {
            var result = await transport.ExecuteAsync(Search(Seeded(1, 2)[0], SearchEffort.PgVector(value), null), CancellationToken.None);
            Assert.True(result.IsSuccess, result.ErrorDetails);
            Assert.Equal(SearchEffort.PgVector(value), result.EffortInForce);
            Assert.Equal(value.ToString(), await ShowEffortAsync(transport));
        }

        // No effort leaves the server value in force, and the result records what the server reports.
        var unset = await transport.ExecuteAsync(Search(Seeded(1, 2)[0], null, null), CancellationToken.None);
        Assert.True(unset.IsSuccess, unset.ErrorDetails);
        Assert.Equal(serverValue, await ShowEffortAsync(transport));
        Assert.Equal(SearchEffort.PgVector(int.Parse(serverValue)), unset.EffortInForce);
    });

    [RequiresPostgreSqlFact]
    public Task A_Query_After_A_Cancelled_One_Runs_Under_Its_Own_Effort() => WithTransport(1, async transport =>
    {
        await transport.PutAsync("a", new VectorRow(Seeded(1, 1)[0], null));
        var first = await transport.ExecuteAsync(Search(Seeded(1, 2)[0], SearchEffort.PgVector(17), null), CancellationToken.None);
        Assert.True(first.IsSuccess, first.ErrorDetails);

        // The cancel lands mid-statement on a session whose effort no longer matches what the slot applied.
        using var cancel = new CancellationTokenSource(TimeSpan.FromMilliseconds(300));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => transport.ReadWithWorkerAsync(async (connection, _) =>
        {
            await connection.ExecuteAsync("SELECT set_config('hnsw.ef_search', '93', false)", CancellationToken.None);
            return await connection.ExecuteAsync("SELECT pg_sleep(10)", cancel.Token);
        }));

        var next = await transport.ExecuteAsync(Search(Seeded(1, 2)[0], SearchEffort.PgVector(17), null), CancellationToken.None);
        Assert.True(next.IsSuccess, next.ErrorDetails);
        Assert.Equal("17", await ShowEffortAsync(transport));
    });

    [RequiresPostgreSqlFact]
    public Task The_Index_Is_Built_At_Vendor_Defaults_And_The_Server_Settings_Are_Recorded() => WithLoadedSet(async (transport, _) =>
    {
        var settings = await transport.ReadServerSettingsAsync();

        Assert.StartsWith("0.8", settings.ExtensionVersion);
        Assert.Contains("USING hnsw (embedding vector_cosine_ops)", settings.IndexDefinition);
        Assert.DoesNotContain("WITH", settings.IndexDefinition);
        Assert.Empty(settings.IndexOptions);
        Assert.Equal("on", settings.ServerSettings[PostgresYcsbTransport.DurabilitySetting]);
        Assert.All(PgVectorTransport.RecordedSettings, name => Assert.False(string.IsNullOrEmpty(settings.ServerSettings[name]), name));

        var workerDurability = await transport.ReadWithWorkerAsync(async (connection, _) =>
            (await connection.QueryAsync(PostgresYcsbTransport.ReadDurabilitySql, CancellationToken.None))[0].Get<string>(0));
        Assert.Equal("on", workerDurability);
    });

    [RequiresPostgreSqlFact]
    public Task Exact_Search_Skips_The_Index_And_Returns_The_Brute_Force_Truth() => WithLoadedSet(async (transport, set) =>
    {
        var queries = Seeded(10, seed: 11);
        var truth = await BruteForceTruth.ComputeAsync(queries, set.ToAsyncEnumerable(), VectorMetric.Cosine, K);
        for (int q = 0; q < queries.Length; q++)
            Assert.Equal(truth[q], await transport.ExactSearchAsync(queries[q], K, null));
    });

    [RequiresPostgreSqlFact]
    public async Task The_Vector_Parity_Check_Passes_For_PgVector()
    {
        await using var schema = await PgTestSchema.CreateAsync();
        using var transport = new PgVectorTransport(schema.ConnectionString + ",public", PostgreSqlTestEndpoints.Database, 1, VectorParityCheck.Metric, 16);

        var report = await new VectorParityCheck(seed: 5, baseCount: 200, queryCount: 20, dimensions: 16, k: 10)
            .RunAsync([VectorParityCheck.PgVector(transport)], CancellationToken.None);

        var result = Assert.Single(report.Results);
        Assert.True(result.Agreed, result.Failure ?? string.Join(",", result.Mismatches.Select(m => m.Query)));
        Assert.Equal(20, result.Compared);
        Assert.Equal(0, await transport.ReadWithWorkerAsync(async (connection, _) =>
            (await connection.QueryAsync("SELECT count(*)::int8 FROM pg_tables WHERE tablename = 'vectors' AND schemaname = current_schema()", CancellationToken.None))[0].Get<long>(0)));
    }

    [RequiresPostgreSqlFact]
    public Task The_Settings_Read_Only_The_Index_In_The_Transports_Own_Schema() => WithLoadedSet(async (_, _) =>
    {
        await WithTransport(1, async unindexed =>
        {
            var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => unindexed.ReadServerSettingsAsync());
            Assert.Contains(PgVectorTransport.IndexName, ex.Message);
        });
    });

    private static async Task WithLoadedSet(Func<PgVectorTransport, List<BaseVector>, Task> body)
    {
        await WithTransport(4, async transport =>
        {
            var set = Seeded(300, seed: 3).Select((v, i) => new BaseVector(i.ToString(), v)).ToList();
            var load = await transport.ExecuteAsync(new BulkInsertOperation<VectorRow>
            {
                Documents = set.Select(v => new DocumentToWrite<VectorRow>
                {
                    Id = v.Id,
                    Document = new VectorRow(v.Vector, int.Parse(v.Id) % 2 == 0 ? "even" : "odd")
                }).ToList()
            }, CancellationToken.None);
            Assert.True(load.IsSuccess, load.ErrorDetails);
            Assert.Equal(set.Count, await transport.GetDocumentCountAsync(""));

            await transport.BuildIndexAsync();
            await body(transport, set);
        });
    }

    private static async Task WithTransport(int concurrency, Func<PgVectorTransport, Task> body)
    {
        await using var schema = await PgTestSchema.CreateAsync();
        using var transport = new PgVectorTransport(schema.ConnectionString + ",public", PostgreSqlTestEndpoints.Database, concurrency, VectorMetric.Cosine, Dimensions);
        await transport.EnsureDatabaseExistsAsync(PostgreSqlTestEndpoints.Database);
        await body(transport);
    }

    private static Task<string> ShowEffortAsync(PgVectorTransport transport) => transport.ReadWithWorkerAsync(async (connection, _) =>
        (await connection.QueryAsync(PgVectorTransport.ShowEffortSql, CancellationToken.None))[0].Get<string>(0));

    private static VectorSearchOperation Search(float[] query, SearchEffort? effort, VectorFilter? filter) => new()
    {
        QueryVector = query,
        FieldName = "embedding",
        TopK = K,
        Effort = effort,
        Filter = filter
    };

    private static async Task<string[]> SearchAsync(PgVectorTransport transport, float[] query, SearchEffort? effort, VectorFilter? filter)
    {
        var result = await transport.ExecuteAsync(Search(query, effort, filter), CancellationToken.None);
        Assert.True(result.IsSuccess, result.ErrorDetails);
        Assert.InRange(result.NeighborIds!.Count, 0, K);
        return result.NeighborIds!.ToArray();
    }

    private static float[][] Seeded(int count, int seed)
    {
        var random = new Random(seed);
        return Enumerable.Range(0, count)
            .Select(_ => Enumerable.Range(0, Dimensions).Select(_ => (float)(random.NextDouble() * 2 - 1)).ToArray())
            .ToArray();
    }
}
