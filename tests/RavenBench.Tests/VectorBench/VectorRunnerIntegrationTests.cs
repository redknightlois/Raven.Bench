using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using RavenBench.Cli;
using RavenBench.Core.Diagnostics;
using RavenBench.Core.Vector;
using RavenBench.Core.Workload;
using RavenBench.Dataset;
using RavenBench.Dataset.Vectors;
using RavenBench.Tests.Infrastructure;
using RavenBench.VectorBench;
using Xunit;

namespace RavenBench.Tests.VectorBench;

/// <summary>A small pinned set without a query split: raw little-endian float32 rows.</summary>
internal sealed class TinyHeldOutSet : HeldOutVectorDataset
{
    public const int Rows = 3000;
    public const int Dims = 16;
    private readonly DatasetFile _file;

    public TinyHeldOutSet(string directory)
    {
        var random = new Random(11);
        var bytes = new byte[Rows * Dims * 4];
        var floats = Enumerable.Range(0, Rows * Dims).Select(_ => (float)(random.NextDouble() * 2 - 1)).ToArray();
        Buffer.BlockCopy(floats, 0, bytes, 0, bytes.Length);
        Directory.CreateDirectory(Path.Combine(directory, "tiny-held-out"));
        File.WriteAllBytes(Path.Combine(directory, "tiny-held-out", "tiny.f32"), bytes);
        _file = new DatasetFile { FileName = "tiny.f32", Url = "file://unused", Type = "vectors", EstimatedSizeBytes = bytes.Length, Sha256 = Convert.ToHexStringLower(SHA256.HashData(bytes)) };
    }

    public override string Name => "tiny-held-out";
    public override VectorMetric Metric => VectorMetric.Cosine;
    public override int Dimensions => Dims;
    public override IReadOnlyList<DatasetFile> Files => [_file];

    protected override Task<long> CountRowsAsync(VerifiedFiles files, CancellationToken ct) => Task.FromResult((long)Rows);

    protected override async IAsyncEnumerable<BaseVector> ReadRowsAsync(VerifiedFiles files, [EnumeratorCancellation] CancellationToken ct)
    {
        var bytes = await File.ReadAllBytesAsync(files.PathOf("tiny.f32"), ct);
        for (int r = 0; r < Rows; r++)
        {
            var v = new float[Dims];
            Buffer.BlockCopy(bytes, r * Dims * 4, v, 0, Dims * 4);
            yield return new BaseVector(r.ToString(), v);
        }
    }
}

/// <summary>The five runs end to end against the real products, on a set small enough to run on any host.</summary>
[Collection(LiveServers.Name)]
public class VectorRunnerIntegrationTests
{
    private const int Cap = 2000;

    private static VectorScenario SmallScenario(string dataDirectory, int? cap)
    {
        var shipped = VectorScenario.Load(Path.Combine(RepositoryRootLocator.Find(), "benchmarks", "vector", "scenario.json"));
        return shipped with
        {
            VectorCountCap = cap,
            DataDirectory = dataDirectory,
            QueryCount = 20,
            TruthDepth = 30,
            Readers = 8,
            FilterSelectivity = 0.2,
            InsertRate = 50,
            UnderInsertQueryRate = 20,
            Warmup = "0s",
            Duration = "2s",
            // Probes equal to lists scan every list, so the rebuilt index answers exactly.
            CrossCheck = shipped.CrossCheck with
            {
                Dataset = "tiny-held-out",
                PublishedRecall = 1.0,
                PublishedIndexKind = "ivfflat",
                PublishedBuildOptions = new() { ["lists"] = 4 },
                PublishedSearchValue = 4,
                Tolerance = 0.001
            }
        };
    }

    [RequiresRavenDbFact(8081)]
    public Task RavenDb_Completes_The_Five_Runs_On_A_Capped_Set() => RunAsync("ravendb", "http://localhost:8081", "vector_it_" + Guid.NewGuid().ToString("N")[..8], SearchEffort.RavenDbKnob, Cap);

    [RequiresRavenDbFact(8087)]
    public Task RavenDb7_Completes_The_Five_Runs() => RunAsync("ravendb-7", "http://localhost:8087", "vector_it_" + Guid.NewGuid().ToString("N")[..8], SearchEffort.RavenDbKnob, null);

    [RequiresPostgreSqlFact]
    public async Task PgVector_Completes_The_Five_Runs()
    {
        await using var schema = await PgTestSchema.CreateAsync();
        var info = await RunAsync("pgvector", schema.ConnectionString + ",public", PostgreSqlTestEndpoints.Database, SearchEffort.PgVectorKnob, null);

        var check = info["recall"].CrossCheck!;
        check.MeasuredIndexDefinition.Should().Contain("ivfflat").And.Contain("lists='4'");
        check.MeasuredRecall.Should().Be(1.0, "an index searched at the published setting answers like the published run");
        check.Verdict.Should().StartWith("near");
        info["filtered"].ProductSettings.Should().ContainKey("hnsw.iterative_scan");
        info["filtered"].Filtered!.RecallStatement.Should().Contain("hnsw.iterative_scan=");
    }

    private static async Task<Dictionary<string, Core.Reporting.VectorRunInfo>> RunAsync(string target, string url, string database, string knob, int? cap)
    {
        var data = Directory.CreateTempSubdirectory("vector-it-");
        try
        {
            var set = new TinyHeldOutSet(data.FullName);
            var settings = new VectorSettings { Target = target, Url = url, Database = database };
            var results = await new VectorRunner(SmallScenario(data.FullName, cap), new Dictionary<string, string>(), settings, set).RunAsync();

            results.Select(r => r.Run).Should().Equal(VectorRunner.Runs);
            var info = results.ToDictionary(r => r.Run, r => r.Summary.Vector!);

            info["load"].Load!.WallTimeSeconds.Should().BePositive();
            info["load"].Dataset.LoadedBaseVectors.Should().Be((cap ?? TinyHeldOutSet.Rows - 20) - 100);

            var curve = info["recall"].Recall!.Curve;
            curve.Should().HaveCount(3).And.OnlyContain(p => p.Knob == knob && p.Recall > 0 && p.Recall <= 1 && p.ReturnedRows <= 20 * 10);
            info["recall"].Recall!.Statement.Should().NotBeNullOrEmpty();

            var readers = results.Single(r => r.Run == "readers").Summary;
            readers.Steps.Should().HaveCount(2);
            readers.Steps[1].TargetThroughput.Should().Be(info["readers"].Readers!.FixedRate);
            readers.Steps[1].ScheduledOperations.Should().BePositive();
            readers.Steps[1].P9999.Should().BeGreaterThanOrEqualTo(readers.Steps[1].Raw.P50);
            info["readers"].EffortStatement.Should().NotBeNullOrEmpty();

            var filtered = info["filtered"].Filtered!;
            filtered.RowCountPerQuery.Should().HaveCount(20).And.OnlyContain(c => c >= 0 && c <= 10);

            var underInsert = info["under-insert"].UnderInsert!;
            underInsert.Inserted.Should().BePositive();
            underInsert.ScoredQueries.Should().BePositive();
            underInsert.Recall.Should().BeInRange(0, 1);
            return info;
        }
        finally
        {
            data.Delete(recursive: true);
        }
    }
}
