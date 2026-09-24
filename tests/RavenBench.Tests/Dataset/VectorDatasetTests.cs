using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Parquet.Serialization;
using PureHDF;
using RavenBench.Core.Workload;
using RavenBench.Dataset;
using RavenBench.Dataset.Vectors;
using Xunit;

namespace RavenBench.Tests.Dataset;

public class VectorDatasetTests : IDisposable
{
    private readonly string _dataDir = Path.Combine(Path.GetTempPath(), $"vector-sets-{Guid.NewGuid():N}");

    public void Dispose()
    {
        if (Directory.Exists(_dataDir))
            Directory.Delete(_dataDir, recursive: true);
    }

    [Fact]
    public async Task Hdf5_PublishedLayout_YieldsShapesMetricAndPublishedNeighbours()
    {
        var train = Matrix(20, 4, seed: 1);
        var test = Matrix(3, 4, seed: 2);
        var neighbors = new int[3, 5];
        for (int q = 0; q < 3; q++)
            for (int j = 0; j < 5; j++)
                neighbors[q, j] = (q * 5 + j) % 20;

        var set = await PinAsync("tiny-angular", "tiny-angular.hdf5", path =>
        {
            var file = new H5File
            {
                ["train"] = train,
                ["test"] = test,
                ["neighbors"] = neighbors,
                ["distances"] = new float[3, 5]
            };
            file.Attributes["distance"] = "angular";
            file.Write(path);
            return Task.CompletedTask;
        }, f => new AnnBenchmarksHdf5Dataset("tiny-angular", 4, VectorMetric.Cosine, f));

        var files = await PinnedFiles.EnsureAsync(set, _dataDir);
        var queries = await set.GetQueriesAsync(files, new QuerySelection(7, 3), k: 5);
        var baseVectors = await ToListAsync(set.ReadBaseAsync(files, new QuerySelection(7, 3)));

        Assert.Equal(VectorMetric.Cosine, set.Metric);
        Assert.Equal(20, baseVectors.Count);
        Assert.All(baseVectors, b => Assert.Equal(4, b.Vector.Length));
        Assert.Equal(train[19, 3], baseVectors[19].Vector[3]);
        Assert.Equal(3, queries.Queries.Length);
        for (int i = 0; i < 3; i++)
        {
            var row = Enumerable.Range(0, 3).Single(r => Enumerable.Range(0, 4).All(c => test[r, c] == queries.Queries[i][c]));
            Assert.Equal(Enumerable.Range(0, 5).Select(j => neighbors[row, j].ToString()), queries.Neighbors[i]);
        }
    }

    [Fact]
    public async Task Hdf5_FileDistanceDisagreeingWithTheSet_Fails()
    {
        var set = await PinAsync("tiny-euclidean", "tiny.hdf5", path =>
        {
            var file = new H5File { ["train"] = Matrix(4, 2, 1), ["test"] = Matrix(1, 2, 2), ["neighbors"] = new int[1, 2] };
            file.Attributes["distance"] = "angular";
            file.Write(path);
            return Task.CompletedTask;
        }, f => new AnnBenchmarksHdf5Dataset("tiny-euclidean", 2, VectorMetric.L2, f));

        var files = await PinnedFiles.EnsureAsync(set, _dataDir);
        await Assert.ThrowsAsync<InvalidDataException>(() => set.GetQueriesAsync(files, new QuerySelection(1, 1), 1));
    }

    [Fact]
    public async Task Parquet_VectorDbBenchLayout_YieldsShapesMetricAndPublishedNeighbours()
    {
        var train = Enumerable.Range(0, 12).Select(i => new EmbRow { id = 100 + i, emb = Row(i, 6) }).ToList();
        var test = Enumerable.Range(0, 4).Select(i => new EmbRow { id = i, emb = Row(50 + i, 6) }).ToList();
        var neighbors = test.Select(t => new NeighborRow { id = t.id, neighbors_id = Enumerable.Range(0, 3).Select(j => 100L + (t.id + j) % 12).ToArray() }).ToList();

        var dir = Path.Combine(_dataDir, "tiny-cohere");
        Directory.CreateDirectory(dir);
        async Task<DatasetFile> Write<T>(string name, IList<T> rows)
        {
            var path = Path.Combine(dir, name);
            await using (var stream = File.Create(path))
                await ParquetSerializer.SerializeAsync(rows, stream);
            return new DatasetFile { FileName = name, Url = "file://test", Type = "vectors", EstimatedSizeBytes = 0, Sha256 = await PinnedFiles.Sha256Async(path) };
        }

        var set = new VectorDbBenchParquetDataset("tiny-cohere", 6, VectorMetric.Cosine,
            await Write("train.parquet", train), await Write("test.parquet", test), await Write("neighbors.parquet", neighbors));
        var files = await PinnedFiles.EnsureAsync(set, _dataDir);

        var queries = await set.GetQueriesAsync(files, new QuerySelection(3, 4), k: 3);
        var baseVectors = await ToListAsync(set.ReadBaseAsync(files, new QuerySelection(3, 4)));

        Assert.Equal(VectorMetric.Cosine, set.Metric);
        Assert.Equal(train.Select(t => t.id.ToString()), baseVectors.Select(b => b.Id));
        Assert.Equal(train[5].emb, baseVectors[5].Vector);
        Assert.Equal(4, queries.Queries.Length);
        for (int i = 0; i < 4; i++)
        {
            var source = test.Single(t => t.emb.SequenceEqual(queries.Queries[i]));
            Assert.Equal(neighbors.Single(n => n.id == source.id).neighbors_id.Select(n => n.ToString()), queries.Neighbors[i]);
        }
        await Assert.ThrowsAsync<InvalidDataException>(() => set.GetQueriesAsync(files, new QuerySelection(3, 1), k: 4));
    }

    [Fact]
    public async Task Checksum_OneCorruptedByteInAVerifiedFile_FailsTheNextRunBySetAndFile()
    {
        var (set, pin) = await PlaceWordsAsync(10);
        await PinnedFiles.EnsureAsync(set, _dataDir, sha256: pin);

        var path = Path.Combine(_dataDir, set.Name, set.Files[0].FileName);
        var bytes = await File.ReadAllBytesAsync(path);
        bytes[bytes.Length / 2] ^= 0xFF;
        await File.WriteAllBytesAsync(path, bytes);

        var ex = await Assert.ThrowsAsync<DatasetChecksumException>(() => PinnedFiles.EnsureAsync(set, _dataDir, sha256: pin));
        Assert.Equal(set.Name, ex.SetName);
        Assert.Equal(set.Files[0].FileName, ex.FileName);
        Assert.Equal(pin, ex.Expected);
        Assert.NotEqual(ex.Expected, ex.Actual);
        Assert.Contains(set.Name, ex.Message);
        Assert.Contains(ex.Actual!, ex.Message);
    }

    [Fact]
    public async Task Checksum_FileWithoutAPin_FailsBySetName()
    {
        var (set, _) = await PlaceWordsAsync(10);
        var ex = await Assert.ThrowsAsync<DatasetChecksumException>(() => PinnedFiles.EnsureAsync(set, _dataDir));
        Assert.Equal("clinical-words-100", ex.SetName);
    }

    [Fact]
    public async Task OperatorPin_NeverReplacesACatalogPin()
    {
        var set = await PinAsync("sphere-100k", "sphere.jsonl.gz", path => WriteSphereAsync(path, 3), f => new SphereDatasetProvider("100k", f, 3));
        var other = new string('a', 64);

        var ex = await Assert.ThrowsAsync<DatasetChecksumException>(() => PinnedFiles.EnsureAsync(set, _dataDir, sha256: other));
        Assert.Equal(set.Files[0].Sha256, ex.Expected);
        await PinnedFiles.EnsureAsync(set, _dataDir, sha256: set.Files[0].Sha256!.ToUpperInvariant());
    }

    [Fact]
    public async Task SourceFile_OutsideTheDataDirectory_IsVerifiedLikeAFetchedFile()
    {
        var source = Path.Combine(_dataDir, "elsewhere.jsonl.gz");
        Directory.CreateDirectory(_dataDir);
        await WriteSphereAsync(source, 3);
        var set = new SphereDatasetProvider("100k", new DatasetFile { FileName = "sphere.jsonl.gz", Url = "", Type = "vectors", EstimatedSizeBytes = 0, Sha256 = await PinnedFiles.Sha256Async(source) }, 3);

        var files = await PinnedFiles.EnsureAsync(set, _dataDir, sourcePath: source);
        Assert.Equal(source, files.PathOf("sphere.jsonl.gz"));

        await File.AppendAllTextAsync(source, "x");
        await Assert.ThrowsAsync<DatasetChecksumException>(() => PinnedFiles.EnsureAsync(set, _dataDir, sourcePath: source));
    }

    [Theory]
    [InlineData(VectorMetric.Cosine, new[] { "a", "b", "c" })]
    [InlineData(VectorMetric.Dot, new[] { "b", "a", "c" })]
    [InlineData(VectorMetric.L2, new[] { "a", "c", "b" })]
    public async Task BruteForce_HandCheckedCase_RanksByTheMetric(VectorMetric metric, string[] expected)
    {
        // q=(1,0): cosine a=1, b=0.995, c=0; dot b=10, a=1, c=0; squared L2 a=0, c=2, b=82.
        var baseVectors = new[] { new BaseVector("a", [1f, 0f]), new BaseVector("b", [10f, 1f]), new BaseVector("c", [0f, 1f]) };
        var truth = await BruteForceTruth.ComputeAsync([[1f, 0f]], baseVectors.ToAsyncEnumerable(), metric, k: 3);
        Assert.Equal(expected, truth[0]);
    }

    [Fact]
    public async Task BruteForce_EqualScores_RankTheEarlierBaseVectorFirst()
    {
        var baseVectors = new[] { new BaseVector("x", [0f, 1f]), new BaseVector("first", [1f, 0f]), new BaseVector("second", [1f, 0f]) };
        var truth = await BruteForceTruth.ComputeAsync([[1f, 0f]], baseVectors.ToAsyncEnumerable(), VectorMetric.L2, k: 2);
        Assert.Equal(new[] { "first", "second" }, truth[0]);
    }

    [Fact]
    public async Task ClinicalWords_QueriesDrawnBySeed_AreHeldOutOfTheLoadAndOutOfTheirTruth()
    {
        const int words = 60;
        var (set, pin) = await PlaceWordsAsync(words);
        var files = await PinnedFiles.EnsureAsync(set, _dataDir, sha256: pin);

        var selection = new QuerySelection(11, 8);
        var queries = await set.GetQueriesAsync(files, selection, k: 5);
        var loaded = await ToListAsync(set.ReadBaseAsync(files, selection));

        Assert.Equal(words - 8, loaded.Count);
        AssertHeldOut(queries, loaded);
        Assert.Equal(await BruteForceTruth.ComputeAsync(queries.Queries, loaded.ToAsyncEnumerable(), VectorMetric.Cosine, 5), queries.Neighbors);

        var same = await set.GetQueriesAsync(files, new QuerySelection(11, 8), k: 5);
        var other = await set.GetQueriesAsync(files, new QuerySelection(12, 8), k: 5);
        Assert.Equal(queries.Queries, same.Queries);
        Assert.NotEqual(queries.Queries, other.Queries);
    }

    [Fact]
    public async Task TruthCache_KeyedBySelection_ReusesAMatchAndRecomputesOnAChange()
    {
        var (set, pin) = await PlaceWordsAsync(30);
        var files = await PinnedFiles.EnsureAsync(set, _dataDir, sha256: pin);
        string[] CacheFiles() => Directory.GetFiles(files.Directory, "truth-*.bin");

        var first = await set.GetQueriesAsync(files, new QuerySelection(1, 4), k: 3);
        Assert.Single(CacheFiles());
        Assert.Equal(first.Neighbors, (await set.GetQueriesAsync(files, new QuerySelection(1, 4), k: 3)).Neighbors);
        Assert.Single(CacheFiles());

        await set.GetQueriesAsync(files, new QuerySelection(2, 4), k: 3);
        await set.GetQueriesAsync(files, new QuerySelection(1, 5), k: 3);
        Assert.Equal(3, CacheFiles().Length);
    }

    [Fact]
    public async Task Sphere_SmallSelection_HoldsQueriesOutOfTheLoad()
    {
        const int passages = 40;
        var set = await PinAsync("sphere-100k", "sphere.jsonl.gz", path => WriteSphereAsync(path, passages),
            f => new SphereDatasetProvider("100k", f, passages));
        var files = await PinnedFiles.EnsureAsync(set, _dataDir);

        var selection = new QuerySelection(5, 6);
        var queries = await set.GetQueriesAsync(files, selection, k: 10);
        var loaded = await ToListAsync(set.ReadBaseAsync(files, selection));

        Assert.Equal(passages - 6, loaded.Count);
        AssertHeldOut(queries, loaded);
        Assert.Equal(VectorMetric.Cosine, set.Metric);
    }

    [Fact]
    public void Recall_ResultEqualToTheTruth_IsOne_AndShortResultsLowerIt()
    {
        var truth = new Dictionary<int, string[]> { [0] = ["a", "b", "c"], [1] = ["d", "e", "f"] };
        Assert.Equal(1.0, RecallMeasurement.ComputeRecall([["a", "b", "c"], ["d", "e", "f"]], truth, [1, 3])[3]);
        Assert.Equal(0.5, RecallMeasurement.ComputeRecall([["a", "b", "c"], []], truth, [3])[3]);
    }

    private static void AssertHeldOut(VectorQuerySet queries, List<BaseVector> loaded)
    {
        var loadedIds = loaded.Select(b => b.Id).ToHashSet();
        foreach (var query in queries.Queries)
            Assert.DoesNotContain(loaded, b => b.Vector.SequenceEqual(query));
        Assert.All(queries.Neighbors, n => Assert.All(n, id => Assert.Contains(id, loadedIds)));
    }

    private async Task<T> PinAsync<T>(string setName, string fileName, Func<string, Task> write, Func<DatasetFile, T> create) where T : IVectorDataset
    {
        var dir = Path.Combine(_dataDir, setName);
        Directory.CreateDirectory(dir);
        var path = Path.Combine(dir, fileName);
        await write(path);
        var set = create(new DatasetFile { FileName = fileName, Url = "file://test", Type = "vectors", EstimatedSizeBytes = 0, Sha256 = await PinnedFiles.Sha256Async(path) });
        Assert.Equal(setName, set.Name);
        return set;
    }

    // A ClinicalWords parquet placed where the set reads it, with the SHA-256 an operator would pass.
    private async Task<(ClinicalWordsDatasetProvider Set, string Pin)> PlaceWordsAsync(int count)
    {
        var set = new ClinicalWordsDatasetProvider(100);
        var dir = Path.Combine(_dataDir, set.Name);
        Directory.CreateDirectory(dir);
        var path = Path.Combine(dir, set.Files[0].FileName);
        await WriteWordsAsync(path, count);
        return (set, await PinnedFiles.Sha256Async(path));
    }

    private static async Task WriteWordsAsync(string path, int count)
    {
        var rows = Enumerable.Range(0, count).Select(i => new WordRow { word = $"word{i}", vector = Row(i, 100).Select(v => (double)v).ToArray() }).ToList();
        await using var stream = File.Create(path);
        await ParquetSerializer.SerializeAsync(rows, stream);
    }

    private static async Task WriteSphereAsync(string path, int count)
    {
        await using var fs = File.Create(path);
        await using var gz = new GZipStream(fs, CompressionLevel.Fastest);
        await using var writer = new StreamWriter(gz, Encoding.UTF8);
        for (int i = 0; i < count; i++)
            await writer.WriteLineAsync(JsonSerializer.Serialize(new { id = $"p{i}", raw = "t", sha = $"sha{i}", title = "t", url = "u", vector = Row(i, 768) }));
    }

    private static float[] Row(int seed, int dims)
    {
        var rng = new Random(seed);
        return Enumerable.Range(0, dims).Select(_ => (float)(rng.NextDouble() * 2 - 1)).ToArray();
    }

    private static float[,] Matrix(int rows, int cols, int seed)
    {
        var m = new float[rows, cols];
        for (int r = 0; r < rows; r++)
        {
            var row = Row(seed * 1000 + r, cols);
            for (int c = 0; c < cols; c++)
                m[r, c] = row[c];
        }
        return m;
    }

    private static async Task<List<T>> ToListAsync<T>(IAsyncEnumerable<T> source)
    {
        var list = new List<T>();
        await foreach (var item in source)
            list.Add(item);
        return list;
    }

    public sealed class EmbRow
    {
        public long id { get; set; }
        public float[] emb { get; set; } = [];
    }

    public sealed class NeighborRow
    {
        public long id { get; set; }
        public long[] neighbors_id { get; set; } = [];
    }

    public sealed class WordRow
    {
        public string word { get; set; } = "";
        public double[] vector { get; set; } = [];
    }
}
