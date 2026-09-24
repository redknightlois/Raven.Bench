using System.Runtime.CompilerServices;
using Parquet;
using Parquet.Schema;
using PureHDF;
using PureHDF.Selections;
using PureHDF.VOL.Native;
using RavenBench.Core.Workload;

namespace RavenBench.Dataset.Vectors;

/// <summary>
/// An ann-benchmarks HDF5 file: datasets train, test, neighbors (row indexes into train) and distances,
/// and the metric in the root attribute "distance". Base ids are train row indexes. Opening a file whose
/// shape or distance disagrees with the declared set fails.
/// </summary>
public sealed class AnnBenchmarksHdf5Dataset(string name, int dimensions, VectorMetric metric, DatasetFile file) : IVectorDataset
{
    private const int ReadRows = 8192;

    public string Name { get; } = name;
    public int Dimensions { get; } = dimensions;
    public IReadOnlyList<DatasetFile> Files { get; } = [file];

    public VectorMetric Metric { get; } = metric;

    public static VectorMetric ParseMetric(string distance) => distance switch
    {
        "angular" or "cosine" => VectorMetric.Cosine,
        "euclidean" => VectorMetric.L2,
        "dot" or "ip" => VectorMetric.Dot,
        _ => throw new NotSupportedException($"ann-benchmarks distance '{distance}' is not a supported vector metric.")
    };

    public Task<VectorQuerySet> GetQueriesAsync(VerifiedFiles files, QuerySelection selection, int k, CancellationToken ct = default)
    {
        using var h5 = Open(files);
        var test = h5.Dataset("test").Read<float[,]>();
        var neighbors = h5.Dataset("neighbors").Read<int[,]>();
        if (neighbors.GetLength(1) < k)
            throw new InvalidDataException($"Set '{Name}' publishes {neighbors.GetLength(1)} neighbours per query; k={k} needs more.");

        var positions = QueryDraw.Positions(test.GetLength(0), selection);
        var queries = positions.Select(p => Row(test, p)).ToArray();
        var truth = positions.Select(p => Enumerable.Range(0, neighbors.GetLength(1)).Select(j => neighbors[p, j].ToString()).ToArray()).ToArray();
        return Task.FromResult(new VectorQuerySet(queries, truth));
    }

    public async IAsyncEnumerable<BaseVector> ReadBaseAsync(VerifiedFiles files, QuerySelection selection, [EnumeratorCancellation] CancellationToken ct = default)
    {
        using var h5 = Open(files);
        var train = h5.Dataset("train");
        var rows = train.Space.Dimensions[0];
        for (ulong start = 0; start < rows; start += ReadRows)
        {
            ct.ThrowIfCancellationRequested();
            var count = Math.Min(ReadRows, rows - start);
            var block = train.Read<float[,]>(new HyperslabSelection(2, [start, 0], [count, (ulong)Dimensions]), memoryDims: [count, (ulong)Dimensions]);
            for (int i = 0; i < (int)count; i++)
                yield return new BaseVector((start + (ulong)i).ToString(), Row(block, i));
            await Task.Yield();
        }
    }

    private NativeFile Open(VerifiedFiles files)
    {
        var h5 = H5File.OpenRead(files.PathOf(Files[0].FileName));
        var declared = h5.Attribute("distance").Read<string>();
        if (ParseMetric(declared) != Metric)
        {
            h5.Dispose();
            throw new InvalidDataException($"Set '{Name}' file declares distance '{declared}'; the set is declared {Metric}.");
        }
        var dims = h5.Dataset("train").Space.Dimensions;
        if (dims.Length != 2 || (int)dims[1] != Dimensions)
        {
            h5.Dispose();
            throw new InvalidDataException($"Set '{Name}' train has shape [{string.Join(",", dims)}]; expected {Dimensions} dimensions.");
        }
        return h5;
    }

    private static float[] Row(float[,] matrix, long row)
    {
        var vector = new float[matrix.GetLength(1)];
        for (int j = 0; j < vector.Length; j++)
            vector[j] = matrix[row, j];
        return vector;
    }
}

/// <summary>
/// A VectorDBBench parquet set: train and test files with columns id and emb, and a neighbors file with
/// columns id (the test id) and neighbors_id (train ids, nearest first). Base ids are train ids.
/// </summary>
public sealed class VectorDbBenchParquetDataset(string name, int dimensions, VectorMetric metric, DatasetFile train, DatasetFile test, DatasetFile neighbors) : IVectorDataset
{
    public string Name { get; } = name;
    public int Dimensions { get; } = dimensions;
    public VectorMetric Metric { get; } = metric;
    public IReadOnlyList<DatasetFile> Files { get; } = [train, test, neighbors];

    public async Task<VectorQuerySet> GetQueriesAsync(VerifiedFiles files, QuerySelection selection, int k, CancellationToken ct = default)
    {
        var tests = new List<(long Id, float[] Vector)>();
        await foreach (var row in ParquetVectors.ReadAsync(files.PathOf(test.FileName), "id", "emb", Dimensions, ct).ConfigureAwait(false))
            tests.Add(row);

        var truthById = new Dictionary<long, long[]>();
        await foreach (var (id, list) in ParquetVectors.ReadListsAsync(files.PathOf(neighbors.FileName), "id", "neighbors_id", ct).ConfigureAwait(false))
            truthById[id] = list;

        var positions = QueryDraw.Positions(tests.Count, selection);
        var queries = new float[positions.Length][];
        var truth = new string[positions.Length][];
        for (int i = 0; i < positions.Length; i++)
        {
            var (id, vector) = tests[(int)positions[i]];
            if (truthById.TryGetValue(id, out var nearest) == false)
                throw new InvalidDataException($"Set '{Name}' publishes no neighbours for test id {id}.");
            if (nearest.Length < k)
                throw new InvalidDataException($"Set '{Name}' publishes {nearest.Length} neighbours for test id {id}; k={k} needs more.");
            queries[i] = vector;
            truth[i] = nearest.Select(n => n.ToString()).ToArray();
        }
        return new VectorQuerySet(queries, truth);
    }

    public async IAsyncEnumerable<BaseVector> ReadBaseAsync(VerifiedFiles files, QuerySelection selection, [EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var (id, vector) in ParquetVectors.ReadAsync(files.PathOf(train.FileName), "id", "emb", Dimensions, ct).ConfigureAwait(false))
            yield return new BaseVector(id.ToString(), vector);
    }
}

/// <summary>
/// Reads an id column and a list column from a parquet file, row group by row group.
/// </summary>
internal static class ParquetVectors
{
    public static async IAsyncEnumerable<(long Id, float[] Vector)> ReadAsync(string path, string idColumn, string listColumn, int dimensions, [EnumeratorCancellation] CancellationToken ct)
    {
        await foreach (var (ids, flat) in ReadColumnsAsync(path, idColumn, listColumn, ct).ConfigureAwait(false))
        {
            var values = ToFloats(flat, path, listColumn);
            if (values.Length != ids.Length * dimensions)
                throw new InvalidDataException($"'{path}' column {listColumn} holds {values.Length} values for {ids.Length} rows; expected {dimensions} per row.");
            for (int i = 0; i < ids.Length; i++)
                yield return (ids[i], values.AsSpan(i * dimensions, dimensions).ToArray());
        }
    }

    public static async IAsyncEnumerable<(long Id, long[] List)> ReadListsAsync(string path, string idColumn, string listColumn, [EnumeratorCancellation] CancellationToken ct)
    {
        await foreach (var (ids, flat) in ReadColumnsAsync(path, idColumn, listColumn, ct).ConfigureAwait(false))
        {
            var values = ToLongs(flat, path, listColumn);
            if (ids.Length == 0 || values.Length % ids.Length != 0)
                throw new InvalidDataException($"'{path}' column {listColumn} holds {values.Length} values for {ids.Length} rows; lists must have one length.");
            var width = values.Length / ids.Length;
            for (int i = 0; i < ids.Length; i++)
                yield return (ids[i], values.AsSpan(i * width, width).ToArray());
        }
    }

    private static async IAsyncEnumerable<(long[] Ids, Array Flat)> ReadColumnsAsync(string path, string idColumn, string listColumn, [EnumeratorCancellation] CancellationToken ct)
    {
        await using var stream = File.OpenRead(path);
        using var reader = await ParquetReader.CreateAsync(stream, cancellationToken: ct).ConfigureAwait(false);
        var fields = reader.Schema.GetDataFields();
        DataField Find(string column) => fields.FirstOrDefault(f => f.Path.ToList()[0] == column)
            ?? throw new InvalidDataException($"'{path}' has no column '{column}'.");
        var idField = Find(idColumn);
        var listField = Find(listColumn);

        for (int g = 0; g < reader.RowGroupCount; g++)
        {
            using var group = reader.OpenRowGroupReader(g);
            var ids = ToLongs((await group.ReadColumnAsync(idField, ct).ConfigureAwait(false)).Data, path, idColumn);
            var flat = (await group.ReadColumnAsync(listField, ct).ConfigureAwait(false)).Data;
            yield return (ids, flat);
        }
    }

    private static float[] ToFloats(Array data, string path, string column) => data switch
    {
        float[] f => f,
        double[] d => Array.ConvertAll(d, v => (float)v),
        float?[] f => Array.ConvertAll(f, v => v ?? throw Null(path, column)),
        double?[] d => Array.ConvertAll(d, v => (float)(v ?? throw Null(path, column))),
        _ => throw new InvalidDataException($"'{path}' column {column} has element type {data.GetType().GetElementType()}, not float.")
    };

    private static long[] ToLongs(Array data, string path, string column) => data switch
    {
        long[] l => l,
        int[] i => Array.ConvertAll(i, v => (long)v),
        long?[] l => Array.ConvertAll(l, v => v ?? throw Null(path, column)),
        int?[] i => Array.ConvertAll(i, v => (long)(v ?? throw Null(path, column))),
        _ => throw new InvalidDataException($"'{path}' column {column} has element type {data.GetType().GetElementType()}, not an integer.")
    };

    private static InvalidDataException Null(string path, string column) => new($"'{path}' column {column} holds a null.");
}
