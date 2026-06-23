using System.Net.Http;
using System.IO;
using RavenBench.Core;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;

namespace RavenBench.Dataset;

/// <summary>
/// Manages dataset download, caching, and import into RavenDB.
/// </summary>
public sealed class DatasetManager : IDisposable
{
    private readonly string _cacheDir;
    private readonly HttpClient _httpClient;

    public DatasetManager(string? cacheDir = null)
    {
        _cacheDir = cacheDir ?? Path.Combine(Path.GetTempPath(), "RavenBench", "datasets");
        Directory.CreateDirectory(_cacheDir);
        _httpClient = new HttpClient { Timeout = TimeSpan.FromHours(2) };
    }

    public void Dispose()
    {
        _httpClient.Dispose();
    }

    /// <summary>
    /// Downloads a dataset file if not already cached.
    /// </summary>
    public async Task<string> DownloadAsync(DatasetFile file, IProgress<double>? progress = null, CancellationToken ct = default)
    {
        var localPath = Path.Combine(_cacheDir, file.FileName);

        if (File.Exists(localPath))
        {
            Console.WriteLine($"[Dataset] Using cached {file.FileName}");
            return localPath;
        }

        Console.WriteLine($"[Dataset] Downloading {file.FileName} from {file.Url}");
        Console.WriteLine($"[Dataset] Estimated size: {FormatBytes(file.EstimatedSizeBytes)}");

        using var response = await _httpClient.GetAsync(file.Url, HttpCompletionOption.ResponseHeadersRead, ct);
        response.EnsureSuccessStatusCode();

        var totalBytes = response.Content.Headers.ContentLength ?? file.EstimatedSizeBytes;
        var downloadedBytes = 0L;

        await using var contentStream = await response.Content.ReadAsStreamAsync(ct);
        var tempPath = localPath + ".downloading";
        await using (var fileStream = new FileStream(tempPath, FileMode.Create, FileAccess.Write, FileShare.None, 8192, useAsync: true))
        {
            var buffer = new byte[8192];
            int bytesRead;
            var lastProgressReport = DateTime.UtcNow;

            while ((bytesRead = await contentStream.ReadAsync(buffer, ct)) > 0)
            {
                await fileStream.WriteAsync(buffer.AsMemory(0, bytesRead), ct);
                downloadedBytes += bytesRead;

                if (DateTime.UtcNow - lastProgressReport > TimeSpan.FromSeconds(2))
                {
                    var progressPct = totalBytes > 0 ? (double)downloadedBytes / totalBytes * 100 : 0;
                    Console.WriteLine($"[Dataset] Downloaded {FormatBytes(downloadedBytes)} / {FormatBytes(totalBytes)} ({progressPct:F1}%)");
                    progress?.Report(progressPct);
                    lastProgressReport = DateTime.UtcNow;
                }
            }
        }

        File.Move(tempPath, localPath);
        Console.WriteLine($"[Dataset] Download complete: {file.FileName}");
        return localPath;
    }

    /// <summary>
    /// Ensures a database exists on the server.
    /// </summary>
    private async Task EnsureDatabaseExistsAsync(string serverUrl, string databaseName, string? httpVersion = null, CancellationToken ct = default)
    {
        using var store = new DocumentStore
        {
            Urls = new[] { serverUrl }
        };
        
        if (string.IsNullOrEmpty(httpVersion) == false)
            HttpHelper.ConfigureHttpVersion(store, httpVersion);

        store.Initialize();

        var dbRecord = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName), ct);
        if (dbRecord == null)
        {
            Console.WriteLine($"[Dataset] Creating database '{databaseName}'");
            await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(databaseName)), ct);
        }
    }

    /// <summary>
    /// Imports a RavenDB dump file into the specified database using Smuggler API.
    /// </summary>
    public async Task ImportDumpAsync(string dumpFilePath, string serverUrl, string databaseName, string? httpVersion = null, CancellationToken ct = default)
    {
        Console.WriteLine($"[Dataset] Importing {Path.GetFileName(dumpFilePath)} into database '{databaseName}'");

        // Ensure database exists before importing
        await EnsureDatabaseExistsAsync(serverUrl, databaseName, httpVersion, ct);

        using var store = new DocumentStore
        {
            Urls = new[] { serverUrl },
            Database = databaseName
        };
        
        if (string.IsNullOrEmpty(httpVersion) == false)
            HttpHelper.ConfigureHttpVersion(store, httpVersion);

        store.Initialize();

        await using var dumpStream = await OpenDumpStreamAsync(dumpFilePath, ct);
        // Raven.Bench builds its own -corax indexes per workload; the dump's bundled indexes
        // are never queried and their builds saturate small SKUs, so exclude them on import.
        var importOptions = new DatabaseSmugglerImportOptions();
        importOptions.OperateOnTypes &= ~DatabaseItemType.Indexes;
        var operation = await store.Smuggler.ImportAsync(importOptions, dumpStream, ct);

        Console.WriteLine($"[Dataset] Import operation started");

        try
        {
            await operation.WaitForCompletionAsync();
            Console.WriteLine($"[Dataset] Import completed successfully");
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Import failed: {ex.Message}", ex);
        }
    }

    /// <summary>
    /// Downloads and imports a complete dataset.
    /// </summary>
    public async Task ImportDatasetAsync(DatasetInfo dataset, string serverUrl, string databaseName, string? httpVersion = null, CancellationToken ct = default)
    {
        Console.WriteLine($"[Dataset] Importing dataset '{dataset.Name}' into database '{databaseName}'");
        Console.WriteLine($"[Dataset] {dataset.Description}");
        Console.WriteLine($"[Dataset] Files to import: {dataset.Files.Count}");

        foreach (var file in dataset.Files)
        {
            var localPath = await DownloadAsync(file, progress: null, ct);

            await ImportDumpAsync(localPath, serverUrl, databaseName, httpVersion, ct);
        }

        Console.WriteLine($"[Dataset] Dataset '{dataset.Name}' import complete");
    }

    /// <summary>
    /// Checks if the StackOverflow dataset appears to be already imported by checking
    /// for questions and users collections with minimum document counts.
    /// </summary>
    public async Task<bool> IsStackOverflowDatasetImportedAsync(string serverUrl, string databaseName, string? httpVersion = null, int expectedMinDocuments = 1000)
    {
        try
        {
            using var store = new DocumentStore
            {
                Urls = new[] { serverUrl }
            };
            
            if (string.IsNullOrEmpty(httpVersion) == false)
                HttpHelper.ConfigureHttpVersion(store, httpVersion);

            store.Initialize();

            // First check if database exists
            var dbRecord = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
            if (dbRecord == null)
            {
                Console.WriteLine($"[Dataset] Database '{databaseName}' does not exist");
                return false;
            }

            // Check document counts using ForDatabase() since store is already initialized
            var stats = await store.Maintenance.ForDatabase(databaseName).SendAsync(new Raven.Client.Documents.Operations.GetStatisticsOperation());

            if (stats.CountOfDocuments < expectedMinDocuments)
            {
                Console.WriteLine($"[Dataset] Database '{databaseName}' exists but has only {stats.CountOfDocuments} documents (expected >= {expectedMinDocuments})");
                return false;
            }

            using var session = store.OpenAsyncSession(databaseName);
            var questionsExist = await session.Advanced.AsyncRawQuery<object>("from questions")
                .Take(1)
                .AnyAsync();

            var usersExist = await session.Advanced.AsyncRawQuery<object>("from users")
                .Take(1)
                .AnyAsync();

            if (questionsExist == false || usersExist == false)
            {
                Console.WriteLine($"[Dataset] Database '{databaseName}' exists but missing expected collections (questions: {questionsExist}, users: {usersExist})");
                return false;
            }

            Console.WriteLine($"[Dataset] Database '{databaseName}' already has {stats.CountOfDocuments} documents with expected collections");
            return true;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Dataset] Error checking if dataset exists: {ex.Message}");
            return false;
        }
    }

    /// <summary>
    /// Opens a dump for import. Some published StackOverflow dumps declare BuildVersion 40000
    /// while carrying v3-era metadata (Raven-Entity-Name); the server translates that to
    /// @collection only for BuildVersion below 40000, so such dumps are rewritten to 30000 —
    /// otherwise every document lands in the @empty collection and no index maps anything.
    /// </summary>
    private static async Task<Stream> OpenDumpStreamAsync(string path, CancellationToken ct)
    {
        Stream stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 128 * 1024, useAsync: true);
        var header = new byte[2];
        var headerRead = await stream.ReadAtLeastAsync(header, 2, throwOnEndOfStream: false, ct);
        stream.Position = 0;
        if (headerRead == 2 && header[0] == 0x1F && header[1] == 0x8B)
            stream = new System.IO.Compression.GZipStream(stream, System.IO.Compression.CompressionMode.Decompress);

        var prefix = new byte[64 * 1024];
        var prefixLength = await stream.ReadAtLeastAsync(prefix, prefix.Length, throwOnEndOfStream: false, ct);

        var text = System.Text.Encoding.Latin1.GetString(prefix, 0, prefixLength);
        var version = System.Text.RegularExpressions.Regex.Match(text, """^\{\s*"BuildVersion"\s*:\s*(\d+)""");
        if (version.Success && version.Groups[1].Length == 5 &&
            long.Parse(version.Groups[1].Value) >= 40000 && text.Contains("\"Raven-Entity-Name\""))
        {
            Console.WriteLine("[Dataset] Dump carries v3 metadata under a v4+ BuildVersion; rewriting header so collections are preserved");
            System.Text.Encoding.Latin1.GetBytes("30000", prefix.AsSpan(version.Groups[1].Index, 5));
        }

        return new PrefixedStream(prefix, prefixLength, stream);
    }

    private sealed class PrefixedStream(byte[] prefix, int prefixLength, Stream inner) : Stream
    {
        private int _position;

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }

        public override int Read(byte[] buffer, int offset, int count) => Read(buffer.AsSpan(offset, count));

        public override int Read(Span<byte> buffer)
        {
            if (_position < prefixLength)
            {
                var n = Math.Min(buffer.Length, prefixLength - _position);
                prefix.AsSpan(_position, n).CopyTo(buffer);
                _position += n;
                return n;
            }
            return inner.Read(buffer);
        }

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken ct = default)
        {
            if (_position < prefixLength)
            {
                var n = Math.Min(buffer.Length, prefixLength - _position);
                prefix.AsMemory(_position, n).CopyTo(buffer);
                _position += n;
                return n;
            }
            return await inner.ReadAsync(buffer, ct);
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken ct) =>
            ReadAsync(buffer.AsMemory(offset, count), ct).AsTask();

        public override void Flush() { }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            if (disposing)
                inner.Dispose();
            base.Dispose(disposing);
        }

        public override async ValueTask DisposeAsync()
        {
            await inner.DisposeAsync();
            await base.DisposeAsync();
        }
    }

    private static string FormatBytes(long bytes)
    {
        string[] sizes = { "B", "KB", "MB", "GB", "TB" };
        double len = bytes;
        int order = 0;
        while (len >= 1024 && order < sizes.Length - 1)
        {
            order++;
            len /= 1024;
        }
        return $"{len:0.##} {sizes[order]}";
    }
}
