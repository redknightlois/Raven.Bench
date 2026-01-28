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
public sealed class DatasetManager
{
    private readonly string _cacheDir;
    private readonly HttpClient _httpClient;
    private readonly Dictionary<string, IDatasetProvider> _providers;

    public DatasetManager(string? cacheDir = null)
    {
        _cacheDir = cacheDir ?? Path.Combine(Path.GetTempPath(), "RavenBench", "datasets");
        Directory.CreateDirectory(_cacheDir);
        _httpClient = new HttpClient { Timeout = TimeSpan.FromHours(2) };

        // Register known dataset providers
        _providers = new Dictionary<string, IDatasetProvider>(StringComparer.OrdinalIgnoreCase)
        {
            { "stackoverflow", new StackOverflowDatasetProvider() },
            { "clinicalwords100d", new ClinicalWordsDatasetProvider(100) },
            { "clinicalwords300d", new ClinicalWordsDatasetProvider(300) },
            { "clinicalwords600d", new ClinicalWordsDatasetProvider(600) },
        };
    }

    /// <summary>
    /// Gets a dataset provider by name, or null if not found.
    /// </summary>
    public IDatasetProvider? GetProvider(string datasetName)
    {
        return _providers.TryGetValue(datasetName, out var provider) ? provider : null;
    }

    /// <summary>
    /// Downloads a dataset file if not already cached.
    /// </summary>
    public async Task<string> DownloadAsync(DatasetFile file, IProgress<double>? progress = null, CancellationToken ct = default)
    {
        var localPath = Path.Combine(_cacheDir, file.FileName);

        // Check if already downloaded
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
        await using var fileStream = new FileStream(localPath, FileMode.Create, FileAccess.Write, FileShare.None, 8192, useAsync: true);

        var buffer = new byte[8192];
        int bytesRead;
        var lastProgressReport = DateTime.UtcNow;

        while ((bytesRead = await contentStream.ReadAsync(buffer, ct)) > 0)
        {
            await fileStream.WriteAsync(buffer.AsMemory(0, bytesRead), ct);
            downloadedBytes += bytesRead;

            // Report progress every 2 seconds
            if (DateTime.UtcNow - lastProgressReport > TimeSpan.FromSeconds(2))
            {
                var progressPct = totalBytes > 0 ? (double)downloadedBytes / totalBytes * 100 : 0;
                Console.WriteLine($"[Dataset] Downloaded {FormatBytes(downloadedBytes)} / {FormatBytes(totalBytes)} ({progressPct:F1}%)");
                progress?.Report(progressPct);
                lastProgressReport = DateTime.UtcNow;
            }
        }

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

        var operation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), dumpFilePath, ct);

        Console.WriteLine($"[Dataset] Import operation started");

        // Wait for operation to complete (Smuggler.ImportAsync returns Operation<SmugglerResult>)
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
            // Download
            var localPath = await DownloadAsync(file, progress: null, ct);

            // Import
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

            // Verify it has the expected collections
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

    public void ClearCache()
    {
        if (Directory.Exists(_cacheDir))
        {
            Console.WriteLine($"[Dataset] Clearing cache directory: {_cacheDir}");
            Directory.Delete(_cacheDir, recursive: true);
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
