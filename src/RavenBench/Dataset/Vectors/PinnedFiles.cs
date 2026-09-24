using System.Security.Cryptography;
using System.Text.RegularExpressions;

namespace RavenBench.Dataset.Vectors;

/// <summary>
/// Thrown when a dataset file has no pin, when its SHA-256 differs from the pin, or when an operator pin contradicts the catalog.
/// The run stops; the file is neither re-downloaded nor used.
/// </summary>
public sealed class DatasetChecksumException : IOException
{
    public string SetName { get; }
    public string FileName { get; }
    public string? Expected { get; }
    public string? Actual { get; }

    public DatasetChecksumException(string setName, string fileName, string? expected, string? actual, string message)
        : base(message)
    {
        SetName = setName;
        FileName = fileName;
        Expected = expected;
        Actual = actual;
    }
}

/// <summary>
/// The files of one vector set, each verified against its SHA-256 pin. Only <see cref="PinnedFiles"/> creates one.
/// </summary>
public sealed class VerifiedFiles
{
    private readonly IReadOnlyDictionary<string, (string Path, string Sha256)> _files;

    internal VerifiedFiles(string setName, string directory, IReadOnlyDictionary<string, (string Path, string Sha256)> files)
    {
        SetName = setName;
        Directory = directory;
        _files = files;
    }

    public string SetName { get; }

    /// <summary>
    /// The directory the set's files live in; derived artefacts such as the truth cache live beside them.
    /// </summary>
    public string Directory { get; }

    public string PathOf(string fileName) => _files.TryGetValue(fileName, out var f)
        ? f.Path
        : throw new KeyNotFoundException($"Set '{SetName}' pins no file named '{fileName}'.");

    /// <summary>
    /// The pins of every file, in file-name order. Truth caches key on it.
    /// </summary>
    public string Fingerprint => string.Join(",", _files.OrderBy(f => f.Key, StringComparer.Ordinal).Select(f => $"{f.Key}={f.Value.Sha256}"));
}

/// <summary>
/// Fetches each pinned file once into the data directory and verifies every file by SHA-256 on every call.
/// </summary>
public static class PinnedFiles
{
    /// <summary>
    /// Verifies the set's files. For a single-file set the operator may supply <paramref name="sourcePath"/>, a file
    /// placed outside the data directory, and <paramref name="sha256"/>, the pin of a file the catalog does not pin.
    /// A catalog pin is never replaced.
    /// </summary>
    public static async Task<VerifiedFiles> EnsureAsync(IVectorDataset set, string dataDirectory, string? sourcePath = null, string? sha256 = null, HttpClient? http = null, CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(dataDirectory))
            throw new ArgumentException("A data directory is required.", nameof(dataDirectory));
        if ((sourcePath != null || sha256 != null) && set.Files.Count != 1)
            throw new ArgumentException($"Set '{set.Name}' has {set.Files.Count} files; a source file or an operator pin applies to a single-file set only.");
        if (sha256 != null && Regex.IsMatch(sha256, "^[0-9a-fA-F]{64}$") == false)
            throw new ArgumentException($"'{sha256}' is not a hex SHA-256.", nameof(sha256));

        var setDirectory = Path.Combine(dataDirectory, set.Name);
        Directory.CreateDirectory(setDirectory);
        var files = new Dictionary<string, (string, string)>(StringComparer.Ordinal);
        foreach (var file in set.Files)
        {
            if (sha256 != null && file.Sha256 != null && string.Equals(sha256, file.Sha256, StringComparison.OrdinalIgnoreCase) == false)
                throw new DatasetChecksumException(set.Name, file.FileName, file.Sha256, sha256.ToLowerInvariant(),
                    $"Set '{set.Name}' file '{file.FileName}' is pinned to {file.Sha256}; the operator pin {sha256} cannot replace it.");

            var pin = (file.Sha256 ?? sha256)?.ToLowerInvariant();
            if (pin == null)
                throw new DatasetChecksumException(set.Name, file.FileName, null, null,
                    $"Set '{set.Name}' file '{file.FileName}' has no pinned SHA-256; pass its SHA-256 with --dataset-sha256.");

            var path = sourcePath ?? Path.Combine(setDirectory, file.FileName);
            if (File.Exists(path) == false)
            {
                if (sourcePath != null)
                    throw new FileNotFoundException($"Set '{set.Name}' source file '{sourcePath}' does not exist.", sourcePath);
                await DownloadAsync(set.Name, file, pin, path, http, ct).ConfigureAwait(false);
            }

            await VerifyAsync(set.Name, file.FileName, pin, path, ct).ConfigureAwait(false);
            files[file.FileName] = (path, pin);
        }
        return new VerifiedFiles(set.Name, setDirectory, files);
    }

    public static async Task VerifyAsync(string setName, string fileName, string expected, string path, CancellationToken ct = default)
    {
        var actual = await Sha256Async(path, ct).ConfigureAwait(false);
        if (string.Equals(actual, expected, StringComparison.OrdinalIgnoreCase) == false)
            throw new DatasetChecksumException(setName, fileName, expected, actual,
                $"Set '{setName}' file '{fileName}' failed SHA-256 verification: expected {expected}, actual {actual} ({path}). Delete the file to fetch it again.");
    }

    public static async Task<string> Sha256Async(string path, CancellationToken ct = default)
    {
        await using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 1 << 20, useAsync: true);
        return Convert.ToHexStringLower(await SHA256.HashDataAsync(stream, ct).ConfigureAwait(false));
    }

    // A partial download stays under a temporary name unique to this writer, so a file under the final name is always a complete download.
    private static async Task DownloadAsync(string setName, DatasetFile file, string pin, string path, HttpClient? http, CancellationToken ct)
    {
        if (Uri.TryCreate(file.Url, UriKind.Absolute, out var uri) == false || (uri.Scheme != Uri.UriSchemeHttp && uri.Scheme != Uri.UriSchemeHttps))
            throw new InvalidOperationException($"Set '{setName}' file '{file.FileName}' has no fetchable URL. Place the file at {path} or pass it with --dataset-source. {file.Description}".TrimEnd());

        var temp = $"{path}.{Guid.NewGuid():N}.downloading";
        Console.WriteLine($"[Dataset] {setName}: fetching {file.FileName} from {uri}");

        var owned = http == null;
        http ??= new HttpClient { Timeout = Timeout.InfiniteTimeSpan };
        try
        {
            using var response = await http.GetAsync(uri, HttpCompletionOption.ResponseHeadersRead, ct).ConfigureAwait(false);
            if (response.IsSuccessStatusCode == false)
                throw new HttpRequestException($"Set '{setName}' file '{file.FileName}': {uri} answered {(int)response.StatusCode}. Place the file at {path}.", null, response.StatusCode);
            await using (var source = await response.Content.ReadAsStreamAsync(ct).ConfigureAwait(false))
            await using (var target = new FileStream(temp, FileMode.Create, FileAccess.Write, FileShare.None, 1 << 20, useAsync: true))
                await source.CopyToAsync(target, ct).ConfigureAwait(false);

            await VerifyAsync(setName, file.FileName, pin, temp, ct).ConfigureAwait(false);
            try
            {
                File.Move(temp, path);
            }
            catch (IOException) when (File.Exists(path))
            {
                // A concurrent run placed the file first; the caller verifies that file.
            }
        }
        finally
        {
            File.Delete(temp);
            if (owned)
                http.Dispose();
        }
    }
}
