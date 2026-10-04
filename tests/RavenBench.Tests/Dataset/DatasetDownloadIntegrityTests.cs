using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using RavenBench.Dataset;
using RavenBench.Dataset.Vectors;
using Xunit;

namespace RavenBench.Tests.Dataset;

[Trait("Category", "Unit")]
public sealed class DatasetDownloadIntegrityTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), $"dataset-download-{Guid.NewGuid():N}");

    public void Dispose()
    {
        if (Directory.Exists(_dir))
            Directory.Delete(_dir, recursive: true);
    }

    // Serves the URL's path as the body; a body whose first read waits on a gate holds that download open.
    private sealed class FakeSource(Func<Uri, Stream> body) : HttpMessageHandler
    {
        public int Requests;

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            Interlocked.Increment(ref Requests);
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK) { Content = new StreamContent(body(request.RequestUri!)) });
        }
    }

    private sealed class GatedStream(byte[] data, Task gate, TaskCompletionSource started) : MemoryStream(data)
    {
        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken ct = default)
        {
            started.TrySetResult();
            await gate;
            return Read(buffer.Span);
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken ct) =>
            ReadAsync(buffer.AsMemory(offset, count), ct).AsTask();
    }

    private static string Sha(string text) => Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(text)));

    private static DatasetFile File(string url, string? sha256) => new() { FileName = "posts.ravendbdump", Url = url, Type = "posts", EstimatedSizeBytes = 1, Sha256 = sha256 };

    [Fact]
    public async Task ACachedFileThatFailsItsHash_IsNotUsed()
    {
        Directory.CreateDirectory(_dir);
        await System.IO.File.WriteAllTextAsync(Path.Combine(_dir, "posts.ravendbdump"), "tampered");
        var source = new FakeSource(_ => new MemoryStream());
        using var manager = new DatasetManager(_dir, source);

        await Assert.ThrowsAsync<DatasetChecksumException>(() => manager.DownloadAsync(File("http://fake/good", Sha("good"))));
    }

    [Fact]
    public async Task AFreshDownloadThatFailsItsHash_IsNotUsed_AndLeavesNoFile()
    {
        var source = new FakeSource(uri => new MemoryStream(Encoding.UTF8.GetBytes(uri.AbsolutePath.Trim('/'))));
        using var manager = new DatasetManager(_dir, source);

        await Assert.ThrowsAsync<DatasetChecksumException>(() => manager.DownloadAsync(File("http://fake/bad", Sha("good"))));
        Assert.Empty(Directory.GetFiles(_dir));

        var path = await manager.DownloadAsync(File("http://fake/good", Sha("good")));
        Assert.Equal("good", await System.IO.File.ReadAllTextAsync(path));
    }

    [Fact]
    public async Task TwoOverlappingDownloads_NeverShareATemporaryPath()
    {
        var gate = new TaskCompletionSource();
        var started = new TaskCompletionSource();
        var source = new FakeSource(uri => uri.AbsolutePath == "/first"
            ? new GatedStream(Encoding.UTF8.GetBytes("first"), gate.Task, started)
            : new MemoryStream(Encoding.UTF8.GetBytes("second")));
        using var first = new DatasetManager(_dir, source);
        using var second = new DatasetManager(_dir, source);

        var held = first.DownloadAsync(File("http://fake/first", sha256: null));
        await Task.WhenAny(started.Task, held);
        Assert.True(started.Task.IsCompleted, "the first download finished without reading its body");
        await second.DownloadAsync(File("http://fake/second", sha256: null));
        gate.SetResult();
        var path = await held;

        Assert.Equal(2, source.Requests);
        Assert.Equal("first", await System.IO.File.ReadAllTextAsync(path));
        Assert.Equal(new[] { path }, Directory.GetFiles(_dir));
    }
}
