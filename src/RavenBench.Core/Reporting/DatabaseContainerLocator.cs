using System.Globalization;
using System.Net;
using System.Net.NetworkInformation;
using System.Text.Json;
using RavenBench.Core.Diagnostics;

namespace RavenBench.Core.Reporting;

/// <summary>
/// The container image that served a database target. The reference is the image name the
/// container was created from; the digest is the immutable repo digest of that image, so a reader
/// can rerun the row even when the reference uses a moving tag.
/// </summary>
public sealed record DatabaseContainerInfo
{
    public required string ImageReference { get; init; }
    public required string ImageDigest { get; init; }
}

/// <summary>
/// A Docker command failed or the container that publishes the port has no repo digest. The
/// run cannot record a truthful image for its target, so it fails rather than writing nothing.
/// </summary>
public sealed class DatabaseContainerLocatorException : Exception
{
    public DatabaseContainerLocatorException(string message) : base(message)
    {
    }

    public DatabaseContainerLocatorException(string message, Exception innerException) : base(message, innerException)
    {
    }
}

/// <summary>
/// Locates a database container through the Docker CLI. The container is matched by the host port
/// it publishes, so it does not matter whether the benchmark script started it or found it
/// already running. Returns null when the Docker CLI or daemon is not usable, or when the daemon
/// answers but no container maps the port: a client with no Docker records no image instead of
/// failing, and never invents one. Throws when a container that does map the port has an
/// unreadable image or repo digest, so a run never records a half-read image.
/// </summary>
public sealed class DockerDatabaseContainerLocator
{
    /// <summary>
    /// True when the endpoint names this machine: a loopback host, the machine's own host name, or
    /// an address of one of its interfaces. Only such an endpoint can be served by a container the
    /// local daemon publishes; any other host name is not resolved and counts as remote.
    /// </summary>
    public static bool IsLocalEndpoint(Uri endpoint) =>
        endpoint.IsLoopback
        || string.Equals(endpoint.IdnHost, Dns.GetHostName(), StringComparison.OrdinalIgnoreCase)
        || (IPAddress.TryParse(endpoint.IdnHost, out var address)
            && NetworkInterface.GetAllNetworkInterfaces().SelectMany(n => n.GetIPProperties().UnicastAddresses).Any(u => u.Address.Equals(address)));

    public DatabaseContainerInfo? Locate(int hostPort) =>
        Find(hostPort) is { } found ? new DatabaseContainerInfo { ImageReference = found.Image, ImageDigest = ReadRepoDigest(found.Id) } : null;

    /// <summary>The id of the running container that publishes the host port, or null when Docker is not usable or no container maps it.</summary>
    public string? LocateId(int hostPort) => Find(hostPort)?.Id;

    /// <summary>
    /// Sets the container's memory and memory-plus-swap limits and restarts it, so the server sizes itself under them.
    /// A memory-plus-swap equal to the memory keeps swap from softening the limit; -1 lifts the swap limit.
    /// Docker refuses a memory above the swap limit in force when the same update lifts the swap limit, so a lift first
    /// sets both to the new memory.
    /// </summary>
    public void LimitMemoryAndRestart(string containerId, long memory, long memorySwap)
    {
        var commands = new List<string[]>();
        if (memorySwap == -1)
            commands.Add(["update", "--memory", Invariant(memory), "--memory-swap", Invariant(memory), containerId]);
        commands.Add(["update", "--memory", Invariant(memory), "--memory-swap", Invariant(memorySwap), containerId]);
        commands.Add(["restart", containerId]);
        foreach (var command in commands)
        {
            var result = RunDocker(command);
            if (result.ExitCode != 0)
                throw new DatabaseContainerLocatorException($"docker {string.Join(' ', command)} failed: {Describe(result)}");
        }
    }

    /// <summary>The memory and memory-plus-swap limits the container reports; 0 means no limit.</summary>
    public (long Memory, long MemorySwap) ReadMemoryLimits(string containerId)
    {
        var inspect = RunDocker("inspect", "--format", "{{.HostConfig.Memory}} {{.HostConfig.MemorySwap}}", containerId);
        var fields = inspect.StandardOutput.Split(' ');
        if (inspect.ExitCode != 0 || fields.Length != 2 || long.TryParse(fields[0], CultureInfo.InvariantCulture, out var memory) == false || long.TryParse(fields[1], CultureInfo.InvariantCulture, out var swap) == false)
            throw new DatabaseContainerLocatorException($"Could not read the memory limit of container '{containerId}': {Describe(inspect)}");
        return (memory, swap);
    }

    /// <summary>The total memory of the Docker host, as <c>docker info</c> reports it.</summary>
    public long ReadHostMemory()
    {
        var info = RunDocker("info", "--format", "{{.MemTotal}}");
        if (info.ExitCode != 0 || long.TryParse(info.StandardOutput.Trim(), CultureInfo.InvariantCulture, out var total) == false)
            throw new DatabaseContainerLocatorException($"Could not read the Docker host memory: {Describe(info)}");
        return total;
    }

    private static string Invariant(long value) => value.ToString(System.Globalization.CultureInfo.InvariantCulture);

    private (string Id, string Image)? Find(int hostPort)
    {
        var containers = TryRunDocker("ps", "--no-trunc", "--format", "{{.ID}}\t{{.Image}}\t{{.Ports}}");
        if (containers == null || containers.Value.ExitCode != 0)
            return null;

        foreach (var line in containers.Value.StandardOutput.Split('\n', StringSplitOptions.RemoveEmptyEntries))
        {
            var fields = line.Split('\t');
            if (fields.Length >= 3 && PublishesPort(fields[2], hostPort))
                return (fields[0], fields[1]);
        }

        return null;
    }

    private static bool PublishesPort(string ports, int hostPort) =>
        ports.Contains($":{hostPort}->", StringComparison.Ordinal);

    private static string ReadRepoDigest(string containerId)
    {
        var imageId = RunDocker("inspect", "--format", "{{.Image}}", containerId);
        if (imageId.ExitCode != 0 || string.IsNullOrWhiteSpace(imageId.StandardOutput))
            throw new DatabaseContainerLocatorException($"Could not read the image of container '{containerId}': {Describe(imageId)}");

        var digests = RunDocker("image", "inspect", "--format", "{{json .RepoDigests}}", imageId.StandardOutput);
        if (digests.ExitCode != 0)
            throw new DatabaseContainerLocatorException($"Could not read the repo digest of image '{imageId.StandardOutput}': {Describe(digests)}");

        return ExtractRepoDigest(digests.StandardOutput, imageId.StandardOutput);
    }

    // The recorded value is the full repo digest, <repository>@<algorithm>:<hash>, so it names the
    // digest's repository as well as its hash and matches what `docker image inspect` reports.
    private static string ExtractRepoDigest(string repoDigestsJson, string imageId)
    {
        string[]? digests;
        try
        {
            digests = JsonSerializer.Deserialize<string[]>(repoDigestsJson);
        }
        catch (JsonException ex)
        {
            throw new DatabaseContainerLocatorException($"Image '{imageId}' reported an unreadable repo digest.", ex);
        }

        if (digests == null || digests.Length == 0)
            throw new DatabaseContainerLocatorException($"Image '{imageId}' has no repo digest, so the run cannot record the image that served it.");

        var repoDigest = digests[0];
        var at = repoDigest.LastIndexOf('@');
        if (at < 0 || at == repoDigest.Length - 1)
            throw new DatabaseContainerLocatorException($"Image '{imageId}' reported the repo digest '{repoDigest}' without a digest component.");

        return repoDigest;
    }

    private static HostCommand.Result RunDocker(params string[] arguments)
    {
        try
        {
            return HostCommand.Run("docker", arguments);
        }
        catch (System.ComponentModel.Win32Exception ex)
        {
            throw new DatabaseContainerLocatorException("The Docker CLI is not available, so the run cannot identify the container that serves its target.", ex);
        }
    }

    // Docker is optional on the client. A CLI that cannot be started and a daemon that does not
    // answer are both "no readable container", not a run failure: the result records no image.
    private static HostCommand.Result? TryRunDocker(params string[] arguments)
    {
        try
        {
            return HostCommand.Run("docker", arguments);
        }
        catch (System.ComponentModel.Win32Exception)
        {
            return null;
        }
    }

    private static string Describe(HostCommand.Result result) =>
        string.IsNullOrWhiteSpace(result.StandardError) ? $"exit code {result.ExitCode}" : result.StandardError;
}

/// <summary>
/// Limits one container's memory for the length of a run. Disposing restores the limits the container had,
/// or the Docker host's total memory when it had none, because Docker cannot lift a memory limit once set.
/// </summary>
public sealed class ContainerMemoryLimit(string containerId) : IDisposable
{
    private readonly DockerDatabaseContainerLocator _docker = new();
    private (long Memory, long MemorySwap)? _original;

    public string ContainerId => containerId;

    /// <summary>Sets memory and memory-plus-swap to the same value, restarts the container, and returns the limits it reports.</summary>
    public (long Memory, long MemorySwap) Apply(long bytes)
    {
        _original ??= _docker.ReadMemoryLimits(containerId);
        _docker.LimitMemoryAndRestart(containerId, bytes, bytes);
        return _docker.ReadMemoryLimits(containerId);
    }

    public void Dispose()
    {
        if (_original is not { } original)
            return;
        if (original.Memory > 0)
            _docker.LimitMemoryAndRestart(containerId, original.Memory, original.MemorySwap != 0 ? original.MemorySwap : -1);
        else
            _docker.LimitMemoryAndRestart(containerId, _docker.ReadHostMemory(), -1);
    }
}

