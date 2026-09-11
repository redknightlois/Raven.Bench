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
/// already running. Returns null when the Docker daemon answers but no container maps the port,
/// and throws when the Docker CLI or daemon cannot be read, so a fingerprint never records a
/// placeholder for an unknown container state.
/// </summary>
public sealed class DockerDatabaseContainerLocator
{
    public DatabaseContainerInfo? Locate(int hostPort)
    {
        var containers = RunDocker("ps", "--no-trunc", "--format", "{{.ID}}\t{{.Image}}\t{{.Ports}}");
        if (containers.ExitCode != 0)
            throw new DatabaseContainerLocatorException($"The Docker daemon did not list containers: {Describe(containers)}");

        foreach (var line in containers.StandardOutput.Split('\n', StringSplitOptions.RemoveEmptyEntries))
        {
            var fields = line.Split('\t');
            if (fields.Length < 3)
                continue;

            if (PublishesPort(fields[2], hostPort) == false)
                continue;

            var containerId = fields[0];
            var imageReference = fields[1];
            return new DatabaseContainerInfo
            {
                ImageReference = imageReference,
                ImageDigest = ReadRepoDigest(containerId)
            };
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

    private static string Describe(HostCommand.Result result) =>
        string.IsNullOrWhiteSpace(result.StandardError) ? $"exit code {result.ExitCode}" : result.StandardError;
}
