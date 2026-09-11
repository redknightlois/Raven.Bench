using System.Globalization;
using System.Runtime.InteropServices;
using System.Text.Json;
using RavenBench.Core.Reporting;

namespace RavenBench.Core.Diagnostics;

/// <summary>
/// A fingerprint source could not provide a value the machine is expected to report. The run
/// fails rather than recording an empty string or a placeholder such as "unknown".
/// </summary>
public sealed class MachineFingerprintException : Exception
{
    public MachineFingerprintException(string message) : base(message)
    {
    }

    public MachineFingerprintException(string message, Exception innerException) : base(message, innerException)
    {
    }
}

/// <summary>The cloud VM identity, present only when the metadata endpoint answers.</summary>
public sealed record CloudVmMetadata(string VmSku, string Region);

/// <summary>
/// The machine facts a fingerprint is built from. One method per fact so a test can vary or fail a
/// single source without a live machine.
/// </summary>
public interface IMachineFingerprintSource
{
    string GetCpuModel();
    int GetPhysicalCoreCount();
    int GetLogicalCoreCount();
    long GetRamBytes();
    string GetOsName();
    string GetKernelRelease();
    string GetStorageDevice(string repositoryRoot);
    string GetFilesystem(string repositoryRoot);
    string GetDotNetVersion();
    string GetHarnessCommit(string repositoryRoot);
    CloudVmMetadata? GetCloudVmMetadata();
}

/// <summary>
/// Builds a <see cref="MachineFingerprint"/> from a source. A required fact that comes back empty
/// is a failure, not a placeholder; a value the machine does not expose (cloud metadata) is absent.
/// </summary>
public sealed class MachineFingerprintCollector
{
    private readonly IMachineFingerprintSource _source;

    public MachineFingerprintCollector(IMachineFingerprintSource source)
    {
        _source = source;
    }

    public MachineFingerprint Collect(string repositoryRoot, DatabaseContainerInfo? databaseContainer)
    {
        var physicalCoreCount = _source.GetPhysicalCoreCount();
        var logicalCoreCount = _source.GetLogicalCoreCount();
        if (physicalCoreCount < 1)
            throw new MachineFingerprintException($"The fingerprint source for the physical core count returned {physicalCoreCount}.");
        if (logicalCoreCount < 1)
            throw new MachineFingerprintException($"The fingerprint source for the logical core count returned {logicalCoreCount}.");

        var ramBytes = _source.GetRamBytes();
        if (ramBytes < 1)
            throw new MachineFingerprintException($"The fingerprint source for the RAM size returned {ramBytes} bytes.");

        var cloud = _source.GetCloudVmMetadata();

        return new MachineFingerprint
        {
            CpuModel = Require(_source.GetCpuModel(), "CPU model"),
            PhysicalCoreCount = physicalCoreCount,
            LogicalCoreCount = logicalCoreCount,
            SmT = logicalCoreCount != physicalCoreCount,
            RamBytes = ramBytes,
            Os = Require(_source.GetOsName(), "OS name"),
            Kernel = Require(_source.GetKernelRelease(), "kernel release"),
            StorageDevice = Require(_source.GetStorageDevice(repositoryRoot), "storage device"),
            Filesystem = Require(_source.GetFilesystem(repositoryRoot), "filesystem"),
            DotNetVersion = Require(_source.GetDotNetVersion(), ".NET runtime version"),
            HarnessCommit = Require(_source.GetHarnessCommit(repositoryRoot), "harness commit"),
            DatabaseInDocker = databaseContainer != null,
            DatabaseImage = databaseContainer?.ImageReference,
            CloudVmSku = cloud?.VmSku,
            CloudRegion = cloud?.Region
        };
    }

    private static string Require(string? value, string fieldName) =>
        string.IsNullOrWhiteSpace(value)
            ? throw new MachineFingerprintException($"The fingerprint source for the {fieldName} returned no value.")
            : value;
}

/// <summary>
/// Locates the harness repository root by walking up from the running assembly until a
/// <c>.git</c> entry is found.
/// </summary>
public static class RepositoryRootLocator
{
    /// <summary>The absolute path of the repository root. Throws when no <c>.git</c> entry is found.</summary>
    public static string Find()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory != null)
        {
            var gitPath = Path.Combine(directory.FullName, ".git");
            if (Directory.Exists(gitPath) || File.Exists(gitPath))
                return directory.FullName;

            directory = directory.Parent;
        }

        throw new MachineFingerprintException("The harness repository root (.git) was not found above the running assembly.");
    }
}

/// <summary>
/// Reads the machine facts from the operating system. Linux is the benchmark's environment and
/// reads from the kernel's <c>/proc</c> and <c>/etc</c> files; other platforms use the process
/// commands their operating system provides. A host command that cannot be started surfaces as a
/// failure, never as an empty value.
/// </summary>
public sealed class NativeMachineFingerprintSource : IMachineFingerprintSource
{
    private static bool IsLinux => RuntimeInformation.IsOSPlatform(OSPlatform.Linux);
    private static bool IsMacOs => RuntimeInformation.IsOSPlatform(OSPlatform.OSX);

    public string GetCpuModel()
    {
        if (IsLinux)
        {
            var model = ReadCpuInfoValue("model name") ?? ReadCpuInfoValue("Model") ?? ReadCpuInfoValue("Hardware");
            if (string.IsNullOrWhiteSpace(model) == false)
                return model;
        }
        else if (IsMacOs)
        {
            var model = TryRun("sysctl", "-n", "machdep.cpu.brand_string");
            if (string.IsNullOrWhiteSpace(model) == false)
                return model;
        }
        else
        {
            var identifier = Environment.GetEnvironmentVariable("PROCESSOR_IDENTIFIER");
            if (string.IsNullOrWhiteSpace(identifier) == false)
                return identifier;
        }

        throw new MachineFingerprintException("The operating system did not report a CPU model.");
    }

    public int GetPhysicalCoreCount()
    {
        if (IsLinux)
        {
            var physicalCores = CountPhysicalCoresLinux();
            if (physicalCores > 0)
                return physicalCores;
        }
        else if (IsMacOs)
        {
            if (int.TryParse(TryRun("sysctl", "-n", "hw.physicalcpu"), NumberStyles.Integer, CultureInfo.InvariantCulture, out var physical))
                return physical;
        }

        // The operating system exposes only the logical count here; SMT is then reported false.
        return Environment.ProcessorCount;
    }

    public int GetLogicalCoreCount() => Environment.ProcessorCount;

    public long GetRamBytes()
    {
        if (IsLinux)
        {
            foreach (var line in ReadLines("/proc/meminfo"))
            {
                if (line.StartsWith("MemTotal:", StringComparison.Ordinal) &&
                    TryParseFirstNumber(line, out var kilobytes))
                    return kilobytes * 1024L;
            }
        }
        else if (IsMacOs)
        {
            if (long.TryParse(TryRun("sysctl", "-n", "hw.memsize"), NumberStyles.Integer, CultureInfo.InvariantCulture, out var bytes))
                return bytes;
        }

        var available = GC.GetGCMemoryInfo().TotalAvailableMemoryBytes;
        if (available > 0)
            return available;

        throw new MachineFingerprintException("The operating system did not report the total physical memory.");
    }

    public string GetOsName()
    {
        if (IsLinux)
        {
            foreach (var line in ReadLines("/etc/os-release"))
            {
                if (line.StartsWith("PRETTY_NAME=", StringComparison.Ordinal))
                    return line["PRETTY_NAME=".Length..].Trim().Trim('"');
            }
        }

        return RuntimeInformation.OSDescription;
    }

    public string GetKernelRelease()
    {
        if (IsLinux)
        {
            var release = ReadFirstLine("/proc/sys/kernel/osrelease");
            if (string.IsNullOrWhiteSpace(release) == false)
                return release;
        }

        return Environment.OSVersion.Version.ToString();
    }

    public string GetStorageDevice(string repositoryRoot)
    {
        if (IsLinux)
        {
            var mount = FindMountForPath(repositoryRoot);
            if (mount != null)
                return mount.Value.Device;
        }
        else
        {
            var drive = new DriveInfo(Path.GetPathRoot(Path.GetFullPath(repositoryRoot))!);
            return drive.Name;
        }

        throw new MachineFingerprintException($"The mount table has no entry for the harness repository at '{repositoryRoot}'.");
    }

    public string GetFilesystem(string repositoryRoot)
    {
        if (IsLinux)
        {
            var mount = FindMountForPath(repositoryRoot);
            if (mount != null)
                return mount.Value.Filesystem;
        }
        else
        {
            var drive = new DriveInfo(Path.GetPathRoot(Path.GetFullPath(repositoryRoot))!);
            return drive.DriveFormat;
        }

        throw new MachineFingerprintException($"The mount table has no filesystem for the harness repository at '{repositoryRoot}'.");
    }

    public string GetDotNetVersion() => Environment.Version.ToString();

    public string GetHarnessCommit(string repositoryRoot)
    {
        var result = HostCommand.Run("git", "-C", repositoryRoot, "rev-parse", "HEAD");
        if (result.ExitCode != 0 || string.IsNullOrWhiteSpace(result.StandardOutput))
            throw new MachineFingerprintException($"The harness commit could not be read from '{repositoryRoot}': {result.StandardError}");

        return result.StandardOutput;
    }

    public CloudVmMetadata? GetCloudVmMetadata()
    {
        // The metadata endpoint answers only inside a cloud VM. A non-answer is not an error: the
        // cloud fields are absent on every other machine.
        using var client = new HttpClient { Timeout = TimeSpan.FromMilliseconds(500) };
        try
        {
            var token = ReadCloudIdentityDocument(client, useToken: true) ?? ReadCloudIdentityDocument(client, useToken: false);
            if (token == null)
                return null;

            using var document = JsonDocument.Parse(token);
            var root = document.RootElement;
            var vmSku = root.TryGetProperty("instanceType", out var sku) ? sku.GetString() : null;
            var region = root.TryGetProperty("region", out var regionElement) ? regionElement.GetString() : null;
            if (string.IsNullOrWhiteSpace(vmSku) || string.IsNullOrWhiteSpace(region))
                return null;

            return new CloudVmMetadata(vmSku, region);
        }
        catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException or JsonException)
        {
            return null;
        }
    }

    private static string? ReadCloudIdentityDocument(HttpClient client, bool useToken)
    {
        const string url = "http://169.254.169.254/latest/dynamic/instance-identity/document";
        using var request = new HttpRequestMessage(HttpMethod.Get, url);

        if (useToken)
        {
            try
            {
                using var tokenRequest = new HttpRequestMessage(HttpMethod.Put, "http://169.254.169.254/latest/api/token");
                tokenRequest.Headers.Add("X-aws-ec2-metadata-token-ttl-seconds", "60");
                using var tokenResponse = client.Send(tokenRequest);
                if (tokenResponse.IsSuccessStatusCode == false)
                    return null;

                request.Headers.Add("X-aws-ec2-metadata-token", tokenResponse.Content.ReadAsStringAsync().GetAwaiter().GetResult());
            }
            catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException)
            {
                return null;
            }
        }

        using var response = client.Send(request);
        if (response.IsSuccessStatusCode == false)
            return null;

        return response.Content.ReadAsStringAsync().GetAwaiter().GetResult();
    }

    private static int CountPhysicalCoresLinux()
    {
        var pairs = new HashSet<string>(StringComparer.Ordinal);
        string? physicalId = null;
        string? coreId = null;

        foreach (var line in ReadLines("/proc/cpuinfo"))
        {
            if (line.StartsWith("physical id", StringComparison.Ordinal))
                physicalId = ValueAfterColon(line);
            else if (line.StartsWith("core id", StringComparison.Ordinal))
                coreId = ValueAfterColon(line);

            if (line.Length == 0 && physicalId != null && coreId != null)
            {
                pairs.Add(physicalId + ":" + coreId);
                physicalId = null;
                coreId = null;
            }
        }

        if (physicalId != null && coreId != null)
            pairs.Add(physicalId + ":" + coreId);

        return pairs.Count;
    }

    private static string? ReadCpuInfoValue(string key)
    {
        foreach (var line in ReadLines("/proc/cpuinfo"))
        {
            // The kernel writes the value after an optional tab, so the colon does not follow the key directly.
            if (line.StartsWith(key, StringComparison.Ordinal) && line.Length > key.Length &&
                (line[key.Length] == ':' || char.IsWhiteSpace(line[key.Length])))
                return ValueAfterColon(line);
        }

        return null;
    }

    private static string ValueAfterColon(string line)
    {
        var colon = line.IndexOf(':');
        return colon < 0 ? string.Empty : line[(colon + 1)..].Trim();
    }

    private static bool TryParseFirstNumber(string line, out long value)
    {
        var start = -1;
        for (var i = 0; i < line.Length; i++)
        {
            if (char.IsAsciiDigit(line[i]))
            {
                start = i;
                break;
            }
        }

        value = 0;
        if (start < 0)
            return false;

        var end = start;
        while (end < line.Length && char.IsAsciiDigit(line[end]))
            end++;

        return long.TryParse(line.AsSpan(start, end - start), NumberStyles.Integer, CultureInfo.InvariantCulture, out value);
    }

    private static (string Device, string Filesystem)? FindMountForPath(string path)
    {
        var fullPath = Path.GetFullPath(path);
        (string Device, string Filesystem)? best = null;
        var bestLength = -1;

        foreach (var line in ReadLines("/proc/mounts"))
        {
            var fields = line.Split(' ');
            if (fields.Length < 3)
                continue;

            var mountPoint = UnescapeMountField(fields[1]);
            if (fullPath.Equals(mountPoint, StringComparison.Ordinal) ||
                (fullPath.StartsWith(mountPoint.TrimEnd('/') + "/", StringComparison.Ordinal)))
            {
                if (mountPoint.Length > bestLength)
                {
                    bestLength = mountPoint.Length;
                    best = (UnescapeMountField(fields[0]), fields[2]);
                }
            }
        }

        return best;
    }

    private static string UnescapeMountField(string value) =>
        value.Replace("\\040", " ", StringComparison.Ordinal)
             .Replace("\\011", "\t", StringComparison.Ordinal)
             .Replace("\\012", "\n", StringComparison.Ordinal)
             .Replace("\\134", "\\", StringComparison.Ordinal);

    private static string TryRun(string fileName, params string[] arguments)
    {
        try
        {
            var result = HostCommand.Run(fileName, arguments);
            return result.ExitCode == 0 ? result.StandardOutput : string.Empty;
        }
        catch (System.ComponentModel.Win32Exception)
        {
            return string.Empty;
        }
    }

    private static IEnumerable<string> ReadLines(string path)
    {
        try
        {
            return File.ReadAllLines(path);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            return Array.Empty<string>();
        }
    }

    private static string? ReadFirstLine(string path)
    {
        try
        {
            foreach (var line in File.ReadLines(path))
                return line.Trim();
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            return null;
        }

        return null;
    }
}
