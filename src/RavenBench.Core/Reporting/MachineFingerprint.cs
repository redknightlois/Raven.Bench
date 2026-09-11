namespace RavenBench.Core.Reporting;

/// <summary>
/// The machine a run executed on, collected at run start. Results are grouped by this fingerprint
/// and only compared within one, so a laptop run is never read against a cloud run. Every field
/// that is present holds a measured value; the cloud fields are absent when the metadata endpoint
/// does not answer, and <see cref="DatabaseImage"/> is absent when the target did not run in a
/// container.
/// </summary>
public sealed record MachineFingerprint
{
    /// <summary>The CPU model string the operating system reports.</summary>
    public required string CpuModel { get; init; }

    /// <summary>The physical core count the operating system reports.</summary>
    public required int PhysicalCoreCount { get; init; }

    /// <summary>The logical core count the operating system reports.</summary>
    public required int LogicalCoreCount { get; init; }

    /// <summary>True when the logical core count differs from the physical core count.</summary>
    public required bool SmT { get; init; }

    /// <summary>The total physical memory the operating system reports, in bytes.</summary>
    public required long RamBytes { get; init; }

    /// <summary>The operating system name.</summary>
    public required string Os { get; init; }

    /// <summary>The kernel release.</summary>
    public required string Kernel { get; init; }

    /// <summary>The storage device that holds the harness repository.</summary>
    public required string StorageDevice { get; init; }

    /// <summary>The filesystem that holds the harness repository.</summary>
    public required string Filesystem { get; init; }

    /// <summary>The .NET runtime version the harness runs on.</summary>
    public required string DotNetVersion { get; init; }

    /// <summary>The git commit of the harness repository, read at run time.</summary>
    public required string HarnessCommit { get; init; }

    /// <summary>True when the target database ran in a container the benchmark used.</summary>
    public required bool DatabaseInDocker { get; init; }

    /// <summary>The image reference that ran the target database, when it ran in a container.</summary>
    public string? DatabaseImage { get; init; }

    /// <summary>The cloud VM SKU, present only when the cloud metadata endpoint answers.</summary>
    public string? CloudVmSku { get; init; }

    /// <summary>The cloud region, present only when the cloud metadata endpoint answers.</summary>
    public string? CloudRegion { get; init; }
}
