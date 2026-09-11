using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using FluentAssertions;
using RavenBench.Core.Diagnostics;
using RavenBench.Core.Reporting;
using Xunit;

namespace RavenBench.Tests.Ycsb;

/// <summary>
/// Pins the fingerprint collector without a live machine and without Docker: every field is read
/// from its own source, a source that returns no value fails the run instead of filling a
/// placeholder, and the serialized keys are the contract's names.
/// </summary>
public class MachineFingerprintCollectorTests
{
    [Fact]
    public void Collect_Reads_Each_Field_From_Its_Own_Source()
    {
        var source = new FakeFingerprintSource
        {
            CpuModel = "Test CPU 9000",
            PhysicalCoreCount = 4,
            LogicalCoreCount = 8,
            RamBytes = 16L * 1024 * 1024 * 1024,
            OsName = "Test OS 1.0",
            KernelRelease = "1.2.3-test",
            StorageDevice = "/dev/test0",
            Filesystem = "testfs",
            DotNetVersion = "10.0.0-test",
            HarnessCommit = "0123456789abcdef0123456789abcdef01234567",
            Cloud = new CloudVmMetadata("D8as_v4", "westus2")
        };

        var fingerprint = new MachineFingerprintCollector(source).Collect(
            "/repo/root",
            new DatabaseContainerInfo { ImageReference = "mongo:8.0", ImageDigest = "sha256:abc" });

        fingerprint.CpuModel.Should().Be("Test CPU 9000");
        fingerprint.PhysicalCoreCount.Should().Be(4);
        fingerprint.LogicalCoreCount.Should().Be(8);
        fingerprint.SmT.Should().BeTrue("the logical count differs from the physical count");
        fingerprint.RamBytes.Should().Be(16L * 1024 * 1024 * 1024);
        fingerprint.Os.Should().Be("Test OS 1.0");
        fingerprint.Kernel.Should().Be("1.2.3-test");
        fingerprint.StorageDevice.Should().Be("/dev/test0");
        fingerprint.Filesystem.Should().Be("testfs");
        fingerprint.DotNetVersion.Should().Be("10.0.0-test");
        fingerprint.HarnessCommit.Should().Be("0123456789abcdef0123456789abcdef01234567");
        fingerprint.DatabaseInDocker.Should().BeTrue();
        fingerprint.DatabaseImage.Should().Be("mongo:8.0");
        fingerprint.CloudVmSku.Should().Be("D8as_v4");
        fingerprint.CloudRegion.Should().Be("westus2");
        source.StorageDeviceRoot.Should().Be("/repo/root", "the storage source is asked about the harness repository");
    }

    [Fact]
    public void Collect_Reports_No_Container_And_No_Cloud_When_Their_Sources_Are_Absent()
    {
        var source = new FakeFingerprintSource();

        var fingerprint = new MachineFingerprintCollector(source).Collect("/repo/root", null);

        fingerprint.DatabaseInDocker.Should().BeFalse();
        fingerprint.DatabaseImage.Should().BeNull();
        fingerprint.CloudVmSku.Should().BeNull();
        fingerprint.CloudRegion.Should().BeNull();
    }

    [Fact]
    public void Collect_Reports_SMT_False_When_The_Counts_Match()
    {
        var source = new FakeFingerprintSource { PhysicalCoreCount = 4, LogicalCoreCount = 4 };

        var fingerprint = new MachineFingerprintCollector(source).Collect("/repo/root", null);

        fingerprint.SmT.Should().BeFalse();
    }

    [Fact]
    public void Collect_Throws_When_A_Required_Source_Returns_No_Value()
    {
        var source = new FakeFingerprintSource { CpuModel = string.Empty };

        var act = () => new MachineFingerprintCollector(source).Collect("/repo/root", null);

        act.Should().Throw<MachineFingerprintException>().WithMessage("*CPU model*");
    }

    [Fact]
    public void Collect_Does_Not_Swallow_A_Failing_Source()
    {
        var source = new FakeFingerprintSource { ThrowOnHarnessCommit = true };

        var act = () => new MachineFingerprintCollector(source).Collect("/repo/root", null);

        act.Should().Throw<InvalidOperationException>().WithMessage("*git*");
    }

    [Fact]
    public void The_Serialized_Fingerprint_Carries_The_Contract_Keys()
    {
        var source = new FakeFingerprintSource { Cloud = new CloudVmMetadata("D16as_v4", "eastus") };
        var fingerprint = new MachineFingerprintCollector(source).Collect(
            "/repo/root",
            new DatabaseContainerInfo { ImageReference = "mongo:8.0", ImageDigest = "sha256:abc" });

        using var document = JsonDocument.Parse(JsonSerializer.Serialize(fingerprint));
        var keys = document.RootElement.EnumerateObject().Select(property => property.Name).ToHashSet(StringComparer.Ordinal);

        keys.Should().BeEquivalentTo(new[]
        {
            "CpuModel", "PhysicalCoreCount", "LogicalCoreCount", "SmT", "RamBytes",
            "Os", "Kernel", "StorageDevice", "Filesystem", "DotNetVersion", "HarnessCommit",
            "DatabaseInDocker", "DatabaseImage", "CloudVmSku", "CloudRegion"
        });
    }

    [Fact]
    public void The_Native_Source_Reads_Every_Required_Field()
    {
        var fingerprint = new MachineFingerprintCollector(new NativeMachineFingerprintSource())
            .Collect(RepositoryRootLocator.Find(), null);

        fingerprint.CpuModel.Should().NotBeNullOrWhiteSpace();
        fingerprint.PhysicalCoreCount.Should().BeGreaterThan(0);
        fingerprint.LogicalCoreCount.Should().BeGreaterThanOrEqualTo(fingerprint.PhysicalCoreCount);
        fingerprint.RamBytes.Should().BeGreaterThan(0);
        fingerprint.Os.Should().NotBeNullOrWhiteSpace();
        fingerprint.Kernel.Should().NotBeNullOrWhiteSpace();
        fingerprint.StorageDevice.Should().NotBeNullOrWhiteSpace();
        fingerprint.Filesystem.Should().NotBeNullOrWhiteSpace();
        fingerprint.DotNetVersion.Should().NotBeNullOrWhiteSpace();
        fingerprint.HarnessCommit.Should().MatchRegex("^[0-9a-f]{40}$", "the harness commit is read from git at run time");
    }

    private sealed class FakeFingerprintSource : IMachineFingerprintSource
    {
        public string CpuModel { get; set; } = "Test CPU";
        public int PhysicalCoreCount { get; set; } = 2;
        public int LogicalCoreCount { get; set; } = 2;
        public long RamBytes { get; set; } = 8L * 1024 * 1024 * 1024;
        public string OsName { get; set; } = "Test OS";
        public string KernelRelease { get; set; } = "0.0-test";
        public string StorageDevice { get; set; } = "/dev/test";
        public string Filesystem { get; set; } = "testfs";
        public string DotNetVersion { get; set; } = "10.0.0";
        public string HarnessCommit { get; set; } = "deadbeef";
        public CloudVmMetadata? Cloud { get; set; }
        public bool ThrowOnHarnessCommit { get; set; }
        public string? StorageDeviceRoot { get; private set; }

        public string GetCpuModel() => CpuModel;
        public int GetPhysicalCoreCount() => PhysicalCoreCount;
        public int GetLogicalCoreCount() => LogicalCoreCount;
        public long GetRamBytes() => RamBytes;
        public string GetOsName() => OsName;
        public string GetKernelRelease() => KernelRelease;

        public string GetStorageDevice(string repositoryRoot)
        {
            StorageDeviceRoot = repositoryRoot;
            return StorageDevice;
        }

        public string GetFilesystem(string repositoryRoot) => Filesystem;
        public string GetDotNetVersion() => DotNetVersion;

        public string GetHarnessCommit(string repositoryRoot) =>
            ThrowOnHarnessCommit ? throw new InvalidOperationException("git failed") : HarnessCommit;

        public CloudVmMetadata? GetCloudVmMetadata() => Cloud;
    }
}
