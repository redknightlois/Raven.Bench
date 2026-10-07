using System;
using System.Diagnostics;
using System.IO;
using FluentAssertions;
using RavenBench.Core.Diagnostics;
using Xunit;

namespace RavenBench.Tests.Dataset;

/// <summary>Drives the preparation script's functions under python3 with a stub pandas, so no model or package download is needed.</summary>
[Trait("Category", "Unit")]
public class PrepareClinicalEmbeddingsScriptTests
{
    private static (int ExitCode, string Output) Python(string body)
    {
        var script = Path.Combine(RepositoryRootLocator.Find(), "datasets", "prepare_clinical_embeddings.py");
        var program = $$"""
            import importlib.util, sys, types, pathlib, tempfile
            spec = importlib.util.spec_from_file_location("prepare", r"{{script}}")
            prepare = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(prepare)
            work = pathlib.Path(tempfile.mkdtemp())
            {{body}}
            """;
        var info = new ProcessStartInfo("python3") { RedirectStandardOutput = true, RedirectStandardError = true };
        info.ArgumentList.Add("-c");
        info.ArgumentList.Add(program);
        using var process = Process.Start(info)!;
        var output = process.StandardOutput.ReadToEnd() + process.StandardError.ReadToEnd();
        process.WaitForExit();
        return (process.ExitCode, output);
    }

    [Fact]
    public void A_Digest_Mismatch_Fails_And_Deletes_The_Archive()
    {
        var (exitCode, output) = Python("""
            archive = work / "a.tar.gz"
            archive.write_bytes(b"abc")
            try:
                prepare.verify_archive(archive, "0" * 64)
                raise SystemExit("no error")
            except prepare.ArchiveIntegrityError:
                assert not archive.exists()
            """);

        exitCode.Should().Be(0, output);
    }

    [Fact]
    public void An_Interrupted_Conversion_Leaves_No_File_At_The_Final_Path()
    {
        var (exitCode, output) = Python("""
            class Frame:
                def __init__(self, data): pass
                def to_parquet(self, path, **kwargs):
                    pathlib.Path(path).write_bytes(b"half")
                    raise OSError("disk full")
            sys.modules["pandas"] = types.SimpleNamespace(DataFrame=Frame)
            class Vector(list):
                def tolist(self): return list(self)
            class Wv:
                key_to_index = {"a": 0}
                vector_size = 1
                def __getitem__(self, word): return Vector([1.0])
            target = work / "out.parquet"
            try:
                prepare.embeddings_to_parquet(Wv(), target)
                raise SystemExit("no error")
            except OSError:
                pass
            assert not target.exists()
            assert list(work.iterdir()) == []
            """);

        exitCode.Should().Be(0, output);
    }
}
