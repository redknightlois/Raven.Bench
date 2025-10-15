using System.Text.Json;
using System.Text.Json.Serialization;

namespace RavenBench.Reporting;

public static class JsonResultsWriter
{
    private static readonly JsonSerializerOptions Options = new()
    {
        WriteIndented = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    public static void Write(string path, BenchmarkSummary summary)
    {
        var dir = Path.GetDirectoryName(path);
        if (string.IsNullOrEmpty(dir) == false)
            Directory.CreateDirectory(dir);
        using var fs = File.Create(path);
        JsonSerializer.Serialize(fs, summary, Options);
    }
}

