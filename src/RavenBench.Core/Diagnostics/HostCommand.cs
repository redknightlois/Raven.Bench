using System.Diagnostics;

namespace RavenBench.Core.Diagnostics;

/// <summary>
/// Runs a host executable and captures its output. A missing executable surfaces as the
/// <see cref="System.ComponentModel.Win32Exception"/> the runtime raises, so a caller that treats
/// the tool as required fails fast rather than reading an empty value.
/// </summary>
internal static class HostCommand
{
    internal readonly record struct Result(int ExitCode, string StandardOutput, string StandardError);

    internal static Result Run(string fileName, params string[] arguments)
    {
        var startInfo = new ProcessStartInfo(fileName)
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true
        };

        foreach (var argument in arguments)
            startInfo.ArgumentList.Add(argument);

        using var process = Process.Start(startInfo)
            ?? throw new InvalidOperationException($"Failed to start '{fileName}'.");

        var standardOutput = process.StandardOutput.ReadToEnd();
        var standardError = process.StandardError.ReadToEnd();
        process.WaitForExit();

        return new Result(process.ExitCode, standardOutput.Trim(), standardError.Trim());
    }
}
