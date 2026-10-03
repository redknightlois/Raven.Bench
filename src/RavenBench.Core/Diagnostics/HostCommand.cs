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

    internal static readonly TimeSpan ExitLimit = TimeSpan.FromSeconds(30);

    internal static Result Run(string fileName, params string[] arguments) => Run(ExitLimit, fileName, arguments);

    /// <summary>A child still running after <paramref name="exitLimit"/> is killed and a <see cref="TimeoutException"/> is thrown.</summary>
    internal static Result Run(TimeSpan exitLimit, string fileName, params string[] arguments)
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

        // Both pipes drain at once: a child that fills one pipe while the other is read never blocks.
        var standardError = process.StandardError.ReadToEndAsync();
        var standardOutput = process.StandardOutput.ReadToEndAsync();
        if (process.WaitForExit(exitLimit) == false)
        {
            process.Kill(entireProcessTree: true);
            throw new TimeoutException($"'{fileName}' did not exit within {exitLimit.TotalSeconds.ToString(System.Globalization.CultureInfo.InvariantCulture)} s and was killed.");
        }
        process.WaitForExit();

        return new Result(process.ExitCode, standardOutput.GetAwaiter().GetResult().Trim(), standardError.GetAwaiter().GetResult().Trim());
    }
}
