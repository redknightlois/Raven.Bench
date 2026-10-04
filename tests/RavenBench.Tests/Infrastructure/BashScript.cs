using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace RavenBench.Tests.Infrastructure;

/// <summary>Runs a bash script for a script-driving test and returns its exit code with stdout followed by stderr.</summary>
public static class BashScript
{
    public static readonly TimeSpan Limit = TimeSpan.FromMinutes(5);

    /// <summary>Drains stdout and stderr concurrently, so neither full pipe blocks the child; a child that outlives the limit is killed and a TimeoutException names the limit.</summary>
    public static (int ExitCode, string Output) Run(string script, IEnumerable<string> arguments, IReadOnlyDictionary<string, string>? environment, bool clearEnvironment = false, TimeSpan? limit = null)
    {
        var startInfo = new ProcessStartInfo("/bin/bash") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false, CreateNoWindow = true };
        startInfo.ArgumentList.Add(script);
        foreach (var argument in arguments)
            startInfo.ArgumentList.Add(argument);
        if (clearEnvironment)
            startInfo.Environment.Clear();
        foreach (var (key, value) in environment ?? new Dictionary<string, string>())
            startInfo.Environment[key] = value;

        using var process = Process.Start(startInfo)!;
        var stdout = process.StandardOutput.ReadToEndAsync();
        var stderr = process.StandardError.ReadToEndAsync();
        var effectiveLimit = limit ?? Limit;
        if (process.WaitForExit(effectiveLimit) == false)
        {
            process.Kill(entireProcessTree: true);
            process.WaitForExit();
            throw new TimeoutException($"'{script}' did not exit within {effectiveLimit} and was killed.");
        }

        process.WaitForExit();
        return (process.ExitCode, stdout.GetAwaiter().GetResult() + stderr.GetAwaiter().GetResult());
    }
}
