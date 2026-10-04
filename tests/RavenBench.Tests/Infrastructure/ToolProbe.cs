using System;
using System.ComponentModel;
using System.Diagnostics;

namespace RavenBench.Tests.Infrastructure;

/// <summary>Runs a tool during test discovery and turns its outcome into a skip reason; null means the tool ran and exited with code 0.</summary>
public static class ToolProbe
{
    public static readonly TimeSpan Limit = TimeSpan.FromSeconds(10);

    public static string? SkipReason(string fileName, string arguments, string missingReason) =>
        SkipReason(fileName, arguments, missingReason, Limit);

    /// <summary>Discards the redirected output as it arrives, so a full pipe cannot block the child, and kills a child that outlives the limit.</summary>
    internal static string? SkipReason(string fileName, string arguments, string missingReason, TimeSpan limit)
    {
        Process process;
        try
        {
            process = Process.Start(new ProcessStartInfo(fileName, arguments) { RedirectStandardOutput = true, RedirectStandardError = true })!;
        }
        catch (Win32Exception)
        {
            return missingReason;
        }

        using (process)
        {
            process.OutputDataReceived += (_, _) => { };
            process.ErrorDataReceived += (_, _) => { };
            process.BeginOutputReadLine();
            process.BeginErrorReadLine();

            if (process.WaitForExit(limit) == false)
            {
                process.Kill(entireProcessTree: true);
                process.WaitForExit();
                return $"'{fileName} {arguments}' did not exit within {limit.TotalSeconds:0.###} s.";
            }

            process.WaitForExit();
            return process.ExitCode == 0 ? null : $"'{fileName} {arguments}' exited with code {process.ExitCode}.";
        }
    }
}
