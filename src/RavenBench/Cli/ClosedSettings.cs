using System.ComponentModel;
using Spectre.Console.Cli;

namespace RavenBench.Cli;

public sealed class ClosedSettings : BaseRunSettings
{
    // For backward compatibility during transition, keep --concurrency option
    [CommandOption("--concurrency")]
    [Description("Concurrency ramp: start..end or start..endxfactor (e.g., 8..512x2) (default: 8..512x2)")]
    public string Concurrency { get; init; } = "8..512x2";

    [CommandOption("--pipeline-depth")]
    [Description("HTTP/1.1 requests each connection sends before it reads their responses; above 1 needs --transport raw over HTTP/1.1 with identity compression (default: 1)")]
    public int PipelineDepth { get; init; } = 1;
}
