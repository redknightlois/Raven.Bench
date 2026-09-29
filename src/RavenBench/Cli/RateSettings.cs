using Spectre.Console.Cli;
using System.ComponentModel;

namespace RavenBench.Cli;

public sealed class RateSettings : BaseRunSettings
{
    [CommandOption("--rate-workers")]
    public int? RateWorkers { get; set; }

    [CommandOption("--pipeline-depth")]
    [Description("HTTP/1.1 requests each connection sends before it reads their responses; above 1 needs --transport raw over HTTP/1.1 with identity compression (default: 1)")]
    public int PipelineDepth { get; init; } = 1;
}
