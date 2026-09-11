using System.ComponentModel;
using Spectre.Console.Cli;

namespace RavenBench.Cli;

public sealed class YcsbSettings : BaseRunSettings
{
    [CommandOption("--scenario")]
    [Description("Path to the ycsb scenario file (JSON). Every parameter it holds can be overridden on the command line.")]
    public string? Scenario { get; init; }

    [CommandOption("--target")]
    [Description("Overrides the scenario's target product.")]
    public string? Target { get; init; }

    [CommandOption("--doc-count")]
    [Description("Overrides the scenario's document count.")]
    public int? DocumentCount { get; init; }
}
