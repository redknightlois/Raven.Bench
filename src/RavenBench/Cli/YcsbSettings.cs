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

    [CommandOption("--rates")]
    [Description("Overrides the scenario's fixed rates, in operations per second: a comma-separated list, or empty for no fixed-rate run.")]
    public string? Rates { get; init; }

    [CommandOption("--distributions")]
    [Description("Overrides the distributions workload C runs under: a comma-separated list of uniform, zipfian and latest.")]
    public string? Distributions { get; init; }

    [CommandOption("--repetitions")]
    [Description("Overrides how many times every workload row is repeated.")]
    public int? Repetitions { get; init; }
}
