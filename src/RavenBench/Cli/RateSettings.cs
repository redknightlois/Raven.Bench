using Spectre.Console.Cli;
using System.ComponentModel;

namespace RavenBench.Cli;

public sealed class RateSettings : BaseRunSettings
{
    [CommandOption("--rate-workers")]
    public int? RateWorkers { get; set; }
}
