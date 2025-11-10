using Spectre.Console.Cli;

namespace RavenBench.Cli;

public sealed class RateSettings : BaseRunSettings
{
    [CommandOption("--rate-workers")]
    public int? RateWorkers { get; set; }
}
