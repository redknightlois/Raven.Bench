using RavenBench.Cli;
using RavenBench.Core.Ycsb;

namespace RavenBench.Ycsb;

/// <summary>
/// Resolves a ycsb scenario against the command line that invoked it: an explicit command-line
/// value wins over the file, and the file wins over nothing. <see cref="BaseRunSettings"/> gives
/// several options a non-null default in the settings object itself, so for those the property
/// alone cannot tell "not given" from "given, and equal to the default" - the raw argv decides.
/// Options this command declares itself are nullable, so their presence is the answer.
/// </summary>
internal static class YcsbScenarioResolver
{
    public static YcsbScenario Resolve(YcsbScenario scenario, YcsbSettings settings, string[] commandLineArgs)
    {
        return scenario with
        {
            Seed = IsExplicit(commandLineArgs, "--seed") ? settings.Seed : scenario.Seed,
            Target = settings.Target ?? scenario.Target,
            DocumentCount = settings.DocumentCount ?? scenario.DocumentCount,
            DocumentSize = IsExplicit(commandLineArgs, "--doc-size") ? settings.DocSize : scenario.DocumentSize,
            Concurrency = settings.Step ?? scenario.Concurrency,
            Distribution = IsExplicit(commandLineArgs, "--distribution") ? settings.Distribution : scenario.Distribution,
            Warmup = IsExplicit(commandLineArgs, "--warmup") ? settings.Warmup : scenario.Warmup,
            Duration = IsExplicit(commandLineArgs, "--duration") ? settings.Duration : scenario.Duration
        };
    }

    internal static bool IsExplicit(string[] args, string optionName) =>
        args.Any(a => a == optionName || a.StartsWith(optionName + "=", StringComparison.Ordinal));
}
