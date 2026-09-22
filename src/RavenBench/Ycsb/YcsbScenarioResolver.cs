using System.Globalization;
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
    /// <summary>The separator a list-valued override uses on the command line, as <c>--vector-recall-ks</c> does.</summary>
    private const char ListSeparator = ',';

    public static YcsbScenario Resolve(YcsbScenario scenario, YcsbSettings settings, string[] commandLineArgs)
    {
        var resolved = scenario with
        {
            Seed = IsExplicit(commandLineArgs, "--seed") ? settings.Seed : scenario.Seed,
            Target = settings.Target ?? scenario.Target,
            DocumentCount = settings.DocumentCount ?? scenario.DocumentCount,
            DocumentSize = IsExplicit(commandLineArgs, "--doc-size") ? settings.DocSize : scenario.DocumentSize,
            Concurrency = settings.Step ?? scenario.Concurrency,
            Distribution = IsExplicit(commandLineArgs, "--distribution") ? settings.Distribution : scenario.Distribution,
            Warmup = IsExplicit(commandLineArgs, "--warmup") ? settings.Warmup : scenario.Warmup,
            Duration = IsExplicit(commandLineArgs, "--duration") ? settings.Duration : scenario.Duration,
            // A rates override replaces both spellings the file may carry, so the resolved scenario
            // never names Rate and Rates at once.
            Rate = settings.Rates == null ? scenario.Rate : null,
            Rates = settings.Rates == null ? scenario.Rates : ParseRates(settings.Rates),
            Distributions = settings.Distributions == null ? scenario.Distributions : SplitList(settings.Distributions),
            Repetitions = settings.Repetitions ?? scenario.Repetitions
        };

        resolved.Validate();
        return resolved;
    }

    private static string[] SplitList(string value) =>
        value.Split(ListSeparator, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

    private static double[] ParseRates(string value) =>
        SplitList(value).Select(part => double.TryParse(part, NumberStyles.Float, CultureInfo.InvariantCulture, out var rate)
            ? rate
            : throw new YcsbScenarioException($"Option '--rates' holds '{part}', which is not a number.")).ToArray();

    internal static bool IsExplicit(string[] args, string optionName) =>
        args.Any(a => a == optionName || a.StartsWith(optionName + "=", StringComparison.Ordinal));
}
