using System.Text.Json;
using RavenBench.Core;
using RavenBench.Core.Transport;
using RavenBench.Core.Ycsb;
using RavenBench.Reporting;
using RavenBench.Ycsb;
using Spectre.Console;
using Spectre.Console.Cli;

namespace RavenBench.Cli;

/// <summary>
/// The Npgsql cross-check: ycsb workload C against PostgreSQL through <c>raw</c> (Apex.PgClient) and
/// through <c>client</c> (Npgsql), with one scenario, one seed and one loaded keyspace. The raw pass
/// loads the keyspace and runs every closed-loop workload C row; the client pass runs the same rows
/// against the same table. Each row's medians are compared against the run-to-run noise of the
/// repetitions, under <see cref="YcsbCrossCheck.NoiseRule"/>.
/// </summary>
public sealed class YcsbCrossCheckCommand : AsyncCommand<YcsbSettings>
{
    /// <summary>The mode the cross-check measures against.</summary>
    internal const TransportKind ReferenceMode = TransportKind.Raw;

    /// <summary>The mode the cross-check measures.</summary>
    internal const TransportKind CandidateMode = TransportKind.Client;

    public override async Task<int> ExecuteAsync(CommandContext context, YcsbSettings settings)
    {
        if (string.IsNullOrWhiteSpace(settings.Scenario))
            throw new ArgumentException("--scenario is required");

        var scenario = YcsbScenario.Load(settings.Scenario);
        var resolved = YcsbScenarioResolver.Resolve(scenario, settings, Environment.GetCommandLineArgs());
        if (string.Equals(resolved.Target, PostgresYcsbTransport.Target, StringComparison.OrdinalIgnoreCase) == false)
            throw new YcsbScenarioException($"The cross-check compares the PostgreSQL transports; target '{resolved.Target}' is not '{PostgresYcsbTransport.Target}'.");

        var reference = await new YcsbRunner(resolved, settings, ReferenceMode, id => id.Kind == YcsbRunKind.Load || IsComparedRow(id)).RunAsync();
        WriteRuns(settings, ReferenceMode, reference);
        var candidate = await new YcsbRunner(resolved, settings, CandidateMode, IsComparedRow).RunAsync();
        WriteRuns(settings, CandidateMode, candidate);

        var comparisons = Compare(reference, candidate);
        Print(comparisons);

        var outPath = YcsbCommand.OutputPathFor(settings, "crosscheck");
        if (outPath != null)
        {
            File.WriteAllText(outPath, JsonSerializer.Serialize(comparisons, new JsonSerializerOptions { WriteIndented = true }));
            AnsiConsole.MarkupLine($"[dim]wrote {Markup.Escape(outPath)}[/]");
        }

        return 0;
    }

    /// <summary>The rows the cross-check compares: every closed-loop workload C row, one per distribution.</summary>
    internal static bool IsComparedRow(YcsbRunIdentity identity) =>
        identity.Kind == YcsbRunKind.WorkloadC && identity.Shape == LoadShape.Closed;

    /// <summary>
    /// Pairs the compared rows of the two passes by row key and compares each. A row missing from
    /// either pass fails, because a one-sided row has nothing to compare.
    /// </summary>
    private static IReadOnlyList<YcsbCrossCheckComparison> Compare(IReadOnlyList<YcsbRunResult> reference, IReadOnlyList<YcsbRunResult> candidate)
    {
        var referenceRows = Rows(ReferenceMode, reference);
        var candidateRows = Rows(CandidateMode, candidate);
        if (referenceRows.Count == 0 || referenceRows.Keys.Order().SequenceEqual(candidateRows.Keys.Order()) == false)
            throw new YcsbCrossCheckException(
                $"The passes ran different workload C rows: '{string.Join(", ", referenceRows.Keys)}' through {CliParsing.FormatTransport(ReferenceMode)}, '{string.Join(", ", candidateRows.Keys)}' through {CliParsing.FormatTransport(CandidateMode)}.");

        return referenceRows.Keys
            .Order(StringComparer.Ordinal)
            .Select(key => YcsbCrossCheck.Compare(key, referenceRows[key], candidateRows[key]))
            .ToList();
    }

    /// <summary>
    /// The sides of every compared row through one mode, from the valid repetitions only, so an
    /// empty or client-bound repetition is never scored as a throughput.
    /// </summary>
    internal static Dictionary<string, YcsbCrossCheckSide> Rows(TransportKind mode, IReadOnlyList<YcsbRunResult> results) =>
        results
            .Where(r => IsComparedRow(r.Identity))
            .GroupBy(r => r.Identity.RowKey)
            .ToDictionary(
                g => g.Key,
                g =>
                {
                    var valid = g.Where(r => YcsbMedianSelector.IsValid(r.Summary.Steps)).ToList();
                    return new YcsbCrossCheckSide(
                        CliParsing.FormatTransport(mode),
                        valid.Select(r => $"{CliParsing.FormatTransport(mode)}-{r.Identity.ResultName}").ToList(),
                        valid.Select(r => YcsbMedianSelector.Statistic(r.Summary.Steps)).ToList());
                });

    private static void WriteRuns(YcsbSettings settings, TransportKind mode, IReadOnlyList<YcsbRunResult> results)
    {
        foreach (var (identity, summary) in results)
        {
            var outPath = YcsbCommand.OutputPathFor(settings, $"{CliParsing.FormatTransport(mode)}-{identity.ResultName}");
            if (outPath != null)
                JsonResultsWriter.Write(outPath, summary);
        }
    }

    private static void Print(IReadOnlyList<YcsbCrossCheckComparison> comparisons)
    {
        AnsiConsole.MarkupLine($"Cross-check over {Markup.Escape(comparisons[0].Statistic)}; noise rule: {Markup.Escape(YcsbCrossCheck.NoiseRule)}");

        var table = new Table();
        table.AddColumn("Row");
        table.AddColumn("Reference median");
        table.AddColumn("Candidate median");
        table.AddColumn("Difference");
        table.AddColumn("Noise");
        table.AddColumn("Verdict");
        foreach (var c in comparisons)
        {
            table.AddRow(
                Markup.Escape(c.RowKey),
                $"{c.Reference.Mode} {c.Reference.Median:F0}",
                $"{c.Candidate.Mode} {c.Candidate.Median:F0}",
                $"{c.Difference:+0;-0;0}",
                $"{c.Noise:F0}",
                c.WithinNoise ? "[green]within noise[/]" : "[yellow]outside noise[/]");
        }

        AnsiConsole.Write(table);
        foreach (var c in comparisons)
            AnsiConsole.MarkupLine($"[dim]{Markup.Escape(c.RowKey)} compared {Markup.Escape(string.Join(", ", c.Reference.Results.Concat(c.Candidate.Results)))}[/]");
    }
}
