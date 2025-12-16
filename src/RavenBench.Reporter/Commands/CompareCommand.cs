using RavenBench.Core.Reporting;
using RavenBench.Reporter.Models;

namespace RavenBench.Reporter.Commands;

/// <summary>
/// Command to generate a multi-run comparison PDF report using a headless browser.
/// </summary>
public static class CompareCommand
{
    /// <summary>
    /// Executes the multi-run comparison report generation.
    /// </summary>
    /// <param name="summaryPaths">Paths to 2+ benchmark summary JSON files.</param>
    /// <param name="labels">Labels for each run (must match summary count, or empty for auto-generation).</param>
    /// <param name="outputPath">Output PDF file path.</param>
    /// <param name="title">Report title override.</param>
    /// <param name="notes">Supplementary notes.</param>
    /// <param name="baselineIndex">Index of baseline summary (default 0).</param>
    public static async Task ExecuteAsync(
        IReadOnlyList<string> summaryPaths,
        IReadOnlyList<string> labels,
        string outputPath,
        string? title,
        string? notes,
        int baselineIndex = 0)
    {
        // Validate inputs
        if (summaryPaths.Count < 2)
        {
            throw new ArgumentException("Need at least two summaries for comparison.");
        }

        // Generate default labels if not provided or empty
        var effectiveLabels = labels != null && labels.Count > 0
            ? labels.ToList()
            : GenerateDefaultLabels(summaryPaths.Count);

        if (effectiveLabels.Count != summaryPaths.Count)
        {
            throw new ArgumentException($"Provided {effectiveLabels.Count} labels for {summaryPaths.Count} runs. Label count must match summary count.");
        }

        if (baselineIndex < 0 || baselineIndex >= summaryPaths.Count)
        {
            throw new ArgumentException($"Baseline index {baselineIndex} is out of range for {summaryPaths.Count} summaries.");
        }

        string absoluteOutputPath = Path.GetFullPath(outputPath);

        // Load summaries
        var summaries = new List<BenchmarkSummary>();
        foreach (var path in summaryPaths)
        {
            var summary = await SummaryLoader.LoadAsync(path);
            summaries.Add(summary);
        }

        // Build comparison model
        ComparisonModel model = ComparisonModelBuilder.Build(summaries, effectiveLabels, baselineIndex);

        // Generate HTML
        string html = ComparisonReportHtmlBuilder.Build(model, title, notes);

        (string htmlOutputPath, bool htmlOnly) = await ReportRenderUtilities.WriteHtmlAsync(html, absoluteOutputPath);

        if (htmlOnly)
        {
            Console.WriteLine($"Interactive HTML saved: {htmlOutputPath}");
            return;
        }

        await ReportRenderUtilities.GeneratePdfAsync(html, absoluteOutputPath);

        Console.WriteLine($"Comparison report generated: {absoluteOutputPath}");
        Console.WriteLine($"Interactive HTML saved: {htmlOutputPath}");
    }

    private static List<string> GenerateDefaultLabels(int count)
    {
        var labels = new List<string>();
        for (int i = 0; i < count; i++)
        {
            labels.Add($"Run {i + 1}");
        }
        return labels;
    }
}
