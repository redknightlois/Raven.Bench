using RavenBench.Core.Reporting;

namespace RavenBench.Reporter.Commands;

/// <summary>
/// Command to generate a single-run PDF report using a headless browser.
/// </summary>
public static class SingleRunCommand
{
    /// <summary>
    /// Executes the single-run report generation.
    /// </summary>
    /// <param name="summaryPath">Benchmark summary JSON path.</param>
    /// <param name="outputPath">Output PDF file path.</param>
    /// <param name="title">Report title override.</param>
    /// <param name="notes">Supplementary notes.</param>
    public static async Task ExecuteAsync(string summaryPath, string outputPath, string? title, string? notes)
    {
        string absoluteOutputPath = Path.GetFullPath(outputPath);

        BenchmarkSummary summary = await SummaryLoader.LoadAsync(summaryPath);
        string html = SingleRunReportHtmlBuilder.Build(summary, title, notes);

        (string htmlOutputPath, bool htmlOnly) = await ReportRenderUtilities.WriteHtmlAsync(html, absoluteOutputPath);

        if (htmlOnly)
        {
            Console.WriteLine($"Interactive HTML saved: {htmlOutputPath}");
            return;
        }

        await ReportRenderUtilities.GeneratePdfAsync(html, absoluteOutputPath);

        Console.WriteLine($"Report generated: {absoluteOutputPath}");
        Console.WriteLine($"Interactive HTML saved: {htmlOutputPath}");
    }
}
