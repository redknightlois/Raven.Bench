using System.CommandLine;
using RavenBench.Reporter.Commands;

namespace RavenBench.Reporter;

public static class Program
{
    public static async Task<int> Main(string[] args)
    {
        var rootCommand = new RootCommand("RavenBench Reporter - Generate PDF reports from benchmark summaries");

        var singleCommand = new Command("single", "Generate a single-run PDF report");
        var summaryOption = new Option<FileInfo>("--summary", "Path to the benchmark summary JSON file") { IsRequired = true };
        var outputOption = new Option<FileInfo>("--output", () => new FileInfo("report.pdf"), "Output PDF file path");
        var titleOption = new Option<string>("--title", "Report title");
        var notesOption = new Option<string>("--notes", "Additional notes for the report");

        singleCommand.AddOption(summaryOption);
        singleCommand.AddOption(outputOption);
        singleCommand.AddOption(titleOption);
        singleCommand.AddOption(notesOption);

        singleCommand.SetHandler(async (FileInfo summary, FileInfo output, string? title, string? notes) =>
        {
            await SingleRunCommand.ExecuteAsync(summary.FullName, output.FullName, title, notes);
        }, summaryOption, outputOption, titleOption, notesOption);

        rootCommand.AddCommand(singleCommand);

        var compareCommand = new Command("compare", "Generate a multi-run comparison PDF report");
        var compareSummaryOption = new Option<FileInfo[]>("--summary", "Path to benchmark summary JSON files (2+ required)") { IsRequired = true, AllowMultipleArgumentsPerToken = true };
        var compareOutputOption = new Option<FileInfo>("--output", () => new FileInfo("comparison.pdf"), "Output PDF file path");
        var compareTitleOption = new Option<string>("--title", "Report title");
        var compareNotesOption = new Option<string>("--notes", "Additional notes for the report");
        var compareLabelsOption = new Option<string[]>("--labels", "Labels for each summary (optional, auto-generated if not provided)") { AllowMultipleArgumentsPerToken = true };
        var compareBaselineOption = new Option<int>("--baseline", () => 0, "Index of baseline summary (default: 0)");

        compareCommand.AddOption(compareSummaryOption);
        compareCommand.AddOption(compareOutputOption);
        compareCommand.AddOption(compareTitleOption);
        compareCommand.AddOption(compareNotesOption);
        compareCommand.AddOption(compareLabelsOption);
        compareCommand.AddOption(compareBaselineOption);

        compareCommand.SetHandler(async (FileInfo[] summaries, FileInfo output, string? title, string? notes, string[]? labels, int baseline) =>
        {
            var summaryPaths = summaries.Select(s => s.FullName).ToList();
            var finalLabels = labels?.ToList() ?? new List<string>();
            await CompareCommand.ExecuteAsync(summaryPaths, finalLabels, output.FullName, title, notes, baseline);
        }, compareSummaryOption, compareOutputOption, compareTitleOption, compareNotesOption, compareLabelsOption, compareBaselineOption);

        rootCommand.AddCommand(compareCommand);

        return await rootCommand.InvokeAsync(args);
    }
}