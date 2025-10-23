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

        return await rootCommand.InvokeAsync(args);
    }
}