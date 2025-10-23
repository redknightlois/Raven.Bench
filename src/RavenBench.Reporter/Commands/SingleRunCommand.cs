using System.Runtime.InteropServices;
using PuppeteerSharp;
using PuppeteerSharp.Media;
using RavenBench.Core.Reporting;

namespace RavenBench.Reporter.Commands;

/// <summary>
/// Command to generate a single-run PDF report using a headless browser.
/// </summary>
public static class SingleRunCommand
{
    private const string ChromeExecutableEnvVar = "PUPPETEER_EXECUTABLE_PATH";

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
        string outputDirectory = Path.GetDirectoryName(absoluteOutputPath) ?? Directory.GetCurrentDirectory();
        Directory.CreateDirectory(outputDirectory);

        BenchmarkSummary summary = await SummaryLoader.LoadAsync(summaryPath);
        string html = SingleRunReportHtmlBuilder.Build(summary, title, notes);

        // If output path already has .html extension, save HTML only and skip PDF generation
        bool htmlOnly = Path.GetExtension(absoluteOutputPath).Equals(".html", StringComparison.OrdinalIgnoreCase);
        string htmlOutputPath = htmlOnly ? absoluteOutputPath : (Path.ChangeExtension(absoluteOutputPath, ".html") ?? Path.Combine(outputDirectory, "report.html"));
        await File.WriteAllTextAsync(htmlOutputPath, html);

        if (htmlOnly)
        {
            Console.WriteLine($"Interactive HTML saved: {htmlOutputPath}");
            return;
        }

        LaunchOptions launchOptions = new LaunchOptions
        {
            Headless = true,
            ExecutablePath = ResolveChromeExecutable(),
            Args = new[]
            {
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--allow-file-access-from-files"
            }
        };

        await using IBrowser browser = await Puppeteer.LaunchAsync(launchOptions);
        await using IPage page = await browser.NewPageAsync();
        await page.SetViewportAsync(new ViewPortOptions
        {
            Width = 1280,
            Height = 720,
            DeviceScaleFactor = 1
        });

        await page.SetContentAsync(html);

        await page.WaitForFunctionAsync("() => window.__reportRendered === true", new WaitForFunctionOptions
        {
            Timeout = 10_000
        });

        PdfOptions pdfOptions = new PdfOptions
        {
            Width = "1280px",
            Height = "720px",
            PrintBackground = true
        };

        await page.PdfAsync(absoluteOutputPath, pdfOptions);

        Console.WriteLine($"Report generated: {absoluteOutputPath}");
        Console.WriteLine($"Interactive HTML saved: {htmlOutputPath}");
    }

    private static string ResolveChromeExecutable()
    {
        string? executablePath = Environment.GetEnvironmentVariable(ChromeExecutableEnvVar);
        if (!string.IsNullOrWhiteSpace(executablePath) && File.Exists(executablePath))
        {
            return executablePath;
        }

        foreach (string candidate in GetCandidateExecutablePaths())
        {
            if (File.Exists(candidate))
            {
                return candidate;
            }
        }

        // To install Chromium on Ubuntu: sudo apt update && sudo apt install -y chromium-browser
        string message = $"Chromium/Chrome executable not found. Set the {ChromeExecutableEnvVar} environment variable or install Chrome/Chromium in a standard location.";
        throw new FileNotFoundException(message);
    }

    private static IEnumerable<string> GetCandidateExecutablePaths()
    {
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            string programFiles = Environment.GetFolderPath(Environment.SpecialFolder.ProgramFiles);
            string programFilesX86 = Environment.GetFolderPath(Environment.SpecialFolder.ProgramFilesX86);
            string localAppData = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);

            yield return Path.Combine(programFiles, "Google", "Chrome", "Application", "chrome.exe");
            yield return Path.Combine(programFilesX86, "Google", "Chrome", "Application", "chrome.exe");
            yield return Path.Combine(localAppData, "Google", "Chrome", "Application", "chrome.exe");
            yield return Path.Combine(programFiles, "Chromium", "Application", "chrome.exe");
            yield return Path.Combine(programFilesX86, "Chromium", "Application", "chrome.exe");
        }
        else if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
        {
            yield return "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome";
            yield return "/Applications/Google Chrome Canary.app/Contents/MacOS/Google Chrome Canary";
            yield return "/Applications/Chromium.app/Contents/MacOS/Chromium";
        }
        else if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
        {
            string[] binaries = { "google-chrome", "google-chrome-stable", "chromium", "chromium-browser" };
            foreach (string candidate in GetExecutablesFromPath(binaries))
            {
                yield return candidate;
            }

            yield return "/usr/bin/google-chrome";
            yield return "/usr/bin/google-chrome-stable";
            yield return "/usr/bin/chromium";
            yield return "/usr/bin/chromium-browser";
            yield return "/snap/bin/chromium";
        }
    }

    private static IEnumerable<string> GetExecutablesFromPath(IEnumerable<string> names)
    {
        string? pathVariable = Environment.GetEnvironmentVariable("PATH");
        if (string.IsNullOrWhiteSpace(pathVariable))
        {
            yield break;
        }

        string[] searchPaths = pathVariable.Split(Path.PathSeparator, StringSplitOptions.RemoveEmptyEntries);
        foreach (string directory in searchPaths)
        {
            foreach (string name in names)
            {
                yield return Path.Combine(directory, name);
            }
        }
    }
}
