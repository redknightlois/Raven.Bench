using System.Runtime.InteropServices;
using PuppeteerSharp;
using PuppeteerSharp.Media;

namespace RavenBench.Reporter.Commands;

/// <summary>
/// Shared helpers for writing report HTML files and exporting PDFs via Puppeteer.
/// </summary>
internal static class ReportRenderUtilities
{
    private const string ChromeExecutableEnvVar = "PUPPETEER_EXECUTABLE_PATH";

    /// <summary>
    /// Writes the hydrated HTML to disk and returns the output path along with a flag indicating whether only HTML was requested.
    /// </summary>
    /// <param name="html">Report HTML content.</param>
    /// <param name="absoluteOutputPath">The absolute path specified by the caller (HTML or PDF).</param>
    public static async Task<(string HtmlOutputPath, bool HtmlOnly)> WriteHtmlAsync(string html, string absoluteOutputPath)
    {
        string outputDirectory = Path.GetDirectoryName(absoluteOutputPath) ?? Directory.GetCurrentDirectory();
        Directory.CreateDirectory(outputDirectory);

        bool htmlOnly = Path.GetExtension(absoluteOutputPath).Equals(".html", StringComparison.OrdinalIgnoreCase);
        string htmlOutputPath = htmlOnly
            ? absoluteOutputPath
            : (Path.ChangeExtension(absoluteOutputPath, ".html") ?? Path.Combine(outputDirectory, "report.html"));

        await File.WriteAllTextAsync(htmlOutputPath, html);
        return (htmlOutputPath, htmlOnly);
    }

    /// <summary>
    /// Generates a PDF from the provided HTML using Puppeteer.
    /// </summary>
    /// <param name="html">Report HTML content.</param>
    /// <param name="absoluteOutputPath">Target PDF path.</param>
    public static async Task GeneratePdfAsync(string html, string absoluteOutputPath)
    {
        LaunchOptions launchOptions = BuildLaunchOptions();

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
    }

    private static LaunchOptions BuildLaunchOptions()
    {
        return new LaunchOptions
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
    }

    private static string ResolveChromeExecutable()
    {
        string? executablePath = Environment.GetEnvironmentVariable(ChromeExecutableEnvVar);
        if (string.IsNullOrWhiteSpace(executablePath) == false && File.Exists(executablePath))
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
