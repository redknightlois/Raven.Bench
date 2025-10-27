using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Reflection;
using RavenBench.Reporter.Models;

namespace RavenBench.Reporter.Commands;

/// <summary>
/// Loads the multi-run HTML template and injects the comparison model payload.
/// </summary>
internal static class ComparisonReportHtmlBuilder
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
        WriteIndented = false
    };

    private const string EmbeddedResourceName = "RavenBench.Reporter.Templates.multi-run.html";
    private const string ModelPlaceholder = "__COMPARISON_MODEL__";
    private const string ContextPlaceholder = "__REPORT_CONTEXT__";

    public static string Build(ComparisonModel model, string? title, string? notes)
    {
        string template = LoadTemplateFromEmbeddedResource();

        string modelJson = JsonSerializer.Serialize(model, JsonOptions);
        string contextJson = JsonSerializer.Serialize(new ReportContext(title, notes, ResolveReporterVersion()), JsonOptions);

        modelJson = modelJson.Replace("</script>", "<\\/script>", StringComparison.OrdinalIgnoreCase);
        contextJson = contextJson.Replace("</script>", "<\\/script>", StringComparison.OrdinalIgnoreCase);

        string populated = template
            .Replace(ModelPlaceholder, modelJson, StringComparison.Ordinal)
            .Replace(ContextPlaceholder, contextJson, StringComparison.Ordinal);

        return populated;
    }

    private static string LoadTemplateFromEmbeddedResource()
    {
        Assembly assembly = Assembly.GetExecutingAssembly();
        using Stream? stream = assembly.GetManifestResourceStream(EmbeddedResourceName);

        if (stream == null)
        {
            string availableResources = string.Join(", ", assembly.GetManifestResourceNames());
            throw new InvalidOperationException(
                $"Embedded resource '{EmbeddedResourceName}' not found. Available resources: {availableResources}");
        }

        using StreamReader reader = new(stream, Encoding.UTF8);
        return reader.ReadToEnd();
    }

    private static string ResolveReporterVersion()
    {
        Assembly assembly = Assembly.GetExecutingAssembly();
        string? informationalVersion = assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion;
        if (string.IsNullOrWhiteSpace(informationalVersion) == false)
        {
            return informationalVersion;
        }

        Version? version = assembly.GetName().Version;
        return version?.ToString() ?? "unknown";
    }

    private sealed record ReportContext(string? Title, string? Notes, string ReporterVersion)
    {
        public string GeneratedAt { get; } = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss 'UTC'");
    }
}