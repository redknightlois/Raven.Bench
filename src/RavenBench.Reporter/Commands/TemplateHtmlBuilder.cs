using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Reflection;

namespace RavenBench.Reporter.Commands;

/// <summary>
/// Loads an embedded HTML template and injects a JSON payload plus the report context.
/// </summary>
public static class TemplateHtmlBuilder
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
        WriteIndented = false,
        Converters = { new JsonStringEnumConverter() }
    };

    private const string ContextPlaceholder = "__REPORT_CONTEXT__";

    public static string Build(string templateName, string payloadPlaceholder, object payload, string? title, string? notes)
    {
        string template = LoadTemplateFromEmbeddedResource($"RavenBench.Reporter.Templates.{templateName}");

        string payloadJson = JsonSerializer.Serialize(payload, JsonOptions);
        string contextJson = JsonSerializer.Serialize(new ReportContext(title, notes, ResolveReporterVersion()), JsonOptions);

        payloadJson = payloadJson.Replace("</script>", "<\\/script>", StringComparison.OrdinalIgnoreCase);
        contextJson = contextJson.Replace("</script>", "<\\/script>", StringComparison.OrdinalIgnoreCase);

        return template
            .Replace(payloadPlaceholder, payloadJson, StringComparison.Ordinal)
            .Replace(ContextPlaceholder, contextJson, StringComparison.Ordinal);
    }

    private static string LoadTemplateFromEmbeddedResource(string resourceName)
    {
        Assembly assembly = Assembly.GetExecutingAssembly();
        using Stream? stream = assembly.GetManifestResourceStream(resourceName);

        if (stream == null)
        {
            string availableResources = string.Join(", ", assembly.GetManifestResourceNames());
            throw new InvalidOperationException(
                $"Embedded resource '{resourceName}' not found. Available resources: {availableResources}");
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
