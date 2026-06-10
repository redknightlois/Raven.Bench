using System.Net.Http;
using System.Text.Json;
using Raven.Client.Documents;
using RavenBench.Core.Metrics.Snmp;

namespace RavenBench.Core.Transport;

/// <summary>
/// Shared administrative plumbing (SNMP discovery, license queries, calibration endpoints,
/// document counting) used by both transports. Does not own the HttpClient it is given.
/// </summary>
internal sealed class TransportAdminClient
{
    private readonly HttpClient _http;
    private readonly string _baseUrl;

    public TransportAdminClient(HttpClient http, string baseUrl)
    {
        _http = http;
        _baseUrl = baseUrl.TrimEnd('/');
    }

    public string BuildCalibrationUrl(string endpoint)
    {
        if (Uri.TryCreate(endpoint, UriKind.Absolute, out var uri) &&
            (uri.Scheme == "http" || uri.Scheme == "https"))
            return endpoint;

        if (endpoint.StartsWith('/') == false)
            endpoint = "/" + endpoint;

        return _baseUrl + endpoint;
    }

    public IReadOnlyList<(string name, string path)> GetCalibrationEndpoints()
    {
        return new List<(string, string)>
        {
            ("server-version", "/build/version"),
            ("license-status", "/license/status")
        };
    }

    public async Task<string> GetServerLicenseTypeAsync()
    {
        try
        {
            using var resp = await _http.GetAsync($"{_baseUrl}/license/status", HttpCompletionOption.ResponseHeadersRead).ConfigureAwait(false);
            resp.EnsureSuccessStatusCode();
            await using var stream = await resp.Content.ReadAsStreamAsync().ConfigureAwait(false);
            using var doc = await JsonDocument.ParseAsync(stream).ConfigureAwait(false);

            if (doc.RootElement.TryGetProperty("Type", out var licenseType))
                return licenseType.GetString() ?? "unknown";
            if (doc.RootElement.TryGetProperty("LicenseType", out var altLicenseType))
                return altLicenseType.GetString() ?? "unknown";

            return "unknown";
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Raven.Bench] Warning: Failed to get license type: {ex.GetType().Name}: {ex.Message}");
            return "unknown";
        }
    }

    public async Task<SnmpSample> GetSnmpMetricsAsync(SnmpOptions snmpOptions, string? databaseName = null)
    {
        long? databaseIndex = null;
        if (string.IsNullOrEmpty(databaseName) == false && await TryGetDatabaseSnmpIndexAsync(databaseName).ConfigureAwait(false) is { } index)
        {
            databaseIndex = index;
        }

        var snmpClient = new SnmpClient();
        var oids = SnmpOids.GetOidsForProfile(snmpOptions.Profile, databaseIndex);
        var host = new Uri(_baseUrl).Host;
        var timeoutMs = (int)snmpOptions.Timeout.TotalMilliseconds;
        var snmpResults = await snmpClient.GetManyAsync(oids, host, snmpOptions.Port, SnmpOptions.Community, timeoutMs).ConfigureAwait(false);
        return SnmpMetricMapper.MapToSample(snmpResults);
    }

    public async Task<long?> TryGetDatabaseSnmpIndexAsync(string databaseName)
    {
        try
        {
            using var resp = await _http.GetAsync($"{_baseUrl}/monitoring/snmp/oids", HttpCompletionOption.ResponseHeadersRead).ConfigureAwait(false);
            resp.EnsureSuccessStatusCode();
            await using var stream = await resp.Content.ReadAsStreamAsync().ConfigureAwait(false);
            using var doc = await JsonDocument.ParseAsync(stream).ConfigureAwait(false);

            if (doc.RootElement.TryGetProperty("Databases", out var databases) == false)
            {
                Console.WriteLine("[WARN] SNMP OID response missing 'Databases' property");
                return null;
            }

            if (databases.TryGetProperty(databaseName, out var dbElement) == false)
            {
                Console.WriteLine($"[WARN] Database '{databaseName}' not found in SNMP OID mapping");
                return null;
            }

            if (dbElement.TryGetProperty("@General", out var general) == false || general.GetArrayLength() == 0)
            {
                Console.WriteLine($"[WARN] No '@General' section or empty for database '{databaseName}'");
                return null;
            }

            var firstOid = general[0].GetProperty("OID").GetString();
            if (SnmpOids.TryParseDatabaseIndexFromOid(firstOid, out var index))
            {
                return index;
            }

            Console.WriteLine($"[WARN] Failed to parse database index from OID: {firstOid}");
            return null;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[WARN] Failed to discover SNMP database index for '{databaseName}': {ex.Message}");
            Console.WriteLine("[WARN] Falling back to server-wide SNMP metrics (IO/request metrics will be unavailable)");
            return null;
        }
    }

    public static async Task<long> GetDocumentCountAsync(IDocumentStore store, string idPrefix)
    {
        using var session = store.OpenAsyncSession();
        // Streaming counts documents without waiting on indexing.
        var count = 0L;
        await using (var stream = await session.Advanced.StreamAsync<object>(startsWith: idPrefix).ConfigureAwait(false))
        {
            while (await stream.MoveNextAsync().ConfigureAwait(false))
            {
                count++;
            }
        }
        return count;
    }
}
