using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Lextm.SharpSnmpLib;
using Lextm.SharpSnmpLib.Messaging;

namespace RavenBench.Core.Metrics.Snmp;

public class SnmpClient : ISnmpClient
{
    private const string DefaultCommunity = "ravendb";
    private const int DefaultTimeoutMs = 5000;
    private const int MaxRetries = 3;
    private const int InitialRetryDelayMs = 100;
    private static readonly VersionCode Version = VersionCode.V2;

    public async Task<Dictionary<string, Variable>> GetManyAsync(IEnumerable<string> oids, string host, int port = 161, string? community = null, int? timeoutMs = null)
    {
        var result = new Dictionary<string, Variable>();
        var endpoint = new IPEndPoint(IPAddress.Parse(host), port);
        var communityString = community ?? DefaultCommunity;
        var timeout = timeoutMs ?? DefaultTimeoutMs;

        var variables = new List<Variable>();
        foreach (var oid in oids)
        {
            variables.Add(new Variable(new ObjectIdentifier(oid)));
        }

        Exception? lastException = null;
        for (int attempt = 0; attempt < MaxRetries; attempt++)
        {
            try
            {
                var resultList = await Messenger.GetAsync(Version, endpoint, new OctetString(communityString), variables);
                foreach (var variable in resultList)
                {
                    result[variable.Id.ToString()] = variable;
                }
                return result; // Success
            }
            catch (Exception ex)
            {
                lastException = ex;
                if (attempt < MaxRetries - 1)
                {
                    var delayMs = InitialRetryDelayMs * (int)Math.Pow(2, attempt);
                    await Task.Delay(delayMs);
                }
            }
        }

        // All retries failed
        if (lastException != null)
        {
            Console.WriteLine($"[Raven.Bench] SNMP query failed after {MaxRetries} attempts: {lastException.Message}");
        }

        return result;
    }
}
