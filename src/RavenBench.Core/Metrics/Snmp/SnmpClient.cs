using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Lextm.SharpSnmpLib;
using Lextm.SharpSnmpLib.Messaging;

namespace RavenBench.Core.Metrics.Snmp;

public class SnmpClient
{
    private const string DefaultCommunity = "ravendb";
    private const int DefaultTimeoutMs = 5000;
    private static readonly VersionCode Version = VersionCode.V2;
    private static int _failureLogged;

    public async Task<Dictionary<string, Variable>> GetManyAsync(IEnumerable<string> oids, string host, int port = 161, string? community = null, int? timeoutMs = null)
    {
        var result = new Dictionary<string, Variable>();
        var endpoint = new IPEndPoint(await ResolveAddressAsync(host), port);
        var communityString = community ?? DefaultCommunity;
        int timeout = timeoutMs ?? DefaultTimeoutMs;

        var variables = new List<Variable>();
        foreach (var oid in oids)
        {
            variables.Add(new Variable(new ObjectIdentifier(oid)));
        }

        try
        {
            var resultList = await Messenger.GetAsync(Version, endpoint, new OctetString(communityString), variables)
                .WaitAsync(TimeSpan.FromMilliseconds(timeout));
            foreach (var variable in resultList)
            {
                result[variable.Id.ToString()] = variable;
            }
        }
        catch (Exception ex)
        {
            if (Interlocked.Exchange(ref _failureLogged, 1) == 0)
            {
                Console.WriteLine($"[Raven.Bench] SNMP query failed: {ex.Message}");
            }
        }

        return result;
    }

    private static async Task<IPAddress> ResolveAddressAsync(string host)
    {
        if (IPAddress.TryParse(host, out var address))
            return address;

        var addresses = await Dns.GetHostAddressesAsync(host);
        if (addresses.Length == 0)
            throw new InvalidOperationException($"DNS resolution returned no addresses for '{host}'");

        return addresses.FirstOrDefault(a => a.AddressFamily == AddressFamily.InterNetwork) ?? addresses[0];
    }
}
