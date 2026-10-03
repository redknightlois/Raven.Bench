using System;
using System.Collections.Generic;
using System.IO;
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
    private readonly TextWriter _log;
    private int _failing;

    public SnmpClient(TextWriter? log = null)
    {
        _log = log ?? Console.Out;
    }

    public async Task<Dictionary<string, Variable>> GetManyAsync(IEnumerable<string> oids, string host, int port = 161, string? community = null, int? timeoutMs = null)
    {
        var endpoint = new IPEndPoint(await ResolveAddressAsync(host), port);
        return await GetManyAsync(oids, endpoint, new Socket(endpoint.AddressFamily, SocketType.Dgram, ProtocolType.Udp), community, timeoutMs);
    }

    /// <summary>Sends one GET over the given socket and disposes the socket when the request ends, on success, failure or timeout.</summary>
    internal async Task<Dictionary<string, Variable>> GetManyAsync(IEnumerable<string> oids, IPEndPoint endpoint, Socket socket, string? community = null, int? timeoutMs = null)
    {
        var result = new Dictionary<string, Variable>();
        var variables = oids.Select(oid => new Variable(new ObjectIdentifier(oid))).ToList();
        var request = new GetRequestMessage(Messenger.NextRequestId, Version, new OctetString(community ?? DefaultCommunity), variables);

        try
        {
            // Disposing the socket ends the pending receive of a timed-out request.
            using (socket)
            {
                var response = await request.GetResponseAsync(endpoint, socket)
                    .WaitAsync(TimeSpan.FromMilliseconds(timeoutMs ?? DefaultTimeoutMs));
                var pdu = response.Pdu();
                if (pdu.ErrorStatus.ToInt32() != 0)
                    throw new InvalidOperationException($"SNMP agent returned error status {pdu.ErrorStatus.ToInt32()} at index {pdu.ErrorIndex.ToInt32()}");

                foreach (var variable in pdu.Variables)
                {
                    result[variable.Id.ToString()] = variable;
                }
            }
            RecordOutcome(null);
        }
        catch (Exception ex)
        {
            RecordOutcome(ex);
        }

        return result;
    }

    /// <summary>Reports a failure on the first failed query after construction or after a successful query; repeated failures stay silent.</summary>
    internal void RecordOutcome(Exception? failure)
    {
        if (failure is null)
        {
            Interlocked.Exchange(ref _failing, 0);
            return;
        }

        if (Interlocked.Exchange(ref _failing, 1) == 0)
            _log.WriteLine($"[Raven.Bench] SNMP query failed: {failure.Message} (further failures are not reported until a query succeeds)");
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
