using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Lextm.SharpSnmpLib;
using Lextm.SharpSnmpLib.Messaging;

namespace RavenBench.Metrics.Snmp;

public class SnmpClient : ISnmpClient
{
    private const string Community = "ravendb";
    private static readonly VersionCode Version = VersionCode.V2;

    public async Task<Dictionary<string, Variable>> GetManyAsync(IEnumerable<string> oids, string host, int port = 161)
    {
        var result = new Dictionary<string, Variable>();
        var endpoint = new IPEndPoint(IPAddress.Parse(host), port);

        var variables = new List<Variable>();
        foreach (var oid in oids)
        {
            variables.Add(new Variable(new ObjectIdentifier(oid)));
        }

        try
        {
            var resultList = await Messenger.GetAsync(Version, endpoint, new OctetString(Community), variables);
            foreach (var variable in resultList)
            {
                result[variable.Id.ToString()] = variable;
            }
        }
        catch (Exception)
        {
            // silent fail for v0
        }

        return result;
    }
}
