using System.Collections.Generic;
using System.Threading.Tasks;
using Lextm.SharpSnmpLib;

namespace RavenBench.Metrics.Snmp;

public interface ISnmpClient
{
    Task<Dictionary<string, Variable>> GetManyAsync(IEnumerable<string> oids, string host, int port = 161);
}