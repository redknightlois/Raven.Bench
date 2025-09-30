using System.Globalization;
using System.IO;
using System.Linq;
using CsvHelper;

namespace RavenBench.Reporting
{
    public static class CsvResultsWriter
    { 
        public static void Write(string path, BenchmarkSummary summary, bool snmpEnabled)
        {
            using var writer = new StreamWriter(path);
            using var csv = new CsvWriter(writer, CultureInfo.InvariantCulture);

            var fields = CsvMetrics.GetVisibleFields(summary);

            foreach (var field in fields)
            {
                csv.WriteField(field.Name);
            }
            csv.NextRecord();

            foreach (var step in summary.Steps)
            {
                foreach (var field in fields)
                {
                    csv.WriteField(field.ValueSelector(step));
                }
                csv.NextRecord();
            }
        }
    }
}
