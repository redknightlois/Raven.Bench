using System.IO;
using System.Text.RegularExpressions;
using RavenBench.Core.Diagnostics;
using Xunit;

namespace RavenBench.Tests.Transport;

public class VectorComposeFileTests
{
    private static string Compose() =>
        File.ReadAllText(Path.Combine(RepositoryRootLocator.Find(), "benchmarks", "vector", "docker-compose.yml"));

    [Fact]
    public void The_PgVector_Image_Is_Pinned_To_A_Release_Tag_Naming_Both_Versions()
    {
        Assert.Matches(new Regex(@"image:\s*pgvector/pgvector:0\.8\.\d+-pg17\s"), Compose());
    }

    [Fact]
    public void The_Compose_File_Carries_No_Server_Tuning_Of_Our_Own()
    {
        var compose = Compose();
        foreach (var setting in new[] { "shared_buffers", "maintenance_work_mem", "max_parallel_maintenance_workers", "command:", "shm_size", "-c " })
            Assert.DoesNotMatch(new Regex(@"^\s*[^#\s].*" + Regex.Escape(setting), RegexOptions.Multiline), compose);
    }

    [Fact]
    public void The_Elasticsearch_Service_Is_A_Pinned_Single_Unsecured_Trial_Node_With_Image_Defaults()
    {
        var compose = Compose();
        Assert.Matches(new Regex(@"image:\s*elasticsearch:9\.5\.\d+\s"), compose);
        Assert.Contains("discovery.type: single-node", compose);
        Assert.Contains("xpack.security.enabled: \"false\"", compose);
        Assert.Contains("xpack.license.self_generated.type: \"${ELASTICSEARCH_LICENSE:-trial}\"", compose);
        Assert.Contains("${ELASTICSEARCH_DATA_DIR:-vector-elasticsearch-data}:/usr/share/elasticsearch/data", compose);
        Assert.Contains("ES_JAVA_OPTS: \"${ELASTICSEARCH_JAVA_OPTS:-}\"", compose);
        foreach (var setting in new[] { "-Xmx", "-Xms", "cluster.routing.allocation.disk" })
            Assert.DoesNotContain(setting, compose);
    }
}
