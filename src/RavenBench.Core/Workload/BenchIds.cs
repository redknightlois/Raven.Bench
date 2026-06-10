namespace RavenBench.Core.Workload;

public static class BenchIds
{
    public static string IdFor(long i) => $"bench/{i:D8}";
}
