using Spectre.Console.Cli;
using RavenBench.Cli;

namespace RavenBench;

internal static class Program
{
    private static async Task<int> Main(string[] args)
    {
        var app = new CommandApp();
        app.Configure(cfg =>
        {
            cfg.SetApplicationName("Raven.Bench");
            cfg.ValidateExamples();
            cfg.AddCommand<RunCommand>("run")
                .WithDescription("Run a benchmark ramp and detect the knee.")
                .WithExample("run", "--url", "http://localhost:10101", "--database", "ycsb", "--reads", "75", "--writes", "25", "--compression", "raw:identity", "--concurrency", "8..512x2");
        });
        return await app.RunAsync(args);
    }
}

