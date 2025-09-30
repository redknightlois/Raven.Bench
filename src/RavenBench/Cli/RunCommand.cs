using RavenBench.Analysis;
using RavenBench.Reporting;
using RavenBench.Util;
using Spectre.Console;
using Spectre.Console.Cli;

namespace RavenBench.Cli;

public sealed class RunCommand : AsyncCommand<RunSettings>
{
    public override async Task<int> ExecuteAsync(CommandContext context, RunSettings settings)
    {
        if (string.IsNullOrWhiteSpace(settings.Url) || string.IsNullOrWhiteSpace(settings.Database))
        {
            AnsiConsole.MarkupLine("[red]--url and --database are required.[/]");
            return -1;
        }
        var opts = settings.ToRunOptions();
        if (string.Equals(opts.Mode, "closed", StringComparison.OrdinalIgnoreCase) == false)
        {
            AnsiConsole.MarkupLine("[yellow]Only closed-loop mode implemented in v0. Use --mode closed.[/]");
            return -2;
        }

        var runner = new BenchmarkRunner(opts);
        var run = await runner.RunAsync();

        // Knee detection and analysis
        var knee = KneeFinder.FindKnee(run.Steps, opts.KneeThroughputDelta, opts.KneeP95Delta, opts.MaxErrorRate);

        // Create a temporary summary for analysis
        var tempSummary = new BenchmarkSummary
        {
            Options = opts,
            Steps = run.Steps,
            Knee = knee,
            Verdict = "temporary",
            ClientCompression = run.ClientCompression,
            EffectiveHttpVersion = run.EffectiveHttpVersion,
            StartupCalibration = run.StartupCalibration,
            Notes = opts.Notes
        };

        // Perform complete result analysis
        var analysis = ResultAnalyzer.Analyze(run, knee, opts, tempSummary);

        // Create final summary with analysis results
        var summary = new BenchmarkSummary
        {
            Options = opts,
            Steps = run.Steps,
            Knee = knee,
            Verdict = analysis.Verdict,
            ClientCompression = run.ClientCompression,
            EffectiveHttpVersion = run.EffectiveHttpVersion,
            StartupCalibration = run.StartupCalibration,
            Notes = opts.Notes
        };

        // Write outputs
        if (string.IsNullOrWhiteSpace(opts.OutJson) == false)
            JsonResultsWriter.Write(opts.OutJson!, summary);
        if (string.IsNullOrWhiteSpace(opts.OutCsv) == false)
            CsvResultsWriter.Write(opts.OutCsv!, summary, opts.SnmpEnabled);

        // Render console report
        RenderResults(summary, run.MaxNetworkUtilization, analysis, opts.LatencyDisplay);

        return 0;
    }

    private static void RenderResults(BenchmarkSummary summary, double maxNetUtil, ResultAnalyzer.Report analysis, LatencyDisplayType latencyDisplay)
    {
        AnsiConsole.WriteLine();
        AnsiConsole.WriteLine();

        // Determine which columns to show based on available data (shared between normalized and raw tables)
        bool showServerReqs = summary.Options.SnmpEnabled &&
                              summary.Options.Snmp.Profile == Util.SnmpProfile.Extended &&
                              summary.Steps.Any(s => s.ServerSnmpRequestsPerSec.HasValue);
        bool showErrors = summary.Steps.Any(s => s.ErrorRate > 0);
        bool showServerIoR = summary.Steps.Any(s => s.ServerIoReadOps.HasValue);
        bool showServerIoW = summary.Steps.Any(s => s.ServerIoWriteOps.HasValue);
        bool showDirtyMem = summary.Options.SnmpEnabled &&
                            summary.Options.Snmp.Profile == Util.SnmpProfile.Extended &&
                            summary.Steps.Any(s => s.DirtyMemoryMb.HasValue);
        bool showLoad1m = summary.Options.SnmpEnabled &&
                          summary.Options.Snmp.Profile == Util.SnmpProfile.Extended &&
                          summary.Steps.Any(s => s.Load1Min.HasValue);
        bool showSnmpErrors = summary.Options.SnmpEnabled &&
                              summary.Options.Snmp.Profile == Util.SnmpProfile.Extended &&
                              summary.Steps.Any(s => s.SnmpErrorsPerSec.HasValue);

        // Display tables based on latency type
        if (latencyDisplay == LatencyDisplayType.Normalized || latencyDisplay == LatencyDisplayType.Both)
        {
            var normalizedTable = new Table().Border(TableBorder.Rounded).Caption("[grey]Per-step metrics (RTT-normalized)[/]");

            // Add columns
            normalizedTable.AddColumn("Concurrency");
            normalizedTable.AddColumn("Thr/s");

            if (showServerReqs)
            {
                normalizedTable.AddColumn("Server Req/s");
            }

            normalizedTable.AddColumn("p50 ms");
            normalizedTable.AddColumn("p95 ms");

            if (showErrors)
            {
                normalizedTable.AddColumn("Errors %");
            }

            normalizedTable.AddColumn("Client CPU %");
            normalizedTable.AddColumn("Client Net %");

            if (summary.Options.SnmpEnabled == false)
            {
                normalizedTable.AddColumn("Server CPU %");
                normalizedTable.AddColumn("Server Mem MB");
            }

            if (showServerIoR)
            {
                normalizedTable.AddColumn("Server IO R");
            }
            if (showServerIoW)
            {
                normalizedTable.AddColumn("Server IO W");
            }

            if (summary.Options.SnmpEnabled)
            {
                normalizedTable.AddColumn("Machine CPU %");
                normalizedTable.AddColumn("Process CPU %");
                normalizedTable.AddColumn("Managed MB");
                normalizedTable.AddColumn("Unmanaged MB");

                if (summary.Options.Snmp.Profile == Util.SnmpProfile.Extended)
                {
                    if (showDirtyMem)
                    {
                        normalizedTable.AddColumn("Dirty MB");
                    }
                    if (showLoad1m)
                    {
                        normalizedTable.AddColumn("Load 1m");
                    }
                    if (showSnmpErrors)
                    {
                        normalizedTable.AddColumn("Err/s");
                    }
                }
            }
            foreach (var s in summary.Steps)
            {
                var row = new List<string>
                {
                    s.Concurrency.ToString(),
                    s.Throughput.ToString("F0")
                };

                if (showServerReqs)
                {
                    row.Add(s.ServerSnmpRequestsPerSec?.ToString("F0") ?? "N/A");
                }

                row.Add(s.Normalized.P50.ToString("F1"));
                row.Add(s.Normalized.P95.ToString("F1"));

                if (showErrors)
                {
                    row.Add(s.ErrorRate == 0 ? "N/A" : (s.ErrorRate * 100).ToString("F1"));
                }

                row.Add((s.ClientCpu * 100).ToString("F0"));
                row.Add((s.NetworkUtilization * 100).ToString("F1"));

                if (summary.Options.SnmpEnabled == false)
                {
                    row.Add(s.ServerCpu?.ToString("F1") ?? "N/A");
                    row.Add(s.ServerMemoryMB?.ToString() ?? "N/A");
                }

                if (showServerIoR)
                {
                    row.Add(s.ServerIoReadOps?.ToString() ?? "N/A");
                }
                if (showServerIoW)
                {
                    row.Add(s.ServerIoWriteOps?.ToString() ?? "N/A");
                }

                if (summary.Options.SnmpEnabled)
                {
                    row.Add(s.MachineCpu?.ToString("F1") ?? "N/A");
                    row.Add(s.ProcessCpu?.ToString("F1") ?? "N/A");
                    row.Add(s.ManagedMemoryMb?.ToString("F0") ?? "N/A");
                    row.Add(s.UnmanagedMemoryMb?.ToString("F0") ?? "N/A");

                    if (summary.Options.Snmp.Profile == Util.SnmpProfile.Extended)
                    {
                        if (showDirtyMem)
                        {
                            row.Add(s.DirtyMemoryMb?.ToString("F0") ?? "N/A");
                        }
                        if (showLoad1m)
                        {
                            row.Add(s.Load1Min?.ToString("F2") ?? "N/A");
                        }
                        if (showSnmpErrors)
                        {
                            row.Add(s.SnmpErrorsPerSec?.ToString("F1") ?? "N/A");
                        }
                    }
                }

                normalizedTable.AddRow(row.ToArray());
            }
            AnsiConsole.Write(normalizedTable);
        }

        if (latencyDisplay == LatencyDisplayType.Raw || latencyDisplay == LatencyDisplayType.Both)
        {
            if (latencyDisplay == LatencyDisplayType.Both)
                AnsiConsole.WriteLine();

            var rawTable = new Table().Border(TableBorder.Rounded).Caption("[grey]Per-step metrics (raw)[/]");

            // Add columns
            rawTable.AddColumn("Concurrency");
            rawTable.AddColumn("Thr/s");

            if (showServerReqs)
            {
                rawTable.AddColumn("Server Req/s");
            }

            rawTable.AddColumn("p50 ms");
            rawTable.AddColumn("p95 ms");

            if (showErrors)
            {
                rawTable.AddColumn("Errors %");
            }

            rawTable.AddColumn("Client CPU %");
            rawTable.AddColumn("Client Net %");

            if (summary.Options.SnmpEnabled == false)
            {
                rawTable.AddColumn("Server CPU %");
                rawTable.AddColumn("Server Mem MB");
            }

            if (showServerIoR)
            {
                rawTable.AddColumn("Server IO R");
            }
            if (showServerIoW)
            {
                rawTable.AddColumn("Server IO W");
            }

            if (summary.Options.SnmpEnabled)
            {
                rawTable.AddColumn("Machine CPU %");
                rawTable.AddColumn("Process CPU %");
                rawTable.AddColumn("Managed MB");
                rawTable.AddColumn("Unmanaged MB");

                if (summary.Options.Snmp.Profile == Util.SnmpProfile.Extended)
                {
                    if (showDirtyMem)
                    {
                        rawTable.AddColumn("Dirty MB");
                    }
                    if (showLoad1m)
                    {
                        rawTable.AddColumn("Load 1m");
                    }
                    if (showSnmpErrors)
                    {
                        rawTable.AddColumn("Err/s");
                    }
                }
            }
            foreach (var s in summary.Steps)
            {
                var rawRow = new List<string>
                {
                    s.Concurrency.ToString(),
                    s.Throughput.ToString("F0")
                };

                if (showServerReqs)
                {
                    rawRow.Add(s.ServerSnmpRequestsPerSec?.ToString("F0") ?? "N/A");
                }

                rawRow.Add(s.Raw.P50.ToString("F1"));
                rawRow.Add(s.Raw.P95.ToString("F1"));

                if (showErrors)
                {
                    rawRow.Add((s.ErrorRate * 100).ToString("F2"));
                }

                rawRow.Add((s.ClientCpu * 100).ToString("F1"));
                rawRow.Add((s.NetworkUtilization * 100).ToString("F1"));

                if (summary.Options.SnmpEnabled == false)
                {
                    rawRow.Add(s.ServerCpu?.ToString("F1") ?? "N/A");
                    rawRow.Add(s.ServerMemoryMB?.ToString() ?? "N/A");
                }

                if (showServerIoR)
                {
                    rawRow.Add(s.ServerIoReadOps?.ToString() ?? "N/A");
                }
                if (showServerIoW)
                {
                    rawRow.Add(s.ServerIoWriteOps?.ToString() ?? "N/A");
                }

                if (summary.Options.SnmpEnabled)
                {
                    rawRow.Add(s.MachineCpu?.ToString("F1") ?? "N/A");
                    rawRow.Add(s.ProcessCpu?.ToString("F1") ?? "N/A");
                    rawRow.Add(s.ManagedMemoryMb?.ToString("F0") ?? "N/A");
                    rawRow.Add(s.UnmanagedMemoryMb?.ToString("F0") ?? "N/A");

                    if (summary.Options.Snmp.Profile == Util.SnmpProfile.Extended)
                    {
                        if (showDirtyMem)
                        {
                            rawRow.Add(s.DirtyMemoryMb?.ToString("F0") ?? "N/A");
                        }
                        if (showLoad1m)
                        {
                            rawRow.Add(s.Load1Min?.ToString("F2") ?? "N/A");
                        }
                        if (showSnmpErrors)
                        {
                            rawRow.Add(s.SnmpErrorsPerSec?.ToString("F1") ?? "N/A");
                        }
                    }
                }

                rawTable.AddRow(rawRow.ToArray());
            }
            AnsiConsole.Write(rawTable);
        }

        if (summary.Knee is { } knee)
        {
            var kneeMessage = latencyDisplay switch
            {
                LatencyDisplayType.Raw => $"KNEE at [bold]{knee.Concurrency}[/]: Thr={knee.Throughput:F0}/s, p95={knee.Raw.P95:F1}ms\nReason: {knee.Reason}",
                LatencyDisplayType.Both => $"KNEE at [bold]{knee.Concurrency}[/]: Thr={knee.Throughput:F0}/s, p95={knee.Normalized.P95:F1}ms (raw:{knee.Raw.P95:F1}ms)\nReason: {knee.Reason}",
                _ => $"KNEE at [bold]{knee.Concurrency}[/]: Thr={knee.Throughput:F0}/s, p95={knee.Normalized.P95:F1}ms\nReason: {knee.Reason}"
            };
            var panel = new Panel(kneeMessage)
                .Header("Knee Summary").BorderColor(Color.Yellow);
            AnsiConsole.Write(panel);
        }

        AnsiConsole.MarkupLine($"[bold]Verdict:[/]\n {summary.Verdict}");

        if (maxNetUtil >= 0.80 && summary.Options.NetworkLimitedMode == false)
        {
            AnsiConsole.MarkupLine("[yellow]WARNING: Network-limited. >80% link utilization. Identity runs may be hose-limited.[/]");
        }

        foreach (var w in analysis.Warnings)
            AnsiConsole.MarkupLine($"[yellow]WARNING:[/]\n {w}");

        foreach (var w in CheckForSnmpDiscrepancy(summary))
            AnsiConsole.MarkupLine($"[yellow]WARNING:[/]\n {w}");

        if (analysis.UnreliableBeyondKnee)
            AnsiConsole.MarkupLine("[italic]Beyond limits = unreliable.[/]");
    }

    private static IEnumerable<string> CheckForSnmpDiscrepancy(BenchmarkSummary summary)
    {
        if (summary.Options.SnmpEnabled == false || summary.Options.Snmp.Profile != Util.SnmpProfile.Extended)
            yield break;

        foreach (var s in summary.Steps)
        {
            if (s.Throughput > 0 && s.ServerSnmpRequestsPerSec.HasValue && s.ServerSnmpRequestsPerSec > 0)
            {
                var clientRate = s.Throughput;
                var serverRate = s.ServerSnmpRequestsPerSec.Value;

                var diff = Math.Abs(clientRate - serverRate);
                var avg = (clientRate + serverRate) / 2;

                if (avg > 0 && (diff / avg) > 0.10)
                {
                    yield return $"SNMP request rate discrepancy detected at concurrency {s.Concurrency}. Client: {clientRate:F0} req/s, Server: {serverRate:F0} req/s. This may indicate that the server-side metrics are not reliable.";
                }
            }
        }
    }
}