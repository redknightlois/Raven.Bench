using RavenBench.Core;

namespace RavenBench.Cli;

public sealed class ClosedCommand : RunCommandBase<ClosedSettings>
{
    protected override RunOptions BuildRunOptions(ClosedSettings settings) => settings.ToRunOptions();
}
