using RavenBench.Core;

namespace RavenBench.Cli;

public sealed class RateCommand : RunCommandBase<RateSettings>
{
    protected override RunOptions BuildRunOptions(RateSettings settings) => settings.ToRunOptions();
}
