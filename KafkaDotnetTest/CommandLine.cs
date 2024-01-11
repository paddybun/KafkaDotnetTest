using System.CommandLine;
using Serilog;

namespace KafkaDotnetTest;

public class CommandLine
{
    public async Task ParseCommandLineAsync(string[] args)
    {
        var fileOption = new System.CommandLine.Option<string?>(
            name: "--out",
            description: "The string to print to stdout.");

        var rootCommand = new RootCommand("Sample app for System.CommandLine");
        rootCommand.AddOption(fileOption);

        rootCommand.SetHandler((file) =>
            {
                Log.Logger.Information(file!);
            },
            fileOption);

        await rootCommand.InvokeAsync(args);
    }
}