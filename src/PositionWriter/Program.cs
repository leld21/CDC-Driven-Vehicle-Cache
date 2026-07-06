using Nstech.Scenario;
using Nstech.Scenario.Models;

const string defaultScenario = "scenarios/e2e.json";

if (args.Length == 0)
{
    PrintUsage();
    return 1;
}

var connectionString = Environment.GetEnvironmentVariable("POSTGRES__CONNECTIONSTRING")
    ?? "Host=localhost;Port=5432;Database=nstech;Username=nstech;Password=nstech";

var writer = new PostgresScenarioWriter(connectionString);

try
{
    if (args[0] == "--help" || args[0] == "-h")
    {
        PrintUsage();
        return 0;
    }

    if (args[0] == "--step" && args.Length >= 2)
    {
        var step = System.Text.Json.JsonSerializer.Deserialize<PositionStep>(args[1])
            ?? throw new InvalidOperationException("Invalid --step JSON");
        var scenarioPath = Environment.GetEnvironmentVariable("SCENARIO_PATH") ?? defaultScenario;
        var scenario = File.Exists(scenarioPath) ? ScenarioLoader.Load(scenarioPath) : new E2eScenario();
        Console.WriteLine($"position step seq={step.Seq} vehicleId={step.VehicleId}");
        await writer.ExecutePositionStepAsync(step, scenario);
        return 0;
    }

    if (args[0] == "--scenario")
    {
        var path = args.Length >= 2 ? args[1] : defaultScenario;
        var scenario = ScenarioLoader.Load(path);
        var steps = scenario.Positions.OrderBy(p => p.Seq).ToList();
        Console.WriteLine($"PositionWriter: {steps.Count} steps from {path}");
        foreach (var step in steps)
        {
            Console.WriteLine($"==> seq={step.Seq} vehicleId={step.VehicleId} recordedAt={step.RecordedAt}");
            await writer.ExecutePositionStepAsync(step, scenario);
        }
        return 0;
    }

    if (args[0] == "--rate")
    {
        var rate = args.Length >= 2 && int.TryParse(args[1], out var r) ? r : 5;
        var vehicleId = args.Length >= 3 && long.TryParse(args[2], out var vid) ? vid : 1L;
        Console.WriteLine($"PositionWriter rate mode: {rate} inserts/s for vehicle {vehicleId} (Ctrl+C to stop)");
        var scenario = new E2eScenario();
        var baseTime = DateTimeOffset.UtcNow;
        var i = 0;
        using var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };
        while (!cts.Token.IsCancellationRequested)
        {
            var step = new PositionStep
            {
                Seq = i,
                VehicleId = vehicleId,
                Lat = -23.5 + i * 0.001,
                Lng = -46.6 + i * 0.001,
                RecordedAt = baseTime.AddSeconds(i).ToString("O")
            };
            await writer.ExecutePositionStepAsync(step, scenario, cts.Token);
            i++;
            await Task.Delay(TimeSpan.FromSeconds(1.0 / rate), cts.Token);
        }
        return 0;
    }

    Console.Error.WriteLine($"Unknown arguments: {string.Join(' ', args)}");
    PrintUsage();
    return 1;
}
catch (OperationCanceledException)
{
    return 0;
}
catch (Exception ex)
{
    Console.Error.WriteLine($"PositionWriter failed: {ex.Message}");
    return 1;
}

static void PrintUsage()
{
    Console.WriteLine("""
        Position Writer — inserts positions in PostgreSQL (Npgsql).

        Usage:
          dotnet run --project src/PositionWriter -- --scenario [path]
          dotnet run --project src/PositionWriter -- --step '{"seq":2,"vehicleId":1,...}'
          dotnet run --project src/PositionWriter -- --rate [perSecond] [vehicleId]

        Environment:
          POSTGRES__CONNECTIONSTRING  default Host=localhost;Port=5432;Database=nstech;...
          SCENARIO_PATH               used by --step for duplicateOf resolution
        """);
}
