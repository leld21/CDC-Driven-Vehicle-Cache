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
        var step = System.Text.Json.JsonSerializer.Deserialize<VehicleStep>(args[1])
            ?? throw new InvalidOperationException("Invalid --step JSON");
        Console.WriteLine($"vehicle step seq={step.Seq} op={step.Op} id={step.Id}");
        await writer.ExecuteVehicleStepAsync(step);
        return 0;
    }

    if (args[0] == "--scenario")
    {
        var path = args.Length >= 2 ? args[1] : defaultScenario;
        var scenario = ScenarioLoader.Load(path);
        var steps = scenario.Vehicles.OrderBy(v => v.Seq).ToList();
        Console.WriteLine($"VehicleWriter: {steps.Count} steps from {path}");
        foreach (var step in steps)
        {
            Console.WriteLine($"==> seq={step.Seq} op={step.Op} id={step.Id} state={step.State}");
            await writer.ExecuteVehicleStepAsync(step);
        }
        return 0;
    }

    Console.Error.WriteLine($"Unknown arguments: {string.Join(' ', args)}");
    PrintUsage();
    return 1;
}
catch (Exception ex)
{
    Console.Error.WriteLine($"VehicleWriter failed: {ex.Message}");
    return 1;
}

static void PrintUsage()
{
    Console.WriteLine("""
        Vehicle Writer — inserts/updates/deletes vehicles in PostgreSQL (Npgsql).

        Usage:
          dotnet run --project src/VehicleWriter -- --scenario [path]
          dotnet run --project src/VehicleWriter -- --step '{"seq":1,"op":"insert","id":1,"state":"REGISTERED"}'

        Environment:
          POSTGRES__CONNECTIONSTRING  default Host=localhost;Port=5432;Database=nstech;...
        """);
}
