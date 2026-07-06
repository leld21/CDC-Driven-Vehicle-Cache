using System.Text.Json;
using Nstech.Scenario.Models;

namespace Nstech.Scenario;

public static class ScenarioLoader
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        ReadCommentHandling = JsonCommentHandling.Skip,
        AllowTrailingCommas = true
    };

    public static E2eScenario Load(string path)
    {
        var json = File.ReadAllText(path);
        return JsonSerializer.Deserialize<E2eScenario>(json, JsonOptions)
            ?? throw new InvalidOperationException($"Failed to deserialize scenario: {path}");
    }

    public static IReadOnlyList<ScenarioTimelineStep> BuildTimeline(E2eScenario scenario)
    {
        var steps = new List<ScenarioTimelineStep>();
        foreach (var v in scenario.Vehicles)
            steps.Add(new ScenarioTimelineStep(v.Seq, "vehicles", v));
        foreach (var p in scenario.Positions)
            steps.Add(new ScenarioTimelineStep(p.Seq, "positions", p));
        return steps.OrderBy(s => s.Seq).ToList();
    }
}
