using System.Text.Json;
using System.Text.Json.Serialization;

namespace Nstech.Scenario.Models;

public sealed class E2eScenario
{
    [JsonPropertyName("name")]
    public string Name { get; init; } = "e2e";

    [JsonPropertyName("description")]
    public string? Description { get; init; }

    [JsonPropertyName("vehicles")]
    public List<VehicleStep> Vehicles { get; init; } = [];

    [JsonPropertyName("positions")]
    public List<PositionStep> Positions { get; init; } = [];

    [JsonPropertyName("expected")]
    public Dictionary<string, ExpectedVehicleState>? Expected { get; init; }
}

public sealed class VehicleStep
{
    [JsonPropertyName("seq")]
    public int Seq { get; init; }

    [JsonPropertyName("op")]
    public string Op { get; init; } = "";

    [JsonPropertyName("id")]
    public long Id { get; init; }

    [JsonPropertyName("state")]
    public string? State { get; init; }

    [JsonPropertyName("note")]
    public string? Note { get; init; }
}

public sealed class PositionStep
{
    [JsonPropertyName("seq")]
    public int Seq { get; init; }

    [JsonPropertyName("vehicleId")]
    public long VehicleId { get; init; }

    [JsonPropertyName("lat")]
    public double Lat { get; init; }

    [JsonPropertyName("lng")]
    public double Lng { get; init; }

    [JsonPropertyName("recordedAt")]
    public string RecordedAt { get; init; } = "";

    [JsonPropertyName("duplicateOf")]
    public int? DuplicateOf { get; init; }

    [JsonPropertyName("note")]
    public string? Note { get; init; }
}

public sealed class ExpectedVehicleState
{
    [JsonPropertyName("state")]
    public string? State { get; init; }

    [JsonPropertyName("deleted")]
    public string Deleted { get; init; } = "0";

    [JsonPropertyName("lat")]
    public string? Lat { get; init; }

    [JsonPropertyName("lng")]
    public string? Lng { get; init; }

    [JsonPropertyName("recordedAt")]
    public string? RecordedAt { get; init; }
}

public sealed record ScenarioTimelineStep(int Seq, string Stream, object Step);
