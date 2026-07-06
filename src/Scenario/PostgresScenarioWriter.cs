using Npgsql;
using Nstech.Scenario.Models;

namespace Nstech.Scenario;

public sealed class PostgresScenarioWriter(string connectionString)
{
    public async Task ExecuteVehicleStepAsync(VehicleStep step, CancellationToken cancellationToken = default)
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync(cancellationToken).ConfigureAwait(false);

        switch (step.Op.ToLowerInvariant())
        {
            case "insert":
                await ExecuteAsync(conn,
                    "INSERT INTO vehicles (id, state) VALUES (@id, @state)",
                    cmd =>
                    {
                        cmd.Parameters.AddWithValue("id", step.Id);
                        cmd.Parameters.AddWithValue("state", step.State ?? throw new InvalidOperationException("state required"));
                    },
                    cancellationToken).ConfigureAwait(false);
                break;
            case "update":
                await ExecuteAsync(conn,
                    "UPDATE vehicles SET state = @state, updated_at = now() WHERE id = @id",
                    cmd =>
                    {
                        cmd.Parameters.AddWithValue("id", step.Id);
                        cmd.Parameters.AddWithValue("state", step.State ?? throw new InvalidOperationException("state required"));
                    },
                    cancellationToken).ConfigureAwait(false);
                break;
            case "delete":
                await ExecuteAsync(conn,
                    "DELETE FROM vehicles WHERE id = @id",
                    cmd => cmd.Parameters.AddWithValue("id", step.Id),
                    cancellationToken).ConfigureAwait(false);
                break;
            default:
                throw new NotSupportedException($"Unknown vehicle op: {step.Op}");
        }
    }

    public async Task ExecutePositionStepAsync(
        PositionStep step,
        E2eScenario scenario,
        CancellationToken cancellationToken = default)
    {
        var lat = step.Lat;
        var lng = step.Lng;
        var recordedAt = step.RecordedAt;

        if (step.DuplicateOf is int dupSeq)
        {
            var source = scenario.Positions.FirstOrDefault(p => p.Seq == dupSeq)
                ?? throw new InvalidOperationException($"duplicateOf seq {dupSeq} not found");
            lat = source.Lat;
            lng = source.Lng;
            recordedAt = source.RecordedAt;
        }

        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
        await ExecuteAsync(conn,
            "INSERT INTO positions (vehicle_id, lat, lng, recorded_at) VALUES (@vehicleId, @lat, @lng, @recordedAt)",
            cmd =>
            {
                cmd.Parameters.AddWithValue("vehicleId", step.VehicleId);
                cmd.Parameters.AddWithValue("lat", lat);
                cmd.Parameters.AddWithValue("lng", lng);
                cmd.Parameters.AddWithValue("recordedAt", DateTimeOffset.Parse(recordedAt).UtcDateTime);
            },
            cancellationToken).ConfigureAwait(false);
    }

    private static async Task ExecuteAsync(
        NpgsqlConnection conn,
        string sql,
        Action<NpgsqlCommand> bind,
        CancellationToken cancellationToken)
    {
        await using var cmd = new NpgsqlCommand(sql, conn);
        bind(cmd);
        var rows = await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        Console.WriteLine($"OK rows={rows}");
    }
}
