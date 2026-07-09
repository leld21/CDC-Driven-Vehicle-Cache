using Nstech.Worker.Domain;

namespace Nstech.Worker.Services;

public interface ICacheWriter
{
    Task<WriteOutcome> WriteAsync(CacheWriteRequest request, CancellationToken cancellationToken);
}

public sealed class ValkeyCacheWriter(IConnectionMultiplexer multiplexer) : ICacheWriter
{
    // docs/spec.md §7.3 — ordering + idempotency enforced atomically server-side.
    private const string VehicleScript = """
        local cur = tonumber(redis.call('HGET', KEYS[1], 'vehicleLsn'))
        local lsn = tonumber(ARGV[1])
        if cur ~= nil and lsn <= cur then return 0 end
        redis.call('HSET', KEYS[1], 'vehicleLsn', ARGV[1])
        if ARGV[2] == 'delete' then
          redis.call('HSET', KEYS[1], 'deleted', '1', 'deletedLsn', ARGV[1])
        else
          redis.call('HSET', KEYS[1], 'state', ARGV[3], 'stateUpdatedAt', ARGV[4], 'deleted', '0')
        end
        return 1
        """;

    private const string PositionScript = """
        local curTs  = tonumber(redis.call('HGET', KEYS[1], 'recordedAt'))
        local curLsn = tonumber(redis.call('HGET', KEYS[1], 'positionLsn'))
        local ts  = tonumber(ARGV[1])
        local lsn = tonumber(ARGV[2])
        local apply = (curTs == nil or ts > curTs)
                   or (ts == curTs and (curLsn == nil or lsn > curLsn))
        if not apply then return 0 end
        redis.call('HSET', KEYS[1], 'recordedAt', ARGV[1], 'positionLsn', ARGV[2],
                                    'lat', ARGV[3], 'lng', ARGV[4])
        return 1
        """;

    public async Task<WriteOutcome> WriteAsync(CacheWriteRequest request, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        return request.Kind switch
        {
            CacheWriteKind.TombstoneNoOp => WriteOutcome.NoOp,
            CacheWriteKind.Ignored => WriteOutcome.Ignored,
            CacheWriteKind.VehicleUpsert => await WriteVehicleAsync(request, op: "upsert", cancellationToken),
            CacheWriteKind.VehicleDelete => await WriteVehicleAsync(request, op: "delete", cancellationToken),
            CacheWriteKind.PositionUpdate => await WritePositionAsync(request, cancellationToken),
            _ => WriteOutcome.Ignored
        };
    }

    private async Task<WriteOutcome> WriteVehicleAsync(
        CacheWriteRequest request,
        string op,
        CancellationToken cancellationToken)
    {
        var db = multiplexer.GetDatabase();
        var key = VehicleKey(request.VehicleId);
        var lsn = request.Lsn!.Value.ToString(System.Globalization.CultureInfo.InvariantCulture);
        RedisValue[] argv = op == "delete"
            ? [lsn, op, "", ""]
            :
            [
                lsn,
                op,
                request.State ?? string.Empty,
                request.StateUpdatedAtMs!.Value.ToString(System.Globalization.CultureInfo.InvariantCulture)
            ];

        var result = await db.ScriptEvaluateAsync(VehicleScript, [key], argv).ConfigureAwait(false);

        cancellationToken.ThrowIfCancellationRequested();
        return (int)result == 1 ? WriteOutcome.Applied : WriteOutcome.SkippedStale;
    }

    private async Task<WriteOutcome> WritePositionAsync(
        CacheWriteRequest request,
        CancellationToken cancellationToken)
    {
        var db = multiplexer.GetDatabase();
        var key = VehicleKey(request.VehicleId);
        var result = await db.ScriptEvaluateAsync(
            PositionScript,
            [key],
            [
                request.RecordedAtMs!.Value.ToString(System.Globalization.CultureInfo.InvariantCulture),
                request.PositionLsn!.Value.ToString(System.Globalization.CultureInfo.InvariantCulture),
                request.Lat!.Value.ToString(System.Globalization.CultureInfo.InvariantCulture),
                request.Lng!.Value.ToString(System.Globalization.CultureInfo.InvariantCulture)
            ]).ConfigureAwait(false);

        cancellationToken.ThrowIfCancellationRequested();
        return (int)result == 1 ? WriteOutcome.Applied : WriteOutcome.SkippedStale;
    }

    private static RedisKey VehicleKey(long vehicleId) => $"vehicle:{vehicleId}";
}
