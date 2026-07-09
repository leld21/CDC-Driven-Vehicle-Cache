using Confluent.Kafka;
using Nstech.Worker.Cdc;
using Nstech.Worker.Domain;

namespace Nstech.Worker.Services;

public interface ICdcPipeline
{
    Task<WriteOutcome> ProcessAsync(
        CdcStream stream,
        ConsumeResult<string, string> message,
        CancellationToken cancellationToken);
}

public sealed class CdcPipeline(
    IPerVehicleProjector projector,
    ICacheWriter cacheWriter,
    ILogger<CdcPipeline> logger) : ICdcPipeline
{
    public async Task<WriteOutcome> ProcessAsync(
        CdcStream stream,
        ConsumeResult<string, string> message,
        CancellationToken cancellationToken)
    {
        var request = projector.Project(stream, message.Message.Value, message.Message.Key);

        var outcome = await cacheWriter.WriteAsync(request, cancellationToken).ConfigureAwait(false);

        logger.LogInformation(
            "stream={Stream} topic={Topic} partition={Partition} offset={Offset} kind={Kind} vehicleId={VehicleId} outcome={Outcome} reason={Reason}",
            stream,
            message.Topic,
            message.Partition.Value,
            message.Offset.Value,
            request.Kind,
            request.VehicleId,
            outcome,
            request.Reason);

        return outcome;
    }
}
