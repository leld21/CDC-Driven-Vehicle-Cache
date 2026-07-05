using Confluent.Kafka;
using Nstech.Worker.Cdc;
using Nstech.Worker.Options;
using Nstech.Worker.Services;

namespace Nstech.Worker.Consumers;

public abstract class CdcTopicConsumerBase(
    IOptions<KafkaOptions> kafkaOptions,
    ICdcPipeline pipeline,
    ILogger logger,
    string topic,
    CdcStream stream) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var options = kafkaOptions.Value;
        var config = new ConsumerConfig
        {
            BootstrapServers = options.BootstrapServers,
            GroupId = options.GroupId,
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnablePartitionEof = false
        };

        await KafkaTopicWaiter.WaitForTopicAsync(config.BootstrapServers, topic, logger, stoppingToken)
            .ConfigureAwait(false);

        using var consumer = new ConsumerBuilder<string, string>(config)
            .SetErrorHandler((_, error) =>
            {
                if (!error.IsFatal)
                    logger.LogWarning("Kafka consumer error: {Reason}", error.Reason);
            })
            .Build();

        consumer.Subscribe(topic);
        logger.LogInformation("Subscribed to {Topic} stream={Stream} group={GroupId}", topic, stream, options.GroupId);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                ConsumeResult<string, string> result;
                try
                {
                    result = consumer.Consume(stoppingToken);
                }
                catch (ConsumeException ex) when (ex.Error.Code == ErrorCode.UnknownTopicOrPart)
                {
                    logger.LogWarning(
                        "Topic {Topic} disappeared or is not ready ({Reason}); waiting...",
                        topic,
                        ex.Error.Reason);
                    consumer.Unsubscribe();
                    await KafkaTopicWaiter.WaitForTopicAsync(config.BootstrapServers, topic, logger, stoppingToken)
                        .ConfigureAwait(false);
                    consumer.Subscribe(topic);
                    continue;
                }
                catch (ConsumeException ex) when (ex.Error.IsFatal)
                {
                    logger.LogError(ex, "Fatal consume error on {Topic}", topic);
                    throw;
                }

                if (result.IsPartitionEOF)
                    continue;

                logger.LogDebug(
                    "Received message on {Topic} partition={Partition} offset={Offset} key={Key}",
                    topic,
                    result.Partition.Value,
                    result.Offset.Value,
                    result.Message.Key);

                await pipeline.ProcessAsync(stream, result, stoppingToken).ConfigureAwait(false);

                // ADR-002: commit only after the Valkey write path completes.
                consumer.Commit(result);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            logger.LogInformation("Stopping consumer for {Topic}", topic);
        }
        finally
        {
            consumer.Close();
        }
    }
}

public sealed class VehicleTopicConsumer(
    IOptions<KafkaOptions> kafkaOptions,
    ICdcPipeline pipeline,
    ILogger<VehicleTopicConsumer> logger)
    : CdcTopicConsumerBase(
        kafkaOptions,
        pipeline,
        logger,
        kafkaOptions.Value.VehiclesTopic,
        CdcStream.Vehicles);

public sealed class PositionTopicConsumer(
    IOptions<KafkaOptions> kafkaOptions,
    ICdcPipeline pipeline,
    ILogger<PositionTopicConsumer> logger)
    : CdcTopicConsumerBase(
        kafkaOptions,
        pipeline,
        logger,
        kafkaOptions.Value.PositionsTopic,
        CdcStream.Positions);
