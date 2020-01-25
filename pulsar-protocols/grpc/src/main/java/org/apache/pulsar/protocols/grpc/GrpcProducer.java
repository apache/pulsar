package org.apache.pulsar.protocols.grpc;

import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class GrpcProducer extends Producer {

    private final GrpcCnx cnx;

    // TODO: replace by gRPC executor. Maybe run gRPC service directly from Netty (directExecutor())
    private final ExecutorService executorService = Executors.newCachedThreadPool();

    public GrpcProducer(Topic topic, GrpcCnx cnx, long producerId, String producerName, String appId, boolean isEncrypted, Map<String, String> metadata, SchemaVersion schemaVersion, long epoch, boolean userProvidedProducerName) {
        super(topic, cnx, producerId, producerName, appId, isEncrypted, metadata, schemaVersion, epoch, userProvidedProducerName);
        this.cnx = cnx;
    }

    @Override
    protected void sendError(long producerId, long sequenceId, org.apache.pulsar.common.api.proto.PulsarApi.ServerError serverError, String message) {
        cnx.getResponseObserver().onNext(Commands.newSendError(sequenceId, ServerErrors.convert(serverError), message));
    }

    @Override
    protected void execute(Runnable runnable) {
        executorService.execute(runnable);
    }

    @Override
    protected void sendReceipt(long sequenceId, long highestSequenceId, long ledgerId, long entryId) {
        cnx.getResponseObserver().onNext(Commands.newSendReceipt(sequenceId, highestSequenceId, ledgerId, entryId));
    }
}
