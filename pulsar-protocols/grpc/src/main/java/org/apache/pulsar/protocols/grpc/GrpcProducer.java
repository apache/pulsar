package org.apache.pulsar.protocols.grpc;

import io.netty.channel.EventLoop;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;

import java.util.Map;

public class GrpcProducer extends Producer {

    private final GrpcCnx cnx;
    private final EventLoop eventLoop;

    public GrpcProducer(Topic topic, GrpcCnx cnx, String producerName, String appId, boolean isEncrypted, Map<String, String> metadata, SchemaVersion schemaVersion, long epoch, boolean userProvidedProducerName, EventLoop eventLoop) {
        super(topic, cnx, 0L, producerName, appId, isEncrypted, metadata, schemaVersion, epoch, userProvidedProducerName);
        this.cnx = cnx;
        this.eventLoop = eventLoop;
    }

    @Override
    protected void sendError(long producerId, long sequenceId, org.apache.pulsar.common.api.proto.PulsarApi.ServerError serverError, String message) {
        cnx.getResponseObserver().onNext(Commands.newSendError(sequenceId, ServerErrors.convert(serverError), message));
    }

    @Override
    public void execute(Runnable runnable) {
        eventLoop.execute(runnable);
    }

    @Override
    protected void sendReceipt(long sequenceId, long highestSequenceId, long ledgerId, long entryId) {
        cnx.getResponseObserver().onNext(Commands.newSendReceipt(sequenceId, highestSequenceId, ledgerId, entryId));
    }
}
