package org.apache.pulsar.protocols.grpc;

import io.grpc.*;
import io.grpc.stub.StreamObserver;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.mledger.util.SafeRun;
import org.apache.pulsar.broker.service.*;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.protocols.grpc.PulsarApi;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class PulsarGrpcService extends PulsarGrpcServiceGrpc.PulsarGrpcServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(PulsarGrpcService.class);

    private final BrokerService service;

    public PulsarGrpcService(BrokerService service) {
        this.service = service;
    }

    @Override
    public void hello(SimpleValue request, StreamObserver<SimpleValue> responseObserver) {
        service.getTopic(request.getName(), true)
            .whenComplete((topic, throwable) ->
                {
                    if (throwable == null) {
                        String state = topic.map(topic1 -> topic1.getInternalStats().state).orElse("null");
                        SimpleValue reply = SimpleValue.newBuilder().setName(state).build();
                        responseObserver.onNext(reply);
                        responseObserver.onCompleted();
                    } else {
                        responseObserver.onError(throwable);
                    }
                }
            );

    }

    @Override
    public StreamObserver<PulsarApi.BaseCommand> produce(StreamObserver<PulsarApi.BaseCommand> responseObserver) {
        // final long producerId = cmdProducer.getProducerId();
        final long producerId = 42L;
        // final long requestId = cmdProducer.getRequestId();
        final long requestId = 42L;
        // Use producer name provided by client if present
        // final String producerName = cmdProducer.hasProducerName() ? cmdProducer.getProducerName()
        //         : service.generateUniqueProducerName();
        final String producerName = service.generateUniqueProducerName();
        // final long epoch = cmdProducer.getEpoch();
        final long epoch = Instant.now().toEpochMilli();
        // final boolean userProvidedProducerName = cmdProducer.getUserProvidedProducerName();
        final boolean userProvidedProducerName = false;
        // final boolean isEncrypted = cmdProducer.getEncrypted();
        final boolean isEncrypted = false;
        // final Map<String, String> metadata = CommandUtils.metadataFromCommand(cmdProducer);
        final Map<String, String> metadata = Collections.emptyMap();
        // final SchemaData schema = cmdProducer.hasSchema() ? getSchema(cmdProducer.getSchema()) : null;
        final SchemaData schema = null;

        // TODO: get from gRPC Context
        SocketAddress remoteAddress = new InetSocketAddress("127.0.0.1", 12345);
        GrpcCnx cnx = new GrpcCnx(service, remoteAddress, responseObserver);

        // TODO: pass topic name in metadata
        String topic = "my-topic";
        String authRole = "admin";

        TopicName topicName;
        try {
            topicName = TopicName.get(topic);
        } catch (Exception e) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Failed to parse topic name '{}'", remoteAddress, topic, e);
            }

            // TODO: add InvalidTopicName code in error metadata so that it's passed to the client
            throw Status.INVALID_ARGUMENT
                .withDescription("Invalid topic name: " + e.getMessage())
                .withCause(e)
                .asRuntimeException();
        }

        Producer producer;
        try {
             producer = service.getOrCreateTopic(topicName.toString()).thenApply((Topic topik) -> {
                Producer grpcProducer = new GrpcProducer(topik, cnx, producerId, producerName, authRole,
                    isEncrypted, metadata, SchemaVersion.Empty, epoch, userProvidedProducerName);

                try {
                    // TODO : check that removeProducer is called even with early client disconnect
                    topik.addProducer(grpcProducer);
                    log.info("[{}] Created new producer: {}", remoteAddress, grpcProducer);
                    responseObserver.onNext(Commands.newProducerSuccess(requestId, producerName,
                        grpcProducer.getLastSequenceId(), grpcProducer.getSchemaVersion()));
                } catch (BrokerServiceException ise) {
                    log.error("[{}] Failed to add producer to topic {}: {}", remoteAddress, topicName,
                        ise.getMessage());
                    throw new RuntimeException(ise);
                }
                return grpcProducer;
                // TODO: make the code non-blocking and run on directExecutor (maybe)
            }).get();
        } catch (InterruptedException | ExecutionException e) {
            Throwable cause = e.getCause();
            if (!(cause instanceof BrokerServiceException.ServiceUnitNotReadyException)) {
                // Do not print stack traces for expected exceptions
                log.error("[{}] Failed to create topic {}", remoteAddress, topicName, e);
            }
            // TODO: add BrokerServiceException code in error metadata so that it's passed to the client
            throw Status.FAILED_PRECONDITION
                .withDescription(e.getMessage())
                .withCause(e)
                .asRuntimeException();
        }

        return new StreamObserver<PulsarApi.BaseCommand>() {
            @Override
            public void onNext(PulsarApi.BaseCommand cmd) {
                switch (cmd.getType()) {
                    case SEND:
                        PulsarApi.CommandSend send = cmd.getSend();
                        ByteBuffer buffer = send.getHeadersAndPayload().asReadOnlyByteBuffer();
                        ByteBuf headersAndPayload = Unpooled.wrappedBuffer(buffer);

                        /*if (producer.isNonPersistentTopic()) {
                            // avoid processing non-persist message if reached max concurrent-message limit
                            if (nonPersistentPendingMessages > MaxNonPersistentPendingMessages) {
                                final long producerId = send.getProducerId();
                                final long sequenceId = send.getSequenceId();
                                final long highestSequenceId = send.getHighestSequenceId();
                                service.getTopicOrderedExecutor().executeOrdered(producer.getTopic().getName(), SafeRun.safeRun(() -> {
                                    ctx.writeAndFlush(org.apache.pulsar.common.protocol.Commands.newSendReceipt(producerId, sequenceId, highestSequenceId, -1, -1), ctx.voidPromise());
                                }));
                                producer.recordMessageDrop(send.getNumMessages());
                                return;
                            } else {
                                nonPersistentPendingMessages++;
                            }
                        }

                        startSendOperation(producer);*/

                        // Persist the message
                        if (send.hasHighestSequenceId() && send.getSequenceId() <= send.getHighestSequenceId()) {
                            producer.publishMessage(send.getProducerId(), send.getSequenceId(), send.getHighestSequenceId(),
                                headersAndPayload, send.getNumMessages());
                        } else {
                            producer.publishMessage(send.getProducerId(), send.getSequenceId(), headersAndPayload, send.getNumMessages());
                        }

                }
            }

            @Override
            public void onError(Throwable throwable) {
                producer.close(true);
            }

            @Override
            public void onCompleted() {
                producer.close(true);
            }
        };
    }

}
