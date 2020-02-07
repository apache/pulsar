package org.apache.pulsar.protocols.grpc;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.netty.channel.EventLoopGroup;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.protocols.grpc.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.apache.pulsar.protocols.grpc.Constants.PRODUCER_PARAMS_CTX_KEY;
import static org.apache.pulsar.protocols.grpc.Constants.REMOTE_ADDRESS_CTX_KEY;
import static org.apache.pulsar.protocols.grpc.ServerErrors.newErrorMetadata;

public class PulsarGrpcService extends PulsarGrpc.PulsarImplBase {

    private static final Logger log = LoggerFactory.getLogger(PulsarGrpcService.class);

    private final BrokerService service;
    private final EventLoopGroup eventLoopGroup;

    public PulsarGrpcService(BrokerService service, EventLoopGroup eventLoopGroup) {
        this.service = service;
        this.eventLoopGroup = eventLoopGroup;
    }

    @Override
    public StreamObserver<CommandSend> produce(StreamObserver<SendResult> responseObserver) {
        CommandProducer cmdProducer = PRODUCER_PARAMS_CTX_KEY.get();
        final String topic = cmdProducer.getTopic();
        // Use producer name provided by client if present
        final String producerName = cmdProducer.hasProducerName() ? cmdProducer.getProducerName()
                : service.generateUniqueProducerName();
        final long epoch = cmdProducer.getEpoch();
        final boolean userProvidedProducerName = cmdProducer.getUserProvidedProducerName();
        final boolean isEncrypted = cmdProducer.getEncrypted();
        final Map<String, String> metadata = cmdProducer.getMetadataMap();

        // TODO: handle schema
        //final SchemaData schema = cmdProducer.hasSchema() ? getSchema(cmdProducer.getSchema()) : null;

        SocketAddress remoteAddress = REMOTE_ADDRESS_CTX_KEY.get();
        log.info("################# init 2" + Thread.currentThread().getName());

        GrpcCnx cnx = new GrpcCnx(service, remoteAddress, (ServerCallStreamObserver<SendResult>) responseObserver);

        // TODO: handle auth
        String authRole = "admin";

        TopicName topicName;
        try {
            topicName = TopicName.get(topic);
        } catch (Exception e) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Failed to parse topic name '{}'", remoteAddress, topic, e);
            }
            StatusRuntimeException statusException = Status.INVALID_ARGUMENT
                .withDescription("Invalid topic name: " + e.getMessage())
                .withCause(e)
                .asRuntimeException(newErrorMetadata(ServerError.InvalidTopicName));
            responseObserver.onError(statusException);
            return NoOpStreamObserver.create();
        }

        Producer producer;
        try {
             producer = service.getOrCreateTopic(topicName.toString()).thenApply((Topic topik) -> {
                Producer grpcProducer = new GrpcProducer(topik, cnx, producerName, authRole,
                    isEncrypted, metadata, SchemaVersion.Empty, epoch, userProvidedProducerName, eventLoopGroup.next());

                try {
                    // TODO : check that removeProducer is called even with early client disconnect
                    topik.addProducer(grpcProducer);
                    log.info("[{}] Created new producer: {}", remoteAddress, grpcProducer);
                    responseObserver.onNext(Commands.newProducerSuccess(producerName,
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

            StatusRuntimeException statusException = Status.FAILED_PRECONDITION
                .withDescription(cause.getMessage())
                .withCause(cause)
                .asRuntimeException(newErrorMetadata(BrokerServiceException.getClientErrorCode(cause)));
            responseObserver.onError(statusException);
            return NoOpStreamObserver.create();
        }

        return new StreamObserver<CommandSend>() {
            @Override
            public void onNext(CommandSend cmd) {
                producer.execute(() -> cnx.handleSend(cmd, producer));
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

    private static class NoOpStreamObserver<T> implements StreamObserver<T> {

        public static <T> NoOpStreamObserver<T> create() {
            return new NoOpStreamObserver<T>();
        }

        private NoOpStreamObserver() {
        }

        @Override
        public void onNext(T value) {
            // NoOp
        }


        @Override
        public void onError(Throwable t) {
            // NoOp
        }

        @Override
        public void onCompleted() {
            // NoOp
        }
    }

}
