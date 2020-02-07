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
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.protocols.grpc.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.pulsar.protocols.grpc.Constants.PRODUCER_PARAMS_CTX_KEY;
import static org.apache.pulsar.protocols.grpc.Constants.REMOTE_ADDRESS_CTX_KEY;

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
            StatusRuntimeException statusException = Commands.newStatusException(Status.INVALID_ARGUMENT,
                "Invalid topic name: " + e.getMessage(), e, ServerError.InvalidTopicName);
            responseObserver.onError(statusException);
            return NoOpStreamObserver.create();
        }

        CompletableFuture<Producer> producerFuture = new CompletableFuture<>();
        service.getOrCreateTopic(topicName.toString()).thenAccept((Topic topik) -> {
            // Before creating producer, check if backlog quota exceeded on topic
            if (topik.isBacklogQuotaExceeded(producerName)) {
                IllegalStateException illegalStateException = new IllegalStateException(
                    "Cannot create producer on topic with backlog quota exceeded");
                BacklogQuota.RetentionPolicy retentionPolicy = topik.getBacklogQuota().getPolicy();
                if (retentionPolicy == BacklogQuota.RetentionPolicy.producer_request_hold) {
                    StatusRuntimeException statusException = Commands.newStatusException(Status.FAILED_PRECONDITION, illegalStateException, ServerError.ProducerBlockedQuotaExceededError);
                    responseObserver.onError(statusException);
                } else if (retentionPolicy == BacklogQuota.RetentionPolicy.producer_exception) {
                    StatusRuntimeException statusException = Commands.newStatusException(Status.FAILED_PRECONDITION, illegalStateException, ServerError.ProducerBlockedQuotaExceededException);
                    responseObserver.onError(statusException);
                }
                producerFuture.completeExceptionally(illegalStateException);;
            }

            // Check whether the producer will publish encrypted messages or not
            if (topik.isEncryptionRequired() && !isEncrypted) {
                String msg = String.format("Encryption is required in %s", topicName);
                log.warn("[{}] {}", remoteAddress, msg);
                StatusRuntimeException statusException = Commands.newStatusException(Status.INVALID_ARGUMENT, msg, null, ServerError.ProducerBlockedQuotaExceededException);
                responseObserver.onError(statusException);
            }

            Producer producer = new GrpcProducer(topik, cnx, producerName, authRole,
                isEncrypted, metadata, SchemaVersion.Empty, epoch, userProvidedProducerName, eventLoopGroup.next());

            try {
                // TODO : check that removeProducer is called even with early client disconnect
                topik.addProducer(producer);
                log.info("[{}] Created new producer: {}", remoteAddress, producer);
                producerFuture.complete(producer);
                responseObserver.onNext(Commands.newProducerSuccess(producerName,
                    producer.getLastSequenceId(), producer.getSchemaVersion()));
            } catch (BrokerServiceException ise) {
                log.error("[{}] Failed to add producer to topic {}: {}", remoteAddress, topicName,
                    ise.getMessage());
                StatusRuntimeException statusException = Commands.newStatusException(Status.FAILED_PRECONDITION, ise,
                    ServerErrors.convert(BrokerServiceException.getClientErrorCode(ise)));
                responseObserver.onError(statusException);
                producerFuture.completeExceptionally(ise);
            }
        }).exceptionally(exception -> {
            Throwable cause = exception.getCause();
            if (!(cause instanceof BrokerServiceException.ServiceUnitNotReadyException)) {
                // Do not print stack traces for expected exceptions
                log.error("[{}] Failed to create topic {}", remoteAddress, topicName, exception);
            }

            if (producerFuture.completeExceptionally(exception)) {
                StatusRuntimeException statusException = Commands.newStatusException(Status.FAILED_PRECONDITION, cause,
                    ServerErrors.convert(BrokerServiceException.getClientErrorCode(cause)));
                responseObserver.onError(statusException);
            }
            return null;
        });

        return new StreamObserver<CommandSend>() {
            @Override
            public void onNext(CommandSend cmd) {
                if (!producerFuture.isDone() || producerFuture.isCompletedExceptionally()) {
                    log.warn("[{}] Producer unavailable", remoteAddress);
                    return;
                }
                Producer producer = producerFuture.getNow(null);
                producer.execute(() -> cnx.handleSend(cmd, producer));
            }

            @Override
            public void onError(Throwable throwable) {
                closeProduce(producerFuture, remoteAddress);
            }

            @Override
            public void onCompleted() {
                closeProduce(producerFuture, remoteAddress);
            }
        };
    }

    private void closeProduce(CompletableFuture<Producer> producerFuture, SocketAddress remoteAddress) {
        if (!producerFuture.isDone() && producerFuture
            .completeExceptionally(new IllegalStateException("Closed producer before creation was complete"))) {
            // We have received a request to close the producer before it was actually completed, we have marked the
            // producer future as failed and we can tell the client the close operation was successful.
            log.info("[{}] Closed producer before its creation was completed", remoteAddress);
            return;
        } else if (producerFuture.isCompletedExceptionally()) {
            log.info("[{}] Closed producer that already failed to be created", remoteAddress);
            return;
        }

        // Proceed with normal close, the producer
        Producer producer = producerFuture.getNow(null);
        log.info("[{}][{}] Closing producer on cnx {}", producer.getTopic(), producer.getProducerName(), remoteAddress);
        producer.close(true);
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
