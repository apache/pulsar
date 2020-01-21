package org.apache.pulsar.protocols.grpc;

import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.apache.pulsar.broker.service.*;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.protocols.grpc.PulsarApi;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;

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
        try {
            TopicName topicName = TopicName.get(topic);
            service.getOrCreateTopic(topicName.toString()).thenAccept((Topic topik) -> {
                Producer producer = new GrpcProducer(topik, cnx, producerId, producerName, authRole,
                    isEncrypted, metadata, SchemaVersion.Empty, epoch, userProvidedProducerName);
            });
        } catch (Exception e) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Failed to parse topic name '{}'", remoteAddress, topic, e);
            }
            responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT.withCause(e)));
        }





        return new StreamObserver<PulsarApi.BaseCommand>() {
            @Override
            public void onNext(PulsarApi.BaseCommand baseCommand) {

            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onCompleted() {

            }
        };
    }

}
