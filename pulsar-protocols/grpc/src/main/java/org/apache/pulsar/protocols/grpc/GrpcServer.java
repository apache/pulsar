package org.apache.pulsar.protocols.grpc;

import io.grpc.*;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.nio.NioEventLoopGroup;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands.ChecksumType;
import org.apache.pulsar.common.util.NettySslContextBuilder;
import org.apache.pulsar.protocols.grpc.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.pulsar.protocols.grpc.Constants.PRODUCER_PARAMS_METADATA_KEY;


public class GrpcServer {

    private static final Logger log = LoggerFactory.getLogger(GrpcServer.class);

    private final ServiceConfiguration configuration;
    private Server server = null;
    private Server tlsServer = null;

    public GrpcServer(ServiceConfiguration configuration) {
        this.configuration = configuration;
    }

    public void start(BrokerService service) throws IOException, GeneralSecurityException {
        PulsarGrpcService pulsarGrpcService = new PulsarGrpcService(service, configuration, new NioEventLoopGroup());
        List<ServerInterceptor> interceptors = Arrays.asList(new GrpcServerInterceptor());
        if (service.isAuthenticationEnabled()) {
            interceptors.add(new AuthenticationInterceptor(service));
        }

        Optional<Integer> grpcServicePort = Optional.ofNullable(configuration.getProperties().getProperty("grpcServicePort"))
            .map(Integer::parseInt);

        if (grpcServicePort.isPresent()) {
            Integer port = grpcServicePort.get();
            server = NettyServerBuilder.forPort(port)
                .addService(ServerInterceptors.intercept(pulsarGrpcService, interceptors))
                .build()
                .start();
            log.info("gRPC Service started, listening on " + port);
        }

        Optional<Integer> grpcServicePortTls = Optional.ofNullable(configuration.getProperties().getProperty("grpcServicePortTls"))
            .map(Integer::parseInt);

        if (grpcServicePortTls.isPresent()) {
            Integer port = grpcServicePortTls.get();
            NettySslContextBuilder sslCtxRefresher = new NettySslContextBuilder(configuration.isTlsAllowInsecureConnection(),
                configuration.getTlsTrustCertsFilePath(), configuration.getTlsCertificateFilePath(),
                configuration.getTlsKeyFilePath(), configuration.getTlsCiphers(), configuration.getTlsProtocols(),
                configuration.isTlsRequireTrustedClientCertOnConnect(),
                configuration.getTlsCertRefreshCheckDurationSec());

            tlsServer = NettyServerBuilder.forPort(port)
                .addService(ServerInterceptors.intercept(pulsarGrpcService, interceptors))
                .sslContext(sslCtxRefresher.get())
                .build()
                .start();
            log.info("gRPC TLS Service started, listening on " + port);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // Use stderr here since the logger may have been reset by its JVM shutdown hook.
            System.err.println("shutting down gRPC server since JVM is shutting down");
            try {
                GrpcServer.this.stop();
            } catch (InterruptedException e) {
                e.printStackTrace(System.err);
            }
            System.err.println("gRPC server shut down");
        }));


    }

    public void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown();
        }
        if (tlsServer != null) {
            tlsServer.shutdown();
        }
        if (server != null) {
            server.awaitTermination(30, TimeUnit.SECONDS);
        }
        if (tlsServer != null) {
            tlsServer.awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    // TO BE REMOVED : For test
    public static void main(String[] args) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 50444).usePlaintext()
            .build();

        Metadata headers = new Metadata();
        CommandProducer producerParams = CommandProducer.newBuilder()
            .setTopic("my-topic2")
            .setProducerName("my-producer")
            .build();
        headers.put(PRODUCER_PARAMS_METADATA_KEY, producerParams.toByteArray());

        PulsarGrpc.PulsarStub asyncStub = MetadataUtils.attachHeaders(PulsarGrpc.newStub(channel), headers);

        AtomicReference<StreamObserver<CommandSend>> requestRef = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        StreamObserver<SendResult> responseObserver = new StreamObserver<SendResult>() {
            @Override
            public void onNext(SendResult sendResult) {
                switch(sendResult.getSendResultOneofCase()) {
                    case PRODUCER_SUCCESS:
                        CommandProducerSuccess producerSuccess = sendResult.getProducerSuccess();
                        requestRef.get().onNext(getSendCommand(
                            producerSuccess.getProducerName(),
                            producerSuccess.getLastSequenceId() +1,
                            "test message " + (producerSuccess.getLastSequenceId() + 1)
                        ));
                        break;
                    case SEND_RECEIPT:
                        CommandSendReceipt sendReceipt = sendResult.getSendReceipt();
                        System.out.println(String.format("LedgerId: %s, EntryId: %s",
                            sendReceipt.getMessageId().getLedgerId(),
                            sendReceipt.getMessageId().getEntryId()
                        ));
                        requestRef.get().onCompleted();
                        break;
                }

            }

            @Override
            public void onError(Throwable t) {
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                latch.countDown();
            }
        };
        StreamObserver<CommandSend> request = asyncStub.produce(responseObserver);
        requestRef.set(request);

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static CommandSend getSendCommand(String producerName, long sequenceId, String message) {
        MessageMetadata metadata = MessageMetadata.newBuilder()
            .setProducerName(producerName)
            .setSequenceId(sequenceId)
            .setPublishTime(Instant.now().toEpochMilli())
            .build();

        byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        ByteBuf payload = Unpooled.wrappedBuffer(bytes);
        return Commands.newSend(sequenceId, 1, ChecksumType.Crc32c, metadata, payload);
    }

}
