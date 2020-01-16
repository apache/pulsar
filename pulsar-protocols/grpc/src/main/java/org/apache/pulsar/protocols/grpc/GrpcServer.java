package org.apache.pulsar.protocols.grpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.service.BrokerService;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

@Slf4j
public class GrpcServer {

    private Server server;

    public void start(BrokerService service) throws IOException {
        /* The port on which the server should run */
        int port = 50444;
        server = ServerBuilder.forPort(port)
                .addService(new PulsarGrpcService(service))
                .addService(ProtoReflectionService.newInstance())
                .build()
                .start();
        log.info("############# Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                log.error("############### shutting down gRPC server since JVM is shutting down");
                try {
                    GrpcServer.this.stop();
                } catch (InterruptedException e) {
                    e.printStackTrace(System.err);
                }
                log.error("############### gRPC server shut down");
            }
        });
    }

    public void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    // TO BE REMOVED : For test
    public static void main(String[] args) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 50444).usePlaintext().build();
        PulsarGrpcServiceGrpc.PulsarGrpcServiceBlockingStub stub = PulsarGrpcServiceGrpc.newBlockingStub(channel);
        String test = stub.hello(SimpleValue.newBuilder().setName("persistent://public/default/my-topic").build()).getName();
        System.out.println(test);
    }
}
