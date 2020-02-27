package org.apache.pulsar.protocols.grpc;

import io.grpc.Server;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslContext;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.protocol.ProtocolHandler;
import org.apache.pulsar.broker.service.BrokerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class GrpcService implements ProtocolHandler {

    private static final Logger log = LoggerFactory.getLogger(GrpcService.class);
    private static final String NAME = "grpc";

    private ServiceConfiguration configuration;
    private Server server = null;
    private Server tlsServer = null;

    @Override
    public String protocolName() {
        return NAME;
    }

    @Override
    public boolean accept(String protocol) {
        return NAME.equals(protocol);
    }

    @Override
    public void initialize(ServiceConfiguration conf) {
        this.configuration = conf;
    }

    @Override
    public String getProtocolDataToAdvertise() {
        return "";
    }

    @Override
    public void start(BrokerService service) {
        try {
            PulsarGrpcService pulsarGrpcService = new PulsarGrpcService(service, configuration, new NioEventLoopGroup());
            List<ServerInterceptor> interceptors = new ArrayList<>();
            interceptors.add(new GrpcServerInterceptor());
            if (service.isAuthenticationEnabled()) {
                interceptors.add(new AuthenticationInterceptor(service));
            }

            Optional<Integer> grpcServicePort = Optional.ofNullable(configuration.getProperties().getProperty("grpcServicePort"))
                    .map(Integer::parseInt);

            if (grpcServicePort.isPresent()) {
                Integer port = grpcServicePort.get();
                server = NettyServerBuilder.forAddress(new InetSocketAddress(service.pulsar().getBindAddress(), port))
                        .addService(ServerInterceptors.intercept(pulsarGrpcService, interceptors))
                        .build()
                        .start();
                log.info("gRPC Service started, listening on " + server.getPort());
            }

            Optional<Integer> grpcServicePortTls = Optional.ofNullable(configuration.getProperties().getProperty("grpcServicePortTls"))
                    .map(Integer::parseInt);

            if (grpcServicePortTls.isPresent()) {
                Integer port = grpcServicePortTls.get();
                SslContext sslContext = SecurityUtility.createNettySslContextForServer(configuration.isTlsAllowInsecureConnection(),
                        configuration.getTlsTrustCertsFilePath(), configuration.getTlsCertificateFilePath(),
                        configuration.getTlsKeyFilePath(), configuration.getTlsCiphers(), configuration.getTlsProtocols(),
                        configuration.isTlsRequireTrustedClientCertOnConnect());

                tlsServer = NettyServerBuilder.forAddress(new InetSocketAddress(service.pulsar().getBindAddress(), port))
                        .addService(ServerInterceptors.intercept(pulsarGrpcService, interceptors))
                        .sslContext(sslContext)
                        .build()
                        .start();
                log.info("gRPC TLS Service started, listening on " + tlsServer.getPort());
            }

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("shutting down gRPC server since JVM is shutting down");
                try {
                    stop();
                } catch (InterruptedException e) {
                    e.printStackTrace(System.err);
                }
                System.err.println("gRPC server shut down");
            }));
        } catch (Exception e) {
            log.error("Couldn't start gRPC server", e);
        }
    }

    @Override
    public Map<InetSocketAddress, ChannelInitializer<SocketChannel>> newChannelInitializers() {
        // The gRPC server uses it's own Netty setup.
        return Collections.emptyMap();
    }

    @Override
    public void close() {
        try {
            stop();
        } catch (InterruptedException e) {
            log.error("Couldn't stop gRPC server", e);
        }
    }

    private void stop() throws InterruptedException {
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

    public Optional<Integer> getListenPort() {
        if (server != null) {
            return Optional.of(server.getPort());
        } else {
            return Optional.empty();
        }
    }

    public Optional<Integer> getListenPortTLS() {
        if (tlsServer != null) {
            return Optional.of(tlsServer.getPort());
        } else {
            return Optional.empty();
        }
    }
}
