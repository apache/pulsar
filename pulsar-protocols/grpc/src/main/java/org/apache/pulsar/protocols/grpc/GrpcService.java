/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
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

import static org.apache.pulsar.protocols.grpc.Constants.*;

public class GrpcService implements ProtocolHandler {

    private static final Logger log = LoggerFactory.getLogger(GrpcService.class);
    private static final String NAME = "grpc";

    private ServiceConfiguration configuration;
    private String advertisedAddress = null;
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
        List<String> args = new ArrayList<>();
        if (advertisedAddress != null) {
            StringBuilder sb = new StringBuilder(GRPC_SERVICE_HOST_PROPERTY_NAME);
            args.add(sb.append("=").append(advertisedAddress).toString());
        }
        if (server != null) {
            StringBuilder sb = new StringBuilder(GRPC_SERVICE_PORT_PROPERTY_NAME);
            args.add(sb.append("=").append(server.getPort()).toString());
        }
        if (tlsServer != null) {
            StringBuilder sb = new StringBuilder(GRPC_SERVICE_PORT_TLS_PROPERTY_NAME);
            args.add(sb.append("=").append(tlsServer.getPort()).toString());
        }
        return String.join(";", args);
    }

    @Override
    public void start(BrokerService service) {
        try {
            advertisedAddress = service.pulsar().getAdvertisedAddress();
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
