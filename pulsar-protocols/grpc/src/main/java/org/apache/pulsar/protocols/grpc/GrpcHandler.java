package org.apache.pulsar.protocols.grpc;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.protocol.ProtocolHandler;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Map;

public class GrpcHandler implements ProtocolHandler {

    private static final Logger log = LoggerFactory.getLogger(GrpcHandler.class);

    public static final String NAME = "grpc";
    private GrpcServer grpcServer;

    @Override
    public String protocolName() {
        return NAME;
    }

    @Override
    public boolean accept(String protocol) {
        return NAME.equals(protocol);
    }

    /**
     * Initialize the protocol handler when the protocol is constructed from reflection.
     *
     * <p>The initialize should initialize all the resources required for serving the protocol
     * handler but don't start those resources until {@link #start(BrokerService)} is called.
     *
     * @param conf broker service configuration
     */
    @Override
    public void initialize(ServiceConfiguration conf) {
        log.info("####################### initialize");
        grpcServer = new GrpcServer();
    }

    /**
     * Retrieve the protocol related data to advertise as part of
     * {@link LocalBrokerData}.
     *
     * <p>For example, when implementing a Kafka protocol handler, you need to advertise
     * corresponding Kafka listeners so that Pulsar brokers understand how to give back
     * the listener information when handling metadata requests.
     *
     * <p>NOTE: this method is called after {@link #initialize(ServiceConfiguration)}
     * and before {@link #start(BrokerService)}.
     *
     * @return the protocol related data to be advertised as part of LocalBrokerData.
     */
    @Override
    public String getProtocolDataToAdvertise() {
        log.info("####################### advertise");
        return "null";
    }

    /**
     * Start the protocol handler with the provided broker service.
     *
     * <p>The broker service provides the accesses to the Pulsar components such as load
     * manager, namespace service, managed ledger and etc.
     *
     * @param service the broker service to start with.
     */
    @Override
    public void start(BrokerService service) {
        log.info("####################### start");
        try {
            grpcServer.start(service);
        } catch (IOException e) {
            log.error("Couldn't start gRPC server", e);
        }
    }

    /**
     * Create the list of channel initializers for the ports that this protocol handler
     * will listen on.
     *
     * <p>NOTE: this method is called after {@link #start(BrokerService)}.
     *
     * @return the list of channel initializers for the ports that this protocol handler listens on.
     */
    @Override
    public Map<InetSocketAddress, ChannelInitializer<SocketChannel>> newChannelInitializers() {
        log.info("####################### newChannelInitializers");
        return Collections.emptyMap();
    }

    @Override
    public void close() {
        log.info("####################### stop");
        try {
            grpcServer.stop();
        } catch (InterruptedException e) {
            log.error("Couldn't stop gRPC server", e);
        }
    }
}
