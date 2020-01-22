package org.apache.pulsar.protocols.grpc;

import io.grpc.stub.StreamObserver;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.ServerCnx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;

public class GrpcCnx implements ServerCnx {

    private static final Logger log = LoggerFactory.getLogger(GrpcCnx.class);

    private final BrokerService service;
    private final SocketAddress remoteAddress;

    private final StreamObserver<PulsarApi.BaseCommand> responseObserver;

    public GrpcCnx(BrokerService service, SocketAddress remoteAddress, StreamObserver<PulsarApi.BaseCommand> responseObserver) {
        this.service = service;
        this.remoteAddress = remoteAddress;
        this.responseObserver = responseObserver;
    }

    @Override
    public String getClientVersion() {
        return null;
    }

    @Override
    public SocketAddress clientAddress() {
        return remoteAddress;
    }

    @Override
    public BrokerService getBrokerService() {
        return service;
    }

    @Override
    public boolean isBatchMessageCompatibleVersion() {
        return false;
    }

    @Override
    public String getRole() {
        return null;
    }

    @Override
    public boolean isActive() {
        return false;
    }

    @Override
    public SocketAddress getRemoteAddress() {
        return null;
    }

    @Override
    public void completedSendOperation(boolean isNonPersistentTopic, int msgSize) {
    }

    @Override
    public AuthenticationDataSource getAuthenticationData() {
        return null;
    }

    @Override
    public void removedProducer(Producer producer) {
        log.info("########### remove producer");
    }

    @Override
    public void closeProducer(Producer producer) {
    }

    @Override
    public void enableCnxAutoRead() {
    }

    @Override
    public long getMessagePublishBufferSize() {
        return Long.MAX_VALUE;
    }

    @Override
    public void cancelPublishRateLimiting() {
    }

    @Override
    public void cancelPublishBufferLimiting() {
    }

    @Override
    public void disableCnxAutoRead() {
    }

    public StreamObserver<PulsarApi.BaseCommand> getResponseObserver() {
        return responseObserver;
    }

}
