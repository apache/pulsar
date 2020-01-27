package org.apache.pulsar.broker.service;

import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import java.net.SocketAddress;

public interface ServerCnx {

    String getClientVersion();

    SocketAddress clientAddress();

    BrokerService getBrokerService();

    boolean isBatchMessageCompatibleVersion();

    String getRole();

    boolean isActive();

    void completedSendOperation(boolean isNonPersistentTopic, int msgSize);

    AuthenticationDataSource getAuthenticationData();

    void removedProducer(Producer producer);

    void closeProducer(Producer producer);

    void enableCnxAutoRead();

    long getMessagePublishBufferSize();

    void cancelPublishRateLimiting();

    void cancelPublishBufferLimiting();

    void disableCnxAutoRead();
}
