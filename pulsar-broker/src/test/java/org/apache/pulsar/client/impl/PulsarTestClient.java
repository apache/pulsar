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
package org.apache.pulsar.client.impl;

import static org.testng.Assert.assertEquals;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.awaitility.Awaitility;

/**
 * A Pulsar Client that is used for testing scenarios where the different
 * asynchronous operations of the client-broker interaction must be orchestrated by the test
 * so that race conditions caused by the test code can be eliminated.
 *
 * features:
 * - can override remote endpoint protocol version in a thread safe manner
 * - can reject new connections from the client to the broker
 * - can drop all OpSend messages after they have been added to pendingMessages and processed
 *   by the client. This simulates a situation where sending messages go to a "black hole".
 * - can synchronize operations with the help of the pending message callback which gets
 *   called after the message to send out has been added to the pending messages in the client.
 *
 */
public class PulsarTestClient extends PulsarClientImpl {
    private volatile int overrideRemoteEndpointProtocolVersion;
    private volatile boolean rejectNewConnections;
    private volatile boolean dropOpSendMessages;
    private volatile Consumer<ProducerImpl.OpSendMsg> pendingMessageCallback;

    /**
     * Create a new PulsarTestClient instance.
     *
     * @param clientBuilder ClientBuilder instance containing the configuration of the client
     * @return a new
     * @throws PulsarClientException
     */
    public static PulsarTestClient create(ClientBuilder clientBuilder) throws PulsarClientException {
        ClientConfigurationData clientConfigurationData =
                ((ClientBuilderImpl) clientBuilder).getClientConfigurationData();

        // the reason to do all the following is to be able to pass the supplier for creating new ClientCnx
        // instances after the constructor of PulsarClientImpl has been called.
        // An anonymous subclass of ClientCnx class is used to override the getRemoteEndpointProtocolVersion()
        // method.
        EventLoopGroup eventLoopGroup = EventLoopUtil.newEventLoopGroup(clientConfigurationData.getNumIoThreads(),
                false,
                new DefaultThreadFactory("pulsar-client-io", Thread.currentThread().isDaemon()));

        AtomicReference<Supplier<ClientCnx>> clientCnxSupplierReference = new AtomicReference<>();
        ConnectionPool connectionPool = new ConnectionPool(clientConfigurationData, eventLoopGroup,
                () -> clientCnxSupplierReference.get().get());

        return new PulsarTestClient(clientConfigurationData, eventLoopGroup, connectionPool,
                clientCnxSupplierReference);
    }

    private PulsarTestClient(ClientConfigurationData conf, EventLoopGroup eventLoopGroup, ConnectionPool cnxPool,
                             AtomicReference<Supplier<ClientCnx>> clientCnxSupplierReference)
            throws PulsarClientException {
        super(conf, eventLoopGroup, cnxPool);
        // workaround initialization order issue so that ClientCnx can be created in this class
        clientCnxSupplierReference.set(this::createClientCnx);
    }

    /**
     * Overrides the default ClientCnx implementation with an implementation that overrides the
     * getRemoteEndpointProtocolVersion() method. This is used to test client behaviour in certain cases.
     *
     * @return new ClientCnx instance
     */
    protected ClientCnx createClientCnx() {
        return new ClientCnx(conf, eventLoopGroup) {
            @Override
            public int getRemoteEndpointProtocolVersion() {
                return overrideRemoteEndpointProtocolVersion != 0
                        ? overrideRemoteEndpointProtocolVersion
                        : super.getRemoteEndpointProtocolVersion();
            }
        };
    }

    /**
     * Overrides the getConnection method to reject new connections from being established between
     * the client and brokers.
     *
     * @param topic the topic for the connection
     * @return the ClientCnx to use, passed a future. Will complete with an exception when connections are rejected.
     */
    @Override
    public CompletableFuture<ClientCnx> getConnection(String topic) {
        if (rejectNewConnections) {
            CompletableFuture<ClientCnx> result = new CompletableFuture<>();
            result.completeExceptionally(new IOException("New connections are rejected."));
            return result;
        } else {
            return super.getConnection(topic);
        }
    }

    /**
     * Overrides the producer instance with an anonynomous subclass that adds hooks for observing new
     * OpSendMsg instances being added to pending messages in the client.
     * It also configures the hook to drop OpSend messages when dropping is enabled.
     */
    @Override
    protected <T> ProducerImpl<T> newProducerImpl(String topic, int partitionIndex, ProducerConfigurationData conf,
                                                  Schema<T> schema, ProducerInterceptors interceptors,
                                                  CompletableFuture<Producer<T>> producerCreatedFuture) {
        return new ProducerImpl<T>(this, topic, conf, producerCreatedFuture, partitionIndex, schema,
                interceptors) {
            @Override
            protected OpSendMsgQueue createPendingMessagesQueue() {
                return new OpSendMsgQueue() {
                    @Override
                    public boolean add(OpSendMsg opSendMsg) {
                        boolean added = super.add(opSendMsg);
                        if (pendingMessageCallback != null) {
                            pendingMessageCallback.accept(opSendMsg);
                        }
                        return added;
                    }
                };
            }

            @Override
            protected ClientCnx getCnxIfReady() {
                if (dropOpSendMessages) {
                    return null;
                } else {
                    return super.getCnxIfReady();
                }
            }
        };
    }

    public void setOverrideRemoteEndpointProtocolVersion(int overrideRemoteEndpointProtocolVersion) {
        this.overrideRemoteEndpointProtocolVersion = overrideRemoteEndpointProtocolVersion;
    }

    public void setRejectNewConnections(boolean rejectNewConnections) {
        this.rejectNewConnections = rejectNewConnections;
    }

    /**
     * Simulates the producer connection getting dropped. Will also reject reconnections to simulate an
     * outage. This reduces race conditions since the reconnection has to be explicitly enabled by calling
     * allowReconnecting() method.
     */
    public void disconnectProducerAndRejectReconnecting(ProducerImpl<?> producer) throws IOException {
        // wait until all possible in-flight messages have been delivered
        Awaitility.await().untilAsserted(() -> {
            if (!dropOpSendMessages && producer.isConnected()) {
                assertEquals(producer.getPendingQueueSize(), 0);
            }
        });

        // reject new connection attempts
        setRejectNewConnections(true);

        // make the existing connection between the producer and broker to break by explicitly closing it
        ClientCnx cnx = producer.cnx();
        producer.connectionClosed(cnx);
        cnx.close();
    }

    /**
     * Resets possible dropping of OpSend messages and allows the client to reconnect to the broker.
     */
    public void allowReconnecting() {
        dropOpSendMessages = false;
        setRejectNewConnections(false);
    }

    /**
     * Assigns the callback to use for handling OpSend messages once a message had been added to pending messages.
     * @param pendingMessageCallback
     */
    public void setPendingMessageCallback(
            Consumer<ProducerImpl.OpSendMsg> pendingMessageCallback) {
        this.pendingMessageCallback = pendingMessageCallback;
    }

    /**
     * Enable dropping of OpSend messages after they have been added to pendingMessages and processed
     * by the client. The OpSend messages won't be delivered until the allowReconnecting method has been called.
     */
    public void dropOpSendMessages() {
        this.dropOpSendMessages = true;
    }
}
