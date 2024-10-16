/*
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
package org.apache.pulsar.broker.service;

import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.util.concurrent.Promise;
import java.net.SocketAddress;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLookupData;

public interface TransportCnx {

    String getClientVersion();
    String getProxyVersion();

    SocketAddress clientAddress();

    String clientSourceAddressAndPort();

    BrokerService getBrokerService();

    PulsarCommandSender getCommandSender();

    boolean isBatchMessageCompatibleVersion();

    /**
     * The security role for this connection.
     *
     * @return the role
     */
    String getAuthRole();

    AuthenticationDataSource getAuthenticationData();

    boolean isActive();

    boolean isWritable();

    void completedSendOperation(boolean isNonPersistentTopic, int msgSize);

    void removedProducer(Producer producer);

    void closeProducer(Producer producer);
    void closeProducer(Producer producer, Optional<BrokerLookupData> assignedBrokerLookupData);

    void execute(Runnable runnable);

    void removedConsumer(Consumer consumer);

    void closeConsumer(Consumer consumer, Optional<BrokerLookupData> assignedBrokerLookupData);

    boolean isPreciseDispatcherFlowControl();

    Promise<Void> newPromise();

    boolean hasHAProxyMessage();

    HAProxyMessage getHAProxyMessage();

    String clientSourceAddress();

    /***
     * Check if the connection is still alive
     * by actively sending a Ping message to the client.
     *
     * @return a completable future where the result is true if the connection is alive, false otherwise. The result
     * is empty if the connection liveness check is disabled.
     */
    CompletableFuture<Optional<Boolean>> checkConnectionLiveness();

    /**
     * Increments the counter that controls the throttling of the connection by pausing reads.
     * The connection will be throttled while the counter is greater than 0.
     * <p>
     * The caller is responsible for decrementing the counter by calling {@link #decrementThrottleCount()}  when the
     * connection should no longer be throttled.
     */
    void incrementThrottleCount();

    /**
     * Decrements the counter that controls the throttling of the connection by pausing reads.
     * The connection will be throttled while the counter is greater than 0.
     * <p>
     * This method should be called when the connection should no longer be throttled. However, the caller should have
     * previously called {@link #incrementThrottleCount()}.
     */
    void decrementThrottleCount();
}
