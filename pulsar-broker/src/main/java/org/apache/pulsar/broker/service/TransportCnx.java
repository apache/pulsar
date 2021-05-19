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
package org.apache.pulsar.broker.service;

import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.util.concurrent.Promise;
import java.net.SocketAddress;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;

public interface TransportCnx {

    String getClientVersion();

    SocketAddress clientAddress();

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

    void cancelPublishRateLimiting();

    void cancelPublishBufferLimiting();

    void disableCnxAutoRead();

    void enableCnxAutoRead();

    void execute(Runnable runnable);

    void removedConsumer(Consumer consumer);

    void closeConsumer(Consumer consumer);

    boolean isPreciseDispatcherFlowControl();

    Promise<Void> newPromise();

    boolean hasHAProxyMessage();

    HAProxyMessage getHAProxyMessage();

    String clientSourceAddress();

}
