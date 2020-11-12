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

import org.apache.bookkeeper.mledger.Entry;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.common.api.proto.PulsarApi.ServerError;

import java.net.SocketAddress;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface TransportCnx {

    default String getClientVersion() {
        return null;
    }

    SocketAddress clientAddress();

    BrokerService getBrokerService();

    PulsarCommandSender getCommandSender();

    default boolean isBatchMessageCompatibleVersion() {
        return true;
    }

    /**
     * The security role for this connection
     * @return the role
     */
    default String getAuthRole() {
        return null;
    }

    default AuthenticationDataSource getAuthenticationData() {
        return null;
    }

    default boolean isActive() {
        return true;
    }

    default boolean isWritable() {
        return true;
    }

    default void completedSendOperation(boolean isNonPersistentTopic, int msgSize) {
        // No-op
    }

    default void removedProducer(Producer producer) {
        // No-op
    }

    default void closeProducer(Producer producer) {
        // No-op
    }

    default long getMessagePublishBufferSize() {
        return Long.MAX_VALUE;
    }

    default void cancelPublishRateLimiting() {
        // No-op
    }

    default void cancelPublishBufferLimiting() {
        // No-op
    }

    default void disableCnxAutoRead() {
        // No-op
    }

    default void enableCnxAutoRead() {
        // No-op
    }

    default void execute(Runnable runnable) {
        CompletableFuture.runAsync(runnable);
    }

    default void removedConsumer(Consumer consumer) {
        // No-op
    }

    default void closeConsumer(Consumer consumer) {
        // No-op
    }

    default boolean isPreciseDispatcherFlowControl() {
        return false;
    }

}
