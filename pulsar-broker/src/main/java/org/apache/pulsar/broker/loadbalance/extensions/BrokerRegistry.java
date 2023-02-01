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
package org.apache.pulsar.broker.loadbalance.extensions;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLookupData;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.NotificationType;

/**
 * Responsible for registering the current Broker lookup info to
 * the distributed store (e.g. Zookeeper) for broker discovery.
 */
public interface BrokerRegistry extends AutoCloseable {

    /**
     * Start broker registry.
     */
    void start() throws PulsarServerException;

    /**
     * Return the broker registry is started.
     */
    boolean isStarted();

    /**
     * Register local broker to metadata store.
     */
    void register() throws MetadataStoreException;

    /**
     * Unregister the broker.
     *
     * Same as {@link org.apache.pulsar.broker.loadbalance.ModularLoadManager#disableBroker()}
     */
    void unregister() throws MetadataStoreException;

    /**
     * Get the current broker ID.
     *
     * @return The service url without the protocol prefix, 'http://'. e.g. broker-xyz:port
     */
    String getBrokerId();

    /**
     * Async get available brokers.
     *
     * @return The brokers service url list.
     */
    CompletableFuture<List<String>> getAvailableBrokersAsync();

    /**
     * Get the broker lookup data.
     *
     * @param broker The service url without the protocol prefix, 'http://'. e.g. broker-xyz:port
     */
    CompletableFuture<Optional<BrokerLookupData>> lookupAsync(String broker);

    /**
     * Get the map of brokerId->brokerLookupData.
     *
     * @return Map of broker lookup data.
     */
    CompletableFuture<Map<String, BrokerLookupData>> getAvailableBrokerLookupDataAsync();

    /**
     * Add listener to listen the broker register change.
     *
     * @param listener Key is broker Id{@link BrokerRegistry#getBrokerId()}
     *                 Value is notification type.
     */
    void addListener(BiConsumer<String, NotificationType> listener);

    /**
     * Close the broker registry.
     *
     * @throws PulsarServerException if it fails to close the broker registry.
     */
    @Override
    void close() throws PulsarServerException;
}
