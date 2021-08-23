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
package org.apache.pulsar.functions.api;

import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * BaseContext provides base contextual information to the executing function/source/sink.
 * It allows to propagate information, such as pulsar environment, logs, metrics, states etc.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface BaseContext {
    /**
     * The tenant this component belongs to.
     *
     * @return the tenant this component belongs to
     */
    String getTenant();

    /**
     * The namespace this component belongs to.
     *
     * @return the namespace this component belongs to
     */
    String getNamespace();

    /**
     * The id of the instance that invokes this component.
     *
     * @return the instance id
     */
    int getInstanceId();

    /**
     * Get the number of instances that invoke this component.
     *
     * @return the number of instances that invoke this component.
     */
    int getNumInstances();

    /**
     * The logger object that can be used to log in a component.
     *
     * @return the logger object
     */
    Logger getLogger();

    /**
     * Get the secret associated with this key.
     *
     * @param secretName The name of the secret
     * @return The secret if anything was found or null
     */
    String getSecret(String secretName);

    /**
     * Get the state store with the provided store name in the tenant & namespace.
     *
     * @param name the state store name
     * @param <S> the type of interface of the store to return
     * @return the state store instance.
     *
     * @throws ClassCastException if the return type isn't a type
     * or interface of the actual returned store.
     */
    default <S extends StateStore> S getStateStore(String name) {
        throw new UnsupportedOperationException("Component cannot get state store");
    }

    /**
     * Get the state store with the provided store name.
     *
     * @param tenant the state tenant name
     * @param ns the state namespace name
     * @param name the state store name
     * @param <S> the type of interface of the store to return
     * @return the state store instance.
     *
     * @throws ClassCastException if the return type isn't a type
     * or interface of the actual returned store.
     */
    default <S extends StateStore> S getStateStore(String tenant, String ns, String name) {
        throw new UnsupportedOperationException("Component cannot get state store");
    }

    /**
     * Update the state value for the key.
     *
     * @param key   name of the key
     * @param value state value of the key
     */
    void putState(String key, ByteBuffer value);

    /**
     * Update the state value for the key, but don't wait for the operation to be completed
     *
     * @param key   name of the key
     * @param value state value of the key
     */
    CompletableFuture<Void> putStateAsync(String key, ByteBuffer value);

    /**
     * Retrieve the state value for the key.
     *
     * @param key name of the key
     * @return the state value for the key.
     */
    ByteBuffer getState(String key);

    /**
     * Retrieve the state value for the key, but don't wait for the operation to be completed
     *
     * @param key name of the key
     * @return the state value for the key.
     */
    CompletableFuture<ByteBuffer> getStateAsync(String key);

    /**
     * Delete the state value for the key.
     *
     * @param key   name of the key
     */
    void deleteState(String key);

    /**
     * Delete the state value for the key, but don't wait for the operation to be completed
     *
     * @param key   name of the key
     */
    CompletableFuture<Void> deleteStateAsync(String key);

    /**
     * Increment the builtin distributed counter referred by key.
     *
     * @param key    The name of the key
     * @param amount The amount to be incremented
     */
    void incrCounter(String key, long amount);

    /**
     * Increment the builtin distributed counter referred by key
     * but dont wait for the completion of the increment operation
     *
     * @param key    The name of the key
     * @param amount The amount to be incremented
     */
    CompletableFuture<Void> incrCounterAsync(String key, long amount);

    /**
     * Retrieve the counter value for the key.
     *
     * @param key name of the key
     * @return the amount of the counter value for this key
     */
    long getCounter(String key);

    /**
     * Retrieve the counter value for the key, but don't wait
     * for the operation to be completed
     *
     * @param key name of the key
     * @return the amount of the counter value for this key
     */
    CompletableFuture<Long> getCounterAsync(String key);

    /**
     * Record a user defined metric
     * @param metricName The name of the metric
     * @param value The value of the metric
     */
    void recordMetric(String metricName, double value);

    /**
     * Get the pre-configured pulsar client.
     *
     * You can use this client to access Pulsar cluster.
     * The Function will be responsible for disposing this client.
     *
     * @return the instance of pulsar client
     */
    default PulsarClient getPulsarClient() {
        throw new UnsupportedOperationException("not implemented");
    }

    /**
     * Get the pre-configured pulsar client builder.
     *
     * You can use this Builder to setup client to connect to the Pulsar cluster.
     * But you need to close client properly after using it.
     *
     * @return the instance of pulsar client builder.
     */
    default ClientBuilder getPulsarClientBuilder() {
        throw new UnsupportedOperationException("not implemented");
    }

}
