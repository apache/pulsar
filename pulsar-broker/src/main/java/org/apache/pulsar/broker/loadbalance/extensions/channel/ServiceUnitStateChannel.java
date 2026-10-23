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
package org.apache.pulsar.broker.loadbalance.extensions.channel;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.loadbalance.extensions.manager.StateChangeListener;
import org.apache.pulsar.broker.loadbalance.extensions.models.Split;
import org.apache.pulsar.broker.loadbalance.extensions.models.Unload;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.stats.Metrics;

/**
 * Defines the ServiceUnitStateChannel interface.
 */
public interface ServiceUnitStateChannel extends Closeable {

    /**
     * Starts the ServiceUnitStateChannel.
     * @throws PulsarServerException if it fails to start the channel.
     */
    void start() throws PulsarServerException;

    /**
     * Whether the channel started.
     */
    boolean started();

    /**
     * Closes the ServiceUnitStateChannel.
     * @throws PulsarServerException if it fails to close the channel.
     */
    @Override
    void close() throws PulsarServerException;

    /**
     * Asynchronously gets the current owner broker of this channel.
     * @return a future of owner brokerId to track the completion of the operation
     */
    CompletableFuture<Optional<String>> getChannelOwnerAsync();

    /**
     * Asynchronously checks if the current broker is the owner broker of this channel.
     * @return a future of check result to track the completion of the operation
     */
    CompletableFuture<Boolean> isChannelOwnerAsync();

    /**
     * Checks if the current broker is the owner broker of this channel.
     * @return True if the current broker is the owner. Otherwise, false.
     * @throws ExecutionException
     * @throws InterruptedException
     * @throws TimeoutException
     */
    boolean isChannelOwner() throws ExecutionException, InterruptedException, TimeoutException;

    /**
     * Asynchronously gets the current owner broker of the service unit.
     *
     * @param serviceUnit (e.g. bundle)
     * @return a future of owner brokerId to track the completion of the operation
     */
    CompletableFuture<Optional<String>> getOwnerAsync(String serviceUnit);

    /**
     * Asynchronously gets the assigned broker of the service unit.
     *
     * @param serviceUnit (e.g. bundle))
     * @return assigned brokerId
     */
    Optional<String> getAssigned(String serviceUnit);


    /**
     * Checks if the target broker is the owner of the service unit.
     *
     * @param serviceUnit (e.g. bundle)
     * @param targetBrokerId
     * @return true if the target brokerId is the owner brokerId. false if unknown.
     */
    boolean isOwner(String serviceUnit, String targetBrokerId);

    /**
     * Checks if the current broker is the owner of the service unit.
     *
     * @param serviceUnit (e.g. bundle))
     * @return true if the current broker is the owner. false if unknown.
     */
    boolean isOwner(String serviceUnit);

    /**
     * Asynchronously publishes the service unit assignment event to this channel.
     * @param serviceUnit (e.g bundle)
     * @param brokerId the assigned brokerId
     * @return a future of owner brokerId to track the completion of the operation
     */
    CompletableFuture<String> publishAssignEventAsync(String serviceUnit, String brokerId);

    /**
     * Asynchronously publishes the service unit unload event to this channel.
     * @param unload (unload specification object)
     * @return a future to track the completion of the operation
     */
    CompletableFuture<Void> publishUnloadEventAsync(Unload unload);

    /**
     * Asynchronously publishes the bundle split event to this channel.
     * @param split (split specification object)
     * @return a future to track the completion of the operation
     */
    CompletableFuture<Void> publishSplitEventAsync(Split split);

    /**
     * Generates the metrics to monitor.
     * @return a list of the metrics
     */
    List<Metrics> getMetrics();

    /**
     * Adds a state change listener.
     *
     * @param listener State change listener.
     */
    void listen(StateChangeListener listener);

    /**
     * Asynchronously returns service unit ownership entry set.
     * @return a set of service unit ownership entries to track the completion of the operation
     */
    Set<Map.Entry<String, ServiceUnitStateData>> getOwnershipEntrySet();

    /**
     * Asynchronously returns service units owned by this broker.
     * @return a set of owned service units to track the completion of the operation
     */
    Set<NamespaceBundle> getOwnedServiceUnits();

    /**
     * Schedules ownership monitor to periodically check and correct invalid ownership states.
     */
    void scheduleOwnershipMonitor();

    /**
     * Cancels the ownership monitor.
     */
    void cancelOwnershipMonitor();

    /**
     * Cleans(gives up) any service unit ownerships from this broker.
     */
    void cleanOwnerships();
}
