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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.loadbalance.extensions.models.Split;
import org.apache.pulsar.broker.loadbalance.extensions.models.Unload;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.extended.SessionEvent;

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
     * Closes the ServiceUnitStateChannel.
     * @throws PulsarServerException if it fails to close the channel.
     */
    @Override
    void close() throws PulsarServerException;

    /**
     * Asynchronously gets the current owner broker of the system topic in this channel.
     * @return the service url without the protocol prefix, 'http://'. e.g. broker-xyz:abcd
     *
     * ServiceUnitStateChannel elects the separate leader as the owner broker of the system topic in this channel.
     */
    CompletableFuture<Optional<String>> getChannelOwnerAsync();

    /**
     * Asynchronously checks if the current broker is the owner broker of the system topic in this channel.
     * @return True if the current broker is the owner. Otherwise, false.
     */
    CompletableFuture<Boolean> isChannelOwnerAsync();

    /**
     * Handles the metadata session events to track
     * if the connection between the broker and metadata store is stable or not.
     * This will be registered as a metadata SessionEvent listener.
     *
     * The stability of the metadata connection is important
     * to determine how to handle the broker deletion(unavailable) event notified from the metadata store.
     *
     * Please refer to handleBrokerRegistrationEvent(String broker, NotificationType type) for more details.
     *
     * @param event metadata session events
     */
    void handleMetadataSessionEvent(SessionEvent event);

    /**
     * Handles the broker registration event from the broker registry.
     * This will be registered as a broker registry listener.
     *
     * Case 1: If NotificationType is Deleted,
     *         it will schedule a clean-up operation to release the ownerships of the deleted broker.
     *
     *      Sub-case1: If the metadata connection has been stable for long time,
     *                 it will immediately execute the cleanup operation to guarantee high-availability.
     *
     *      Sub-case2: If the metadata connection has been stable only for short time,
     *                 it will defer the clean-up operation for some time and execute it.
     *                 This is to gracefully handle the case when metadata connection is flaky --
     *                 If the deleted broker comes back very soon,
     *                 we better cancel the clean-up operation for high-availability.
     *
     *      Sub-case3: If the metadata connection is unstable,
     *                 it will not schedule the clean-up operation, as the broker-metadata connection is lost.
     *                 The brokers will continue to serve existing topics connections,
     *                 and we better not to interrupt the existing topic connections for high-availability.
     *
     *
     * Case 2: If NotificationType is Created,
     *         it will cancel any scheduled clean-up operation if still not executed.
     *
     * @param broker notified broker
     * @param type notification type
     */
    void handleBrokerRegistrationEvent(String broker, NotificationType type);

    /**
     * Asynchronously gets the current owner broker of the service unit.
     *
     *
     * @param serviceUnit (e.g. bundle)
     * @return the future object of the owner broker
     *
     * Case 1: If the service unit is owned, it returns the completed future object with the current owner.
     * Case 2: If the service unit's assignment is ongoing, it returns the non-completed future object.
     *      Sub-case1: If the assigned broker is available and finally takes the ownership,
     *                 the future object will complete and return the owner broker.
     *      Sub-case2: If the assigned broker does not take the ownership in time,
     *                 the future object will time out.
     * Case 3: If none of them, it returns Optional.empty().
     */
    CompletableFuture<Optional<String>> getOwnerAsync(String serviceUnit);

    /**
     * Asynchronously publishes the service unit assignment event to the system topic in this channel.
     * It de-duplicates assignment events if there is any ongoing assignment event for the same service unit.
     * @param serviceUnit (e.g bundle)
     * @param broker the assigned broker
     * @return the completable future object with the owner broker
     * case 1: If the assigned broker is available and takes the ownership,
     *         the future object will complete and return the owner broker.
     *         The returned owner broker could be different from the input broker (due to assignment race-condition).
     * case 2: If the assigned broker does not take the ownership in time,
     *         the future object will time out.
     */
    CompletableFuture<String> publishAssignEventAsync(String serviceUnit, String broker);

    /**
     * Asynchronously publishes the service unit unload event to the system topic in this channel.
     * @param unload (unload specification object)
     * @return the completable future object staged from the event message sendAsync.
     */
    CompletableFuture<Void> publishUnloadEventAsync(Unload unload);

    /**
     * Asynchronously publishes the bundle split event to the system topic in this channel.
     * @param split (split specification object)
     * @return the completable future object staged from the event message sendAsync.
     */
    CompletableFuture<Void> publishSplitEventAsync(Split split);

    /**
     * Generates the metrics to monitor.
     * @return a list of the metrics
     */
    List<Metrics> getMetrics();

}
