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
package org.apache.pulsar.broker.delayed;

import org.apache.pulsar.broker.service.persistent.AbstractPersistentDispatcherMultipleConsumers;

/**
 * Manager interface for topic-level delayed delivery tracking.
 * This interface provides a unified abstraction for managing delayed delivery at the topic level,
 * allowing different implementations (in-memory, bucket-based) to share the same contract.
 * <p>
 * The manager maintains a single global delayed message index per topic that is shared by all
 * subscriptions, and provides per-subscription tracker objects that implement DelayedDeliveryTracker
 * interface for compatibility with existing dispatcher logic.
 */
public interface TopicDelayedDeliveryTrackerManager extends AutoCloseable {

    /**
     * Create or get a delayed delivery tracker for the specified subscription.
     *
     * @param dispatcher the dispatcher instance for the subscription
     * @return a DelayedDeliveryTracker bound to the subscription
     */
    DelayedDeliveryTracker createOrGetTracker(AbstractPersistentDispatcherMultipleConsumers dispatcher);

    /**
     * Unregister a subscription from the manager.
     *
     * @param dispatcher the dispatcher instance to unregister
     */
    void unregister(AbstractPersistentDispatcherMultipleConsumers dispatcher);

    /**
     * Update the tick time configuration for the topic.
     *
     * @param newTickTimeMillis the new tick time in milliseconds
     */
    void onTickTimeUpdated(long newTickTimeMillis);

    /**
     * Get the total memory usage of the topic-level delayed message index.
     *
     * @return memory usage in bytes
     */
    long topicBufferMemoryBytes();

    /**
     * Get the total number of delayed messages in the topic-level index.
     *
     * @return number of delayed messages
     */
    long topicDelayedMessages();

    /**
     * Close the manager and release all resources.
     */
    void close();
}
