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

import com.google.common.annotations.Beta;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherMultipleConsumers;

/**
 * Factory of InMemoryDelayedDeliveryTracker objects. This is the entry point for implementations.
 *
 * Note: this interface is still being refined and some breaking changes might be introduced.
 */
@Beta
public interface DelayedDeliveryTrackerFactory extends AutoCloseable {
    /**
     * Initialize the factory implementation from the broker service configuration.
     *
     * @param pulsarService the broker service
     */
    void initialize(PulsarService pulsarService) throws Exception;

    /**
     * Create a new tracker instance.
     *
     * @param dispatcher
     *            a multi-consumer dispatcher instance
     */
    DelayedDeliveryTracker newTracker(PersistentDispatcherMultipleConsumers dispatcher);

    /**
     * Close the factory and release all the resources.
     */
    void close() throws Exception;
}
