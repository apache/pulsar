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
package org.apache.pulsar.io.core;

import java.util.Collection;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;
import org.apache.pulsar.functions.api.BaseContext;

/**
 * Interface for a sink connector providing information about environment where it is running.
 * It also allows to propagate information, such as logs, metrics, states, back to the Pulsar environment.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface SinkContext extends BaseContext {
    /**
     * The name of the sink that we are executing
     * @return The Sink name
     */
    String getSinkName();

    /**
     * Get a list of all input topics.
     *
     * @return a list of all input topics
     */
    Collection<String> getInputTopics();

    /**
     * Get subscription type used by the source providing data for the sink
     *
     * @return subscription type
     */
    default SubscriptionType getSubscriptionType() {
        throw new UnsupportedOperationException("Context does not provide SubscriptionType");
    }

    /**
     * Reset the subscription associated with this topic and partition to a specific message id.
     *
     * @param topic - topic name
     * @param partition - partition id (0 for non-partitioned topics)
     * @param messageId to reset to
     * @throws PulsarClientException
     */
    default void seek(String topic, int partition, MessageId messageId) throws PulsarClientException {
        throw new UnsupportedOperationException("not implemented");
    }

    /**
     * Stop requesting new messages for given topic and partition until {@link #resume(String topic, int partition)}
     * is called.
     *
     * @param topic - topic name
     * @param partition - partition id (0 for non-partitioned topics)
     */
    default void pause(String topic, int partition) throws PulsarClientException {
        throw new UnsupportedOperationException("not implemented");
    }

    /**
     * Resume requesting messages.
     * @param topic - topic name
     * @param partition - partition id (0 for non-partitioned topics)
     */
    default void resume(String topic, int partition) throws PulsarClientException {
        throw new UnsupportedOperationException("not implemented");
    }
}
