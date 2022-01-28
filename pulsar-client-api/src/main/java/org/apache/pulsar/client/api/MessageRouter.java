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
package org.apache.pulsar.client.api;

import java.io.Serializable;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * Interface for custom message router that can be passed
 * to a producer to select the partition that a particular
 * messsage should be published on.
 *
 * @see ProducerBuilder#messageRouter(MessageRouter)
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface MessageRouter extends Serializable {

    /**
     *
     * @param msg
     *            Message object
     * @return The index of the partition to use for the message
     * @deprecated since 1.22.0. Please use {@link #choosePartition(Message, TopicMetadata)} instead.
     */
    @Deprecated
    default int choosePartition(Message<?> msg) {
        throw new UnsupportedOperationException("Use #choosePartition(Message, TopicMetadata) instead");
    }

    /**
     * Choose a partition based on msg and the topic metadata.
     *
     * @param msg message to route
     * @param metadata topic metadata
     * @return the partition to route the message.
     * @since 1.22.0
     */
    default int choosePartition(Message<?> msg, TopicMetadata metadata) {
        return choosePartition(msg);
    }

}
