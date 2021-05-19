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
 * A listener that will be called in order for every message received.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface MessageListener<T> extends Serializable {
    /**
     * This method is called whenever a new message is received.
     *
     * <p>Messages are guaranteed to be delivered in order and from the same thread for a single consumer
     *
     * <p>This method will only be called once for each message, unless either application or broker crashes.
     *
     * <p>Application is responsible for acking message by calling any of consumer acknowledgement methods.
     *
     * <p>Application is responsible of handling any exception that could be thrown while processing the message.
     *
     * @param consumer
     *            the consumer that received the message
     * @param msg
     *            the message object
     */
    void received(Consumer<T> consumer, Message<T> msg);

    /**
     * Get the notification when a topic is terminated.
     *
     * @param consumer
     *            the Consumer object associated with the terminated topic
     */
    default void reachedEndOfTopic(Consumer<T> consumer) {
        // By default ignore the notification
    }
}
