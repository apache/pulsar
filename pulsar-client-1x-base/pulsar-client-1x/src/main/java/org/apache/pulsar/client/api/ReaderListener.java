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

/**
 * A listener that will be called in order for every message received.
 */
public interface ReaderListener<T> extends Serializable {
    /**
     * This method is called whenever a new message is received.
     *
     * Messages are guaranteed to be delivered in order and from the same thread for a single consumer
     *
     * This method will only be called once for each message, unless either application or broker crashes.
     *
     * Application is responsible of handling any exception that could be thrown while processing the message.
     *
     * @param reader
     *            the Reader object from where the message was received
     * @param msg
     *            the message object
     */
    void received(Reader reader, Message<T> msg);

    /**
     * Get the notification when a topic is terminated.
     *
     * @param reader
     *            the Reader object associated with the terminated topic
     */
    default void reachedEndOfTopic(Reader reader) {
        // By default ignore the notification
    }
}
