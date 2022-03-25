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

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * A Reader can be used to scan through all the messages currently available in a topic.
 *
 */
public interface Reader extends Closeable {

    /**
     * @return the topic from which this reader is reading from
     */
    String getTopic();

    /**
     * Read the next message in the topic.
     *
     * @return the next messasge
     * @throws PulsarClientException
     */
    Message<byte[]> readNext() throws PulsarClientException;

    /**
     * Read the next message in the topic waiting for a maximum of timeout
     * time units. Returns null if no message is recieved in that time.
     *
     * @return the next message(Could be null if none received in time)
     * @throws PulsarClientException
     */
    Message<byte[]> readNext(int timeout, TimeUnit unit) throws PulsarClientException;

    CompletableFuture<Message<byte[]>> readNextAsync();

    /**
     * Asynchronously close the reader and stop the broker to push more messages.
     *
     * @return a future that can be used to track the completion of the operation
     */
    CompletableFuture<Void> closeAsync();

    /**
     * Return true if the topic was terminated and this reader has reached the end of the topic.
     */
    boolean hasReachedEndOfTopic();

    /**
     * Check if there is any message available to read from the current position.
     */
    boolean hasMessageAvailable() throws PulsarClientException;

    /**
     * Asynchronously Check if there is message that has been published successfully to the broker in the topic.
     */
    CompletableFuture<Boolean> hasMessageAvailableAsync();

    /**
     * @return Whether the reader is connected to the broker
     */
    boolean isConnected();
}
