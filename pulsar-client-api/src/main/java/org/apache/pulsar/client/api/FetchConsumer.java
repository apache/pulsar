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
package org.apache.pulsar.client.api;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

@InterfaceAudience.Public
@InterfaceStability.Unstable
public interface FetchConsumer<T> extends Closeable, MessageAcknowledger {

    /**
     * Synchronously fetch a batch of messages.
     *
     * @param maxMessages the maximum number of messages to fetch
     * @param maxBytes the maximum size of messages in bytes to fetch
     * @param messageId the starting point message ID to fetch from
     * @param timeout the maximum time to wait for fetching messages
     * @param unit the time unit for the timeout
     * @return a batch of messages
     * @throws PulsarClientException if fetching messages fails
     */
    Messages<T> fetchMessages(int maxMessages, int maxBytes, MessageId messageId, int timeout, TimeUnit unit)
            throws PulsarClientException;

    /**
     * Asynchronously fetch a batch of messages.
     *
     * @param maxMessages the maximum number of messages to fetch
     * @param maxBytes the maximum size of messages in bytes to fetch
     * @param offset the starting point message ID to fetch from (same as messageId in the synchronous method)
     * @param timeout the maximum time to wait for fetching messages
     * @param unit the time unit for the timeout
     * @return a CompletableFuture that will contain the batch of messages when the fetch is complete
     */
    CompletableFuture<Messages<T>> fetchMessagesAsync(int maxMessages, int maxBytes, MessageId offset, int timeout,
                                                      TimeUnit unit);

    /**
     * Close the consumer and stop the broker from pushing more messages.
     *
     * @throws PulsarClientException if there is a failure in closing the consumer
     */
    @Override
    void close() throws PulsarClientException;

    /**
     * Asynchronously close the consumer and stop the broker from pushing more messages.
     *
     * @return a CompletableFuture that can be used to track the completion of the operation
     */
    CompletableFuture<Void> closeAsync();
}
