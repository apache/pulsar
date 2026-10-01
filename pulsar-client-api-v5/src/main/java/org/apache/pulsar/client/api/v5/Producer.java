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
package org.apache.pulsar.client.api.v5;

import org.apache.pulsar.client.api.v5.async.AsyncProducer;

/**
 * A producer publishes messages to a Pulsar topic.
 *
 * <p>This interface provides synchronous (blocking) operations. For non-blocking
 * usage, obtain an {@link AsyncProducer} via {@link #async()}.
 *
 * @param <T> the type of message values this producer sends
 */
public interface Producer<T> extends AutoCloseable {

    /**
     * The topic this producer is attached to.
     *
     * @return the fully qualified topic name (e.g. {@code topic://tenant/namespace/my-topic})
     */
    String topic();

    /**
     * The name of this producer (system-assigned or user-specified via
     * {@link ProducerBuilder#producerName(String)}).
     *
     * @return the producer name, never {@code null}
     */
    String producerName();

    /**
     * Create a message builder for advanced message construction (key, properties, etc.).
     * Use {@link MessageBuilder#send()} as the terminal operation.
     *
     * @return a new {@link MessageBuilder} instance bound to this producer
     */
    MessageBuilder<T> newMessage();

    /**
     * The last sequence ID published by this producer. Used for deduplication tracking.
     * Returns {@code -1} if no message has been published yet.
     *
     * @return the last published sequence ID, or {@code -1} if none
     */
    long lastSequenceId();

    /**
     * Return the asynchronous view of this producer. The returned object shares the same
     * underlying connection and resources.
     *
     * @return the {@link AsyncProducer} counterpart of this producer
     */
    AsyncProducer<T> async();

    /**
     * Close this producer and release all associated resources. Pending send operations
     * are completed before the producer is closed.
     *
     * @throws PulsarClientException if an error occurs while closing
     */
    @Override
    void close() throws PulsarClientException;
}
