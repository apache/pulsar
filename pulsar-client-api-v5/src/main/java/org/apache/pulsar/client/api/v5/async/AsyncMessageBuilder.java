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
package org.apache.pulsar.client.api.v5.async;

import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.v5.MessageId;
import org.apache.pulsar.client.api.v5.MessageMetadata;

/**
 * Asynchronous message builder, obtained from {@link AsyncProducer#newMessage()}.
 *
 * <p>Inherits all metadata setters from {@link MessageMetadata} and adds an
 * asynchronous {@link #send()} terminal operation.
 *
 * @param <T> the type of the message value
 */
public interface AsyncMessageBuilder<T> extends MessageMetadata<T, AsyncMessageBuilder<T>> {

    /**
     * Send the message asynchronously.
     *
     * <p>The message is charged against the client memory limit before this method returns. When
     * the limit is reached, the call blocks until pending messages have been acknowledged and there
     * is room again, or, with {@link org.apache.pulsar.client.api.v5.ProducerBuilder#blockIfQueueFull(boolean)}
     * set to {@code false}, the returned future fails right away with
     * {@link org.apache.pulsar.client.api.v5.PulsarClientException.MemoryBufferIsFullException}.
     * Once accepted, the message is queued and the future completes when the broker acknowledges it.
     *
     * <p>The future completes on the client's IO thread that received the broker's response, so
     * code chained on it must not block. A send issued from such a continuation never waits for room
     * under the memory limit: at the limit it fails with the same exception, whatever the
     * {@code blockIfQueueFull} setting.
     *
     * @return a {@link CompletableFuture} that completes with the {@link MessageId} assigned
     *         to the published message by the broker
     */
    CompletableFuture<MessageId> send();
}
