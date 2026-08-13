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
 * <p>Inherits all metadata setters from {@link MessageMetadata} and adds a
 * non-blocking {@link #send()} terminal operation.
 *
 * @param <T> the type of the message value
 */
public interface AsyncMessageBuilder<T> extends MessageMetadata<T, AsyncMessageBuilder<T>> {

    /**
     * Send the message asynchronously.
     *
     * @return a {@link CompletableFuture} that completes with the {@link MessageId} assigned
     *         to the published message by the broker
     */
    CompletableFuture<MessageId> send();
}
