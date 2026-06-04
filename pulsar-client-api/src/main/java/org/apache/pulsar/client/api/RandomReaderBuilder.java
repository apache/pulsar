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

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * Builder for creating {@link RandomReader} instances.
 *
 * @param <T> the message payload type
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface RandomReaderBuilder<T> extends Cloneable {

    /**
     * Create a RandomReader synchronously.
     */
    RandomReader<T> create() throws PulsarClientException;

    /**
     * Create a RandomReader asynchronously.
     */
    CompletableFuture<RandomReader<T>> createAsync();

    /**
     * Load configuration from a map.
     */
    RandomReaderBuilder<T> loadConf(Map<String, Object> config);

    /**
     * Clone the builder.
     */
    RandomReaderBuilder<T> clone();

    /**
     * Set the topic name.
     */
    RandomReaderBuilder<T> topic(String topicName);

    /**
     * Set the reader name.
     */
    RandomReaderBuilder<T> readerName(String readerName);

    /**
     * Set a custom message payload processor.
     */
    RandomReaderBuilder<T> messagePayloadProcessor(MessagePayloadProcessor payloadProcessor);

    /**
     * Enable or disable pooled messages.
     */
    RandomReaderBuilder<T> poolMessages(boolean poolMessages);

    /**
     * If set to true, the RandomReader will filter out messages from aborted transactions
     * (READ_COMMITTED). By default (false), all messages including those from aborted
     * transactions are returned (READ_UNCOMMITTED).
     */
    RandomReaderBuilder<T> readCommitted(boolean readCommitted);

    /**
     * Add a key-value property.
     */
    RandomReaderBuilder<T> property(String key, String value);

    /**
     * Set multiple key-value properties.
     */
    RandomReaderBuilder<T> properties(Map<String, String> properties);
}
