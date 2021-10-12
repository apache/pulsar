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

/**
 * The context of the message payload, which usually represents a batched message (batch) or a single message.
 */
public interface MessagePayloadContext {

    /**
     * Get a value associated with the given key.
     *
     * When the message payload is not produced by Pulsar producer, a specific property is usually added to indicate the
     * format. So this method is useful to determine whether the payload is produced by Pulsar producer.
     *
     * @param key
     * @return the value associated with the key or null if the key or value doesn't exist
     */
    String getProperty(String key);

    /**
     * Get the number of messages when the payload is produced by Pulsar producer.
     *
     * @return the number of messages
     */
    int getNumMessages();

    /**
     * Check whether the payload is a batch when the payload is produced by Pulsar producer.
     *
     * @return true if the payload is a batch
     */
    boolean isBatch();

    /**
     * Get the internal single message with a specific index from a payload if the payload is a batch.
     *
     * @param index the batch index
     * @param numMessages the number of messages in the batch
     * @param payload the message payload
     * @param containMetadata whether the payload contains the single message metadata
     * @param schema the schema of the batch
     * @param <T>
     * @return the created message
     * @implNote The `index` and `numMessages` parameters are used to create the message id with batch index.
     *   If `containMetadata` is true, parse the single message metadata from the payload first. The fields of single
     *   message metadata will overwrite the same fields of the entry's metadata.
     */
    <T> Message<T> getMessageAt(int index,
                                int numMessages,
                                MessagePayload payload,
                                boolean containMetadata,
                                Schema<T> schema);

    /**
     * Convert the given payload to a single message if the entry is not a batch.
     *
     * @param payload the message payload
     * @param schema the schema of the message
     * @param <T>
     * @return the created message
     */
    <T> Message<T> asSingleMessage(MessagePayload payload, Schema<T> schema);
}
