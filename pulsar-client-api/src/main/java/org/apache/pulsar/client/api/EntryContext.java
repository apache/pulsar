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
 * The context of an entry, which usually represents a message of a batch if batching is enabled.
 */
public interface EntryContext {

    /**
     * Get a value associated with the given key.
     *
     * @param key
     * @return the value associated with the key or null if the key or value doesn't exist
     */
    String getProperty(String key);

    /**
     * Get the number of messages.
     *
     * Since the message could be batched, a message could have multiple internal single messages.
     *
     * @return the number of internal single messages or 1 if the message is not batched.
     */
    int getNumMessages();

    /**
     * Check whether the entry is a batch.
     *
     * @return true if the entry is a batch.
     */
    boolean isBatch();

    /**
     * Get the internal single message with a specific index from a payload if the entry is a batch.
     *
     * @param index the batch index
     * @param numMessages the number of messages in the batch
     * @param payload the message payload
     * @param containMetadata whether the payload contains the single message metadata
     * @param schema the schema of the batch
     * @param <T>
     * @return the single message
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
     * Convert the given payload to a single message if the entry is non-batched.
     *
     * @param payload the message payload
     * @param schema the schema of the message
     * @param <T>
     * @return the converted single message
     */
    <T> Message<T> asSingleMessage(MessagePayload payload, Schema<T> schema);
}
