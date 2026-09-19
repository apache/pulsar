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

import java.io.IOException;
import org.apache.pulsar.client.api.v5.internal.PulsarClientProvider;

/**
 * Opaque, immutable identifier for a message within a topic.
 *
 * <p>No internal structure (ledger ID, entry ID, partition index) is exposed.
 * Message IDs can be serialized to bytes for external storage and restored later.
 */
public interface MessageId extends Comparable<MessageId> {

    /**
     * Serialize this message ID to a byte array for external storage.
     *
     * @return a byte array representation of this message ID that can be restored
     *         via {@link #fromByteArray(byte[])}
     */
    byte[] toByteArray();

    /**
     * Deserialize a message ID from bytes previously produced by {@link #toByteArray()}.
     *
     * @param data the byte array previously obtained from {@link #toByteArray()}
     * @return the deserialized {@link MessageId}
     * @throws IOException if the byte array is malformed or cannot be deserialized
     */
    static MessageId fromByteArray(byte[] data) throws IOException {
        return PulsarClientProvider.get().messageIdFromBytes(data);
    }

    /**
     * Sentinel representing the oldest available message in the topic.
     *
     * @return a sentinel {@link MessageId} representing the earliest position in the topic
     */
    static MessageId earliest() {
        return PulsarClientProvider.get().earliestMessageId();
    }

    /**
     * Sentinel representing the next message to be published (i.e., the end of the topic).
     *
     * @return a sentinel {@link MessageId} representing the latest position in the topic
     */
    static MessageId latest() {
        return PulsarClientProvider.get().latestMessageId();
    }
}
