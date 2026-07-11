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
 * An opaque, serializable position vector representing a consistent point across all
 * internal hash-range segments of a topic.
 *
 * <p>Checkpoints are created via {@link CheckpointConsumer#checkpoint()} and can be
 * serialized for external storage (e.g. Flink state, S3) using {@link #toByteArray()}.
 *
 * <p>This is the sole position type used with {@link CheckpointConsumer} — for initial
 * positioning use the static factories {@link #earliest()}, {@link #latest()}, or
 * {@link #fromByteArray(byte[])} to restore from a previously saved checkpoint.
 *
 * <p>For timestamp-based positioning, use the {@code scalable-topics seek} admin
 * operation on the subscription instead — see
 * {@link org.apache.pulsar.client.admin.ScalableTopics#seekSubscription}.
 */
public interface Checkpoint {

    /**
     * Serialize this checkpoint for external storage.
     *
     * @return a serializable byte representation of this checkpoint that can be restored
     *         via {@link #fromByteArray(byte[])}
     */
    byte[] toByteArray();

    // --- Static factories ---

    /**
     * A sentinel checkpoint representing the beginning of the topic (oldest available data).
     *
     * @return a sentinel {@link Checkpoint} representing the earliest position in the topic
     */
    static Checkpoint earliest() {
        return PulsarClientProvider.get().earliestCheckpoint();
    }

    /**
     * A sentinel checkpoint representing the end of the topic (next message to be published).
     *
     * @return a sentinel {@link Checkpoint} representing the latest position in the topic
     */
    static Checkpoint latest() {
        return PulsarClientProvider.get().latestCheckpoint();
    }

    /**
     * Deserialize a checkpoint from a byte array previously obtained via {@link #toByteArray()}.
     *
     * @param data the byte array previously obtained from {@link #toByteArray()}
     * @return the deserialized {@link Checkpoint}
     * @throws IOException if the byte array is malformed or cannot be deserialized
     */
    static Checkpoint fromByteArray(byte[] data) throws IOException {
        return PulsarClientProvider.get().checkpointFromBytes(data);
    }
}
