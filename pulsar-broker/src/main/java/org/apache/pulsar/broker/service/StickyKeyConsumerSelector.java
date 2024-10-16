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
package org.apache.pulsar.broker.service;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.Range;

/**
 * Abstraction for selecting the same consumer based on a key.
 * This interface provides methods to add and remove consumers,
 * select a consumer based on a sticky key or hash, and retrieve
 * the hash range assignments for consumers. This is used by the Key_Shared implementation.
 */
public interface StickyKeyConsumerSelector {
    /**
     * The default range size used for hashing.
     * This should be a power of 2 so that it's compatible with all implementations.
     */
    int DEFAULT_RANGE_SIZE =  2 << 15;

    /**
     * The value used to indicate that sticky key hash is not set.
     * This value cannot be -1 since some of the data structures require non-negative values.
     */
    int STICKY_KEY_HASH_NOT_SET = 0;

    /**
     * Add a new consumer.
     *
     * @param consumer the new consumer to be added
     * @return a CompletableFuture that completes with the result of impacted consumers.
     *         The result contains information about the existing consumers whose hash ranges were affected
     *         by the addition of the new consumer.
     */
    CompletableFuture<Optional<ImpactedConsumersResult>> addConsumer(Consumer consumer);

    /**
     * Remove the consumer.
     *
     * @param consumer the consumer to be removed
     * @return the result of impacted consumers. The result contains information about the existing consumers
     *         whose hash ranges were affected by the removal of the consumer.
     */
    Optional<ImpactedConsumersResult> removeConsumer(Consumer consumer);

    /**
     * Select a consumer by sticky key.
     *
     * @param stickyKey the sticky key to select the consumer
     * @return the selected consumer
     */
    default Consumer select(byte[] stickyKey) {
        return select(makeStickyKeyHash(stickyKey));
    }

    /**
     * Make a hash from the sticky key. The hash value is in the range returned by the {@link #getKeyHashRange()}
     * method instead of in the full range of integers. In other words, this returns the "slot".
     *
     * @param stickyKey the sticky key to hash
     * @return the generated hash value
     */
    default int makeStickyKeyHash(byte[] stickyKey) {
        return StickyKeyConsumerSelectorUtils.makeStickyKeyHash(stickyKey, getKeyHashRange());
    }

    /**
     * Select a consumer by hash.
     *
     * @param hash the hash corresponding to the sticky key
     * @return the selected consumer
     */
    Consumer select(int hash);

    /**
     * Get the full range of hash values used by this selector. The upper bound is exclusive.
     *
     * @return the full range of hash values
     */
    Range getKeyHashRange();

    /**
     * Get key hash ranges handled by each consumer.
     *
     * @return a map where the key is a consumer and the value is a list of hash ranges it is receiving messages for
     */
    default Map<Consumer, List<Range>> getConsumerKeyHashRanges() {
        return getConsumerHashAssignmentsSnapshot().getRangesByConsumer();
    }

    /**
     * Get the current mappings of hash range to consumer.
     *
     * @return a snapshot of the consumer hash assignments
     */
    ConsumerHashAssignmentsSnapshot getConsumerHashAssignmentsSnapshot();
}