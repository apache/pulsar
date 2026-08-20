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

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import lombok.CustomLog;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.pulsar.common.api.proto.MessageMetadata;

/**
 * The assigner to assign entries to the proper {@link Consumer} in the shared subscription.
 */
@CustomLog
@RequiredArgsConstructor
public class SharedConsumerAssignor {

    // The cache to map uuid to a consumer because we need to guarantee all chunks with the same uuid to be dispatched
    // to a fixed consumer until the whole chunked message is dispatched.
    @Getter
    @VisibleForTesting
    private final Map<String, Consumer> uuidToConsumer = new ConcurrentHashMap<>();

    // A temporary cache that is cleared each time `assign()` is called
    private final Map<Consumer, Integer> consumerToPermits = new IdentityHashMap<>();

    // The selector for entries without uuid. The Consumer returned must have at least 1 permit.
    private final Supplier<Consumer> defaultSelector;

    // Process the unassigned messages, e.g. adding them to the replay queue
    private final java.util.function.Consumer<EntryAndMetadata> unassignedMessageProcessor;

    private final Subscription subscription;

    public Map<Consumer, List<EntryAndMetadata>> assign(final List<EntryAndMetadata> entryAndMetadataList,
                                                        final int numConsumers) {
        assert numConsumers >= 0;
        consumerToPermits.clear();
        final Map<Consumer, List<EntryAndMetadata>> consumerToEntries = new IdentityHashMap<>();

        Consumer consumer = getConsumer(numConsumers);
        if (consumer == null) {
            if (subscription != null) {
                log.info()
                        .attr("topic", subscription.getTopic().getName())
                        .attr("subscription", subscription.getName())
                        .attr("size", entryAndMetadataList.size())
                        .log("No consumer found to assign, redelivering messages");
            }
            entryAndMetadataList.forEach(unassignedMessageProcessor);
            return consumerToEntries;
        }
        // The actual available permits might change, here we use the permits at the moment to assign entries
        int availablePermits = consumerToPermits.computeIfAbsent(consumer, Consumer::getAvailablePermits);
        int index = 0;
        for (; index < entryAndMetadataList.size(); index++) {
            final EntryAndMetadata entryAndMetadata = entryAndMetadataList.get(index);
            final MessageMetadata metadata = entryAndMetadata.getMetadata();

            // Select another consumer to ensure `consumer != null` and `availablePermits > 0`
            if (availablePermits <= 0) {
                consumerToPermits.put(consumer, availablePermits);
                consumer = getConsumer(numConsumers);
                if (consumer == null) {
                    break;
                }
                availablePermits = consumer.getAvailablePermits();
            }

            if (metadata == null || !metadata.hasUuid() || !metadata.hasChunkId() || !metadata.hasNumChunksFromMsg()) {
                consumerToEntries.computeIfAbsent(consumer, __ -> new ArrayList<>()).add(entryAndMetadata);
            } else {
                final Consumer consumerForUuid = getConsumerForUuid(metadata, consumer);
                if (consumerForUuid == null) {
                    unassignedMessageProcessor.accept(entryAndMetadata);
                    continue;
                }
                consumerToEntries.computeIfAbsent(consumerForUuid, __ -> new ArrayList<>()).add(entryAndMetadata);
            }
            availablePermits--;
        }

        for (; index < entryAndMetadataList.size(); index++) {
            unassignedMessageProcessor.accept(entryAndMetadataList.get(index));
        }

        return consumerToEntries;
    }

    private Consumer getConsumer(final int numConsumers) {
        for (int i = 0; i < numConsumers; i++) {
            final Consumer consumer = defaultSelector.get();
            if (consumer == null) {
                return null;
            }
            final int permits = consumerToPermits.computeIfAbsent(consumer, Consumer::getAvailablePermits);
            if (permits > 0) {
                return consumer;
            }
        }
        return null;
    }

    /**
     * Remove every uuid mapping that points to the given consumer. Must be called when a consumer is removed from the
     * dispatcher: a mapping to a disconnected consumer has no permits left, so the remaining chunks of that uuid stay
     * unassignable and stall the subscription. Also bounds the map when the last chunk is never published.
     */
    public void removeConsumer(final Consumer consumer) {
        uuidToConsumer.values().removeIf(cachedConsumer -> cachedConsumer == consumer);
    }

    /** Drop all uuid mappings, e.g. after all consumers have been removed from the dispatcher. */
    public void clear() {
        uuidToConsumer.clear();
    }

    private Consumer getConsumerForUuid(final MessageMetadata metadata, final Consumer defaultConsumer) {
        final String uuid = metadata.getUuid();
        Consumer consumer = uuidToConsumer.get(uuid);
        if (consumer == null) {
            consumer = defaultConsumer;
            if (metadata.getChunkId() == 0) {
                uuidToConsumer.put(uuid, consumer);
            } else {
                // Orphan chunk: only chunk 0 creates the mapping, and the cursor always re-reads chunk 0 before
                // this one while it is still unacked, so a missing mapping means chunk 0 is acknowledged and gone.
                // Never replay it: the dispatcher drains the replay queue before every normal read, so an entry that
                // can never be assigned stalls the whole subscription. Dispatch it and let the client discard it.
                if (subscription != null) {
                    log.warn()
                            .attr("topic", subscription.getTopic().getName())
                            .attr("subscription", subscription.getName())
                            .attr("uuid", uuid)
                            .attr("chunkId", metadata.getChunkId())
                            .attr("numChunks", metadata.getNumChunksFromMsg())
                            .attr("consumer", defaultConsumer)
                            .log("Dispatching orphan chunk whose first chunk is gone. The client should discard and"
                                    + " acknowledge it");
                }
            }
        }
        final int permits = consumerToPermits.computeIfAbsent(consumer, Consumer::getAvailablePermits);
        if (permits <= 0) {
            return null;
        }
        if (metadata.getChunkId() == metadata.getNumChunksFromMsg() - 1) {
            // The last chunk is received, we should remove the cache
            uuidToConsumer.remove(uuid);
        }
        // Decrement target consumer's permits, not the loop's local availablePermits — on a cache
        // redirect those track different consumers.
        consumerToPermits.put(consumer, permits - 1);
        return consumer;
    }
}
