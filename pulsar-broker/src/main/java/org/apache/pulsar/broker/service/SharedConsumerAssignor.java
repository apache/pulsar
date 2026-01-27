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
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.service.persistent.PulsarCompactorSubscription;
import org.apache.pulsar.common.api.proto.CommandAck.AckType;
import org.apache.pulsar.common.api.proto.MessageMetadata;

/**
 * The assigner to assign entries to the proper {@link Consumer} in the shared subscription.
 */
@Slf4j
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
                log.info("No consumer found to assign in topic:{}, subscription:{}, redelivering {} messages.",
                        subscription.getTopic().getName(), subscription.getName(), entryAndMetadataList.size());
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
                availablePermits--;
            } else {
                final String uuid = metadata.getUuid();
                Consumer consumerForUuid = uuidToConsumer.get(uuid);
                if (consumerForUuid == null) {
                    if (metadata.getChunkId() != 0) {
                        if (subscription != null) {
                            log.warn("[{}][{}] Skip the message because it is not the first chunk."
                                            + " Position: {}, UUID: {}, ChunkId: {}, NumChunksFromMsg: {}",
                                    subscription.getTopicName(), subscription.getName(), entryAndMetadata.getPosition(),
                                    metadata.getUuid(), metadata.getChunkId(), metadata.getNumChunksFromMsg());
                            // Directly ack the message.
                            if (!(subscription instanceof PulsarCompactorSubscription)) {
                                subscription.acknowledgeMessage(Collections.singletonList(
                                        entryAndMetadata.getPosition()), AckType.Individual, Collections.emptyMap());
                                entryAndMetadata.release();
                            }
                        }
                        continue;
                    }
                    consumerForUuid = consumer;
                    uuidToConsumer.put(uuid, consumerForUuid);
                }

                final int permits = consumerToPermits.computeIfAbsent(consumerForUuid, Consumer::getAvailablePermits);
                if (permits <= 0) {
                    unassignedMessageProcessor.accept(entryAndMetadata);
                    continue;
                }
                if (metadata.getChunkId() == metadata.getNumChunksFromMsg() - 1) {
                    // The last chunk is received, we should remove the uuid from the cache.
                    uuidToConsumer.remove(uuid);
                }

                consumerToEntries.computeIfAbsent(consumerForUuid, __ -> new ArrayList<>()).add(entryAndMetadata);
                consumerToPermits.put(consumerForUuid, permits - 1);
                if (consumerForUuid == consumer) {
                    availablePermits--;
                }
            }
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
}
