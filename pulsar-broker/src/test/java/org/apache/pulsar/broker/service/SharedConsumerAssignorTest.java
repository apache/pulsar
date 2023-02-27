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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class SharedConsumerAssignorTest {

    private final ConsumerSelector roundRobinConsumerSelector = new ConsumerSelector();
    private final List<EntryAndMetadata> entryAndMetadataList = new ArrayList<>();
    private final List<EntryAndMetadata> replayQueue = new ArrayList<>();
    private SharedConsumerAssignor assignor;

    @BeforeMethod
    public void prepareData() {
        roundRobinConsumerSelector.clear();
        entryAndMetadataList.clear();
        replayQueue.clear();
        assignor = new SharedConsumerAssignor(roundRobinConsumerSelector, replayQueue::add);
        final AtomicLong entryId = new AtomicLong(0L);
        final MockProducer producerA = new MockProducer("A", entryId, entryAndMetadataList);
        final MockProducer producerB = new MockProducer("B", entryId, entryAndMetadataList);
        producerA.sendMessage();
        producerA.sendChunk(0, 3);
        producerA.sendChunk(1, 3);
        producerB.sendMessage();
        producerB.sendChunk(0, 2);
        producerA.sendChunk(2, 3);
        producerB.sendChunk(1, 2);
        // Use the following data for all tests
        // | entry id | uuid | chunk id | number of chunks |
        // | -------- | ---- | -------- | ---------------- |
        // | 0        | A-0  |          |                  |
        // | 1        | A-1  | 0        | 3                |
        // | 2        | A-1  | 1        | 3                |
        // | 3        | B-0  |          |                  |
        // | 4        | B-1  | 0        | 2                |
        // | 5        | A-1  | 2        | 3                |
        // | 6        | B-1  | 1        | 2               |
        // P.S. In the table above, The uuid represents the "<producer-name>-<sequence-id>" for non-chunks
        assertEquals(toString(entryAndMetadataList), Arrays.asList(
                "0:0@A-0", "0:1@A-1-0-3", "0:2@A-1-1-3", "0:3@B-0", "0:4@B-1-0-2", "0:5@A-1-2-3", "0:6@B-1-1-2"));
    }

    @Test
    public void testSingleConsumerMultiAssign() {
        // Only first 5 entries can be received because the number of permits is 5
        final Consumer consumer = new Consumer("A", 5);
        roundRobinConsumerSelector.addConsumers(consumer);

        Map<Consumer, List<EntryAndMetadata>> result = assignor.assign(entryAndMetadataList, 1);
        assertEquals(result.getOrDefault(consumer, Collections.emptyList()), entryAndMetadataList.subList(0, 5));
        // Since two chunked messages (A-1 and B-1) are both not received, these uuids have been cached
        assertEquals(assignor.getUuidToConsumer().keySet(), Sets.newHashSet("A-1", "B-1"));
        assertEquals(toString(replayQueue), Arrays.asList("0:5@A-1-2-3", "0:6@B-1-1-2"));

        result = assignor.assign(entryAndMetadataList.subList(5, 6), 1);
        assertEquals(result.getOrDefault(consumer, Collections.emptyList()), entryAndMetadataList.subList(5, 6));
        // A-1 is received so that uuid "A-1" has been removed from the cache
        assertEquals(assignor.getUuidToConsumer().keySet(), Sets.newHashSet("B-1"));

        result = assignor.assign(entryAndMetadataList.subList(6, 7), 1);
        assertEquals(result.getOrDefault(consumer, Collections.emptyList()), entryAndMetadataList.subList(6, 7));
        assertTrue(assignor.getUuidToConsumer().isEmpty());
    }

    @Test
    public void testMultiConsumerWithSmallPermits() {
        final Consumer consumerA = new Consumer("A", 3);
        final Consumer consumerB = new Consumer("B", 4);
        roundRobinConsumerSelector.addConsumers(consumerA, consumerB);

        // The original order:
        //   A-0, A-1-0-3, A-1-1-3, B-0, B-1-0-2, A-1-2-3, B-1-1-2
        // First consumerA received 3 entries and the available permits became 0. Then the consumer is switched to
        // consumerB and then B-0 and B-1-0-2 were received by it.
        // When consumerB tried to receive A-1-2-3, since the uuid "A-1" was already assigned to consumerA, consumerB
        // is not able to receive A-1-2-3.
        // However, since consumerA has no more permits, A-1-2-3 cannot be assigned to consumerA as well.
        // Therefore, A-1-2-3 was skipped and added to the replay queue.
        Map<Consumer, List<EntryAndMetadata>> result = assignor.assign(entryAndMetadataList, 2);
        assertEquals(toString(result.getOrDefault(consumerA, Collections.emptyList())),
                Arrays.asList("0:0@A-0", "0:1@A-1-0-3", "0:2@A-1-1-3"));
        assertEquals(toString(result.getOrDefault(consumerB, Collections.emptyList())),
                Arrays.asList("0:3@B-0", "0:4@B-1-0-2", "0:6@B-1-1-2"));
        assertEquals(toString(replayQueue), Collections.singletonList("0:5@A-1-2-3"));
        assertEquals(assignor.getUuidToConsumer().keySet(), Sets.newHashSet("A-1"));

        roundRobinConsumerSelector.clear();
        roundRobinConsumerSelector.addConsumers(consumerB, consumerA);
        assertSame(roundRobinConsumerSelector.peek(), consumerB);
        final List<EntryAndMetadata> leftEntries = new ArrayList<>(replayQueue);
        replayQueue.clear();
        result = assignor.assign(leftEntries, 2);
        // Since uuid "A-1" is still cached, A-1-2-3 will be dispatched to consumerA even if consumerB is the first
        // consumer returned by roundRobinConsumerSelector.get()
        assertEquals(toString(result.getOrDefault(consumerA, Collections.emptyList())),
                Collections.singletonList("0:5@A-1-2-3"));
        assertNull(result.get(consumerB));
        assertTrue(replayQueue.isEmpty());
        assertTrue(assignor.getUuidToConsumer().isEmpty());
    }

    @RequiredArgsConstructor
    static class ConsumerSelector implements Supplier<Consumer> {

        private final List<Consumer> consumers = new ArrayList<>();
        private int index = 0;

        public void addConsumers(Consumer... consumers) {
            this.consumers.addAll(Arrays.asList(consumers));
        }

        public void clear() {
            consumers.clear();
            index = 0;
        }

        @Override
        public Consumer get() {
            // a simple round-robin dispatcher
            final Consumer consumer = peek();
            if (consumer == null) {
                return null;
            }
            index++;
            return consumer;
        }

        public Consumer peek() {
            if (consumers.isEmpty()) {
                return null;
            }
            return consumers.get(index % consumers.size());
        }
    }

    private static List<String> toString(final List<EntryAndMetadata> entryAndMetadataList) {
        return entryAndMetadataList.stream().map(EntryAndMetadata::toString).collect(Collectors.toList());
    }

    @RequiredArgsConstructor
    static class MockProducer {
        final String name;
        final AtomicLong entryId;
        final List<EntryAndMetadata> entryAndMetadataList;
        long sequenceId = 0L;

        void sendMessage() {
            entryAndMetadataList.add(createEntryAndMetadata(entryId.getAndIncrement(),
                    createMetadata(name, sequenceId++, null, null)));
        }

        void sendChunk(int chunkId, int numChunks) {
            entryAndMetadataList.add(createEntryAndMetadata(entryId.getAndIncrement(),
                    createMetadata(name, sequenceId, chunkId, numChunks)));
            if (chunkId == numChunks - 1) {
                sequenceId++;
            }
        }
    }

    private static EntryAndMetadata createEntryAndMetadata(final long entryId,
                                                           final MessageMetadata metadata) {
        final ByteBuf payload = Commands.serializeMetadataAndPayload(
                Commands.ChecksumType.Crc32c, metadata, PulsarByteBufAllocator.DEFAULT.buffer());
        return EntryAndMetadata.create(EntryImpl.create(0L, entryId, payload));
    }

    private static MessageMetadata createMetadata(final String producerName,
                                                  final long sequenceId,
                                                  final Integer chunkId,
                                                  final Integer numChunks) {
        final MessageMetadata metadata = new MessageMetadata();
        metadata.setProducerName(producerName);
        metadata.setSequenceId(sequenceId);
        metadata.setPublishTime(0L);
        if (chunkId != null && numChunks != null) {
            metadata.setUuid(producerName + "-" + sequenceId);
            metadata.setChunkId(chunkId);
            metadata.setNumChunksFromMsg(numChunks);
        }
        return metadata;
    }
}
