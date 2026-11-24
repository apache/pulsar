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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pulsar.client.api.Range;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class EntryBucketConsumerSelectorTest {

    private static final List<Range> FOUR_BUCKETS = List.of(
            Range.of(0x0000, 0x3FFF),
            Range.of(0x4000, 0x7FFF),
            Range.of(0x8000, 0xBFFF),
            Range.of(0xC000, 0xFFFF));

    private static Consumer mockConsumer(String name, long id) {
        Consumer consumer = mock(Consumer.class);
        when(consumer.consumerName()).thenReturn(name);
        when(consumer.consumerId()).thenReturn(id);
        return consumer;
    }

    @Test
    public void testBucketIndexAndCanonicalHash() {
        EntryBucketConsumerSelector selector = new EntryBucketConsumerSelector(FOUR_BUCKETS);
        assertEquals(selector.bucketIndexOfHash(0x0000), 0);
        assertEquals(selector.bucketIndexOfHash(0x3FFF), 0);
        assertEquals(selector.bucketIndexOfHash(0x4000), 1);
        assertEquals(selector.bucketIndexOfHash(0x7FFF), 1);
        assertEquals(selector.bucketIndexOfHash(0x8000), 2);
        assertEquals(selector.bucketIndexOfHash(0xC000), 3);
        assertEquals(selector.bucketIndexOfHash(0xFFFF), 3);

        // Bucket 0's canonical hash is nudged to 1 (0 is the reserved "not set" value).
        assertEquals(selector.canonicalHashOfBucket(0), 1);
        assertEquals(selector.canonicalHashOfBucket(1), 0x4000);
        assertEquals(selector.canonicalHashOfBucket(2), 0x8000);
        assertEquals(selector.canonicalHashOfBucket(3), 0xC000);

        assertEquals(selector.canonicalHashOf(0x1234), 1);
        assertEquals(selector.canonicalHashOf(0x4000), 0x4000);
        assertEquals(selector.canonicalHashOf(0x7FFF), 0x4000);
        assertEquals(selector.canonicalHashOf(0xFFFF), 0xC000);
    }

    @Test
    public void testDeterministicSpreadIsAddOrderIndependent() {
        Consumer a = mockConsumer("consumer-a", 1);
        Consumer b = mockConsumer("consumer-b", 2);

        EntryBucketConsumerSelector selector1 = new EntryBucketConsumerSelector(FOUR_BUCKETS);
        selector1.addConsumer(a);
        selector1.addConsumer(b);

        EntryBucketConsumerSelector selector2 = new EntryBucketConsumerSelector(FOUR_BUCKETS);
        selector2.addConsumer(b);
        selector2.addConsumer(a);

        // Sorted by name: "consumer-a" owns buckets 0-1, "consumer-b" owns buckets 2-3,
        // regardless of connection order.
        for (int bucket = 0; bucket < 4; bucket++) {
            int hash = selector1.canonicalHashOfBucket(bucket);
            Consumer expected = bucket < 2 ? a : b;
            assertSame(selector1.select(hash), expected);
            assertSame(selector2.select(hash), expected);
        }
    }

    @Test
    public void testSpreadWithMoreConsumersThanBuckets() {
        EntryBucketConsumerSelector selector = new EntryBucketConsumerSelector(FOUR_BUCKETS);
        for (int i = 0; i < 6; i++) {
            selector.addConsumer(mockConsumer("consumer-" + i, i));
        }
        // Every bucket has exactly one owner; only 4 of the 6 consumers own a bucket.
        Set<Consumer> owners = new HashSet<>();
        for (int bucket = 0; bucket < 4; bucket++) {
            Consumer owner = selector.select(selector.canonicalHashOfBucket(bucket));
            assertNotNull(owner);
            owners.add(owner);
        }
        assertEquals(owners.size(), 4);
    }

    @Test
    public void testMakeStickyKeyHashNormalizesToCanonical() {
        EntryBucketConsumerSelector selector = new EntryBucketConsumerSelector(FOUR_BUCKETS);
        for (int i = 0; i < 100; i++) {
            byte[] key = ("key-" + i).getBytes(StandardCharsets.UTF_8);
            int rawHash = StickyKeyConsumerSelectorUtils.makeStickyKeyHash(key, selector.getKeyHashRange());
            int hash = selector.makeStickyKeyHash(key);
            assertEquals(hash, selector.canonicalHashOf(rawHash));
            assertEquals(selector.bucketIndexOfHash(hash), selector.bucketIndexOfHash(rawHash));
        }
    }

    @Test
    public void testMembershipChangeReportsImpactedBuckets() {
        Consumer a = mockConsumer("consumer-a", 1);
        Consumer b = mockConsumer("consumer-b", 2);

        EntryBucketConsumerSelector selector = new EntryBucketConsumerSelector(FOUR_BUCKETS);
        selector.addConsumer(a).join();

        // consumer-b joining takes buckets 2-3 away from consumer-a.
        ImpactedConsumersResult impacted = selector.addConsumer(b).join().get();
        Map<Consumer, UpdatedHashRanges> removed = new HashMap<>();
        impacted.processUpdatedHashRanges((consumer, ranges, op) -> {
            if (op == ImpactedConsumersResult.OperationType.REMOVE) {
                removed.put(consumer, ranges);
            }
        });
        assertEquals(removed.keySet(), Set.of(a));
        UpdatedHashRanges lostByA = removed.get(a);
        assertFalse(lostByA.containsStickyKey(selector.canonicalHashOfBucket(0)));
        assertFalse(lostByA.containsStickyKey(selector.canonicalHashOfBucket(1)));
        assertTrue(lostByA.containsStickyKey(selector.canonicalHashOfBucket(2)));
        assertTrue(lostByA.containsStickyKey(selector.canonicalHashOfBucket(3)));

        // consumer-b leaving hands buckets 2-3 back: the removal is attributed to consumer-b.
        ImpactedConsumersResult afterLeave = selector.removeConsumer(b).get();
        Map<Consumer, UpdatedHashRanges> removedOnLeave = new HashMap<>();
        afterLeave.processUpdatedHashRanges((consumer, ranges, op) -> {
            if (op == ImpactedConsumersResult.OperationType.REMOVE) {
                removedOnLeave.put(consumer, ranges);
            }
        });
        assertEquals(removedOnLeave.keySet(), Set.of(b));
        for (int bucket = 0; bucket < 4; bucket++) {
            assertSame(selector.select(selector.canonicalHashOfBucket(bucket)), a);
        }
    }

    @Test
    public void testSelectWithNoConsumers() {
        EntryBucketConsumerSelector selector = new EntryBucketConsumerSelector(FOUR_BUCKETS);
        assertNull(selector.select(1));

        Consumer a = mockConsumer("consumer-a", 1);
        selector.addConsumer(a).join();
        selector.removeConsumer(a);
        assertNull(selector.select(1));
    }

    @Test
    public void testConsumerHashAssignmentsSnapshot() {
        Consumer a = mockConsumer("consumer-a", 1);
        Consumer b = mockConsumer("consumer-b", 2);
        EntryBucketConsumerSelector selector = new EntryBucketConsumerSelector(FOUR_BUCKETS);
        selector.addConsumer(a).join();
        selector.addConsumer(b).join();

        // Contiguous bucket slices are merged: a owns buckets 0-1, b owns buckets 2-3.
        Map<Consumer, List<Range>> ranges = selector.getConsumerHashAssignmentsSnapshot().getRangesByConsumer();
        assertEquals(ranges.get(a), List.of(Range.of(0x0000, 0x7FFF)));
        assertEquals(ranges.get(b), List.of(Range.of(0x8000, 0xFFFF)));
    }
}
