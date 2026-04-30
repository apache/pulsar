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
package org.apache.pulsar.client.impl.v5;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.pulsar.client.impl.v5.SegmentRouter.ActiveSegment;
import org.apache.pulsar.common.scalable.HashRange;
import org.testng.annotations.Test;

public class SegmentRouterTest {

    private static ActiveSegment seg(long id, int start, int end) {
        return new ActiveSegment(id, HashRange.of(start, end), "persistent://t/n/seg-" + id);
    }

    // --- route(key, ...) ---

    @Test
    public void testRouteByKeyLandsOnCorrectSegment() {
        SegmentRouter router = new SegmentRouter();
        // 4 equal segments covering 0x0000..0xFFFF.
        List<ActiveSegment> segments = List.of(
                seg(0, 0x0000, 0x3FFF),
                seg(1, 0x4000, 0x7FFF),
                seg(2, 0x8000, 0xBFFF),
                seg(3, 0xC000, 0xFFFF));

        // Route a handful of keys and verify each hash falls inside the assigned range.
        for (String key : new String[]{"a", "b", "c", "d", "e", "f", "g", "order-42", "customer-99"}) {
            long id = router.route(key, segments);
            ActiveSegment picked = segments.stream()
                    .filter(s -> s.segmentId() == id)
                    .findFirst().orElseThrow();
            int hash = SegmentRouter.hash(key);
            assertTrue(picked.hashRange().contains(hash),
                    "key=" + key + " hash=" + hash + " picked=" + picked);
        }
    }

    @Test
    public void testRouteByKeyIsDeterministic() {
        SegmentRouter router = new SegmentRouter();
        List<ActiveSegment> segments = List.of(
                seg(0, 0x0000, 0x7FFF),
                seg(1, 0x8000, 0xFFFF));
        long first = router.route("stable-key", segments);
        for (int i = 0; i < 20; i++) {
            assertEquals(router.route("stable-key", segments), first);
        }
    }

    @Test
    public void testRouteWithSingleSegmentAlwaysReturnsThatSegment() {
        SegmentRouter router = new SegmentRouter();
        List<ActiveSegment> segments = List.of(seg(0, 0x0000, 0xFFFF));
        for (String key : new String[]{"a", "b", "zzzzzz", ""}) {
            assertEquals(router.route(key, segments), 0L);
        }
    }

    @Test
    public void testRouteWithEmptyListThrows() {
        SegmentRouter router = new SegmentRouter();
        assertThrows(IllegalStateException.class, () -> router.route("k", List.of()));
    }

    @Test
    public void testRouteWithIncompleteCoverageThrowsForUncoveredHash() {
        // Only segments covering the first half of the hash space. Find a key whose hash
        // lands in the uncovered half and verify the router throws.
        SegmentRouter router = new SegmentRouter();
        List<ActiveSegment> segments = List.of(seg(0, 0x0000, 0x7FFF));
        String uncoveredKey = findKeyInRange(0x8000, 0xFFFF);
        assertThrows(IllegalStateException.class,
                () -> router.route(uncoveredKey, segments));
    }

    // --- routeRoundRobin ---

    @Test
    public void testRoundRobinCyclesThroughSegments() {
        SegmentRouter router = new SegmentRouter();
        List<ActiveSegment> segments = List.of(seg(0, 0, 0x3FFF), seg(1, 0x4000, 0x7FFF),
                seg(2, 0x8000, 0xBFFF));
        long first = router.routeRoundRobin(segments);
        long second = router.routeRoundRobin(segments);
        long third = router.routeRoundRobin(segments);
        long fourth = router.routeRoundRobin(segments);

        // Three distinct segments across three consecutive calls; fourth wraps to `first`.
        Set<Long> distinct = new HashSet<>();
        distinct.add(first);
        distinct.add(second);
        distinct.add(third);
        assertEquals(distinct.size(), 3, "round-robin must visit every segment once per cycle");
        assertEquals(fourth, first, "fourth call wraps back to the first segment");
    }

    @Test
    public void testRoundRobinWithEmptyListThrows() {
        SegmentRouter router = new SegmentRouter();
        assertThrows(IllegalStateException.class, () -> router.routeRoundRobin(List.of()));
    }

    // --- hash ---

    @Test
    public void testHashIsWithin16BitSpace() {
        // Every hash must fit in the 16-bit hash space [0, 0xFFFF].
        for (int i = 0; i < 1000; i++) {
            int h = SegmentRouter.hash("key-" + i);
            assertTrue(h >= 0 && h <= 0xFFFF, "hash out of 16-bit space: " + h);
        }
    }

    @Test
    public void testHashIsDeterministic() {
        assertEquals(SegmentRouter.hash("deterministic-key"),
                SegmentRouter.hash("deterministic-key"));
        assertEquals(SegmentRouter.hash(""), SegmentRouter.hash(""));
    }

    @Test
    public void testHashStringAndBytesAgree() {
        String key = "some-key";
        int fromString = SegmentRouter.hash(key);
        int fromBytes = SegmentRouter.hash(key.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        assertEquals(fromString, fromBytes);
    }

    // --- helpers ---

    /**
     * Find any key whose hash lands in [start, end].
     */
    private static String findKeyInRange(int start, int end) {
        for (int i = 0; i < 100_000; i++) {
            String k = "probe-" + i;
            int h = SegmentRouter.hash(k);
            if (h >= start && h <= end) {
                return k;
            }
        }
        throw new AssertionError("Failed to find a key hashing into [" + start + ", " + end + "]");
    }
}
