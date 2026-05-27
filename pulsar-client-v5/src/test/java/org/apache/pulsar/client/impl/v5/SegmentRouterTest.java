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
        return new ActiveSegment(id, HashRange.of(start, end), "persistent://t/n/seg-" + id, null);
    }

    /** Build a legacy segment (synthetic-layout entry wrapping an externally managed persistent:// topic). */
    private static ActiveSegment legacySeg(long id, int start, int end, String underlying) {
        return new ActiveSegment(id, HashRange.of(start, end), "segment://t/n/x/" + id, underlying);
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

    // --- mod-N routing for synthetic layouts (all legacy segments) ---

    @Test
    public void testAllLegacySegmentsRouteModN() {
        // Synthetic layout for a 4-partition regular topic: 4 legacy segments
        // with segment_id == partition_index. routing must match v4 partitioned-topic
        // routing (signSafeMod(murmurHash3_32(key), N)).
        SegmentRouter router = new SegmentRouter();
        int n = 4;
        List<ActiveSegment> segments = List.of(
                legacySeg(0, 0x0000, 0x3FFF, "persistent://t/n/x-partition-0"),
                legacySeg(1, 0x4000, 0x7FFF, "persistent://t/n/x-partition-1"),
                legacySeg(2, 0x8000, 0xBFFF, "persistent://t/n/x-partition-2"),
                legacySeg(3, 0xC000, 0xFFFF, "persistent://t/n/x-partition-3"));

        // For a synthetic layout, the router must NOT use hash ranges — it must do
        // mod-N over segment_id. Verify a handful of keys land on the expected
        // partition computed exactly as v4 would.
        for (String key : new String[]{"a", "customer-1", "customer-2", "order-99", ""}) {
            int hash32 = org.apache.pulsar.common.util.Murmur3_32Hash.getInstance()
                    .makeHash(key.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            int mod = hash32 % n;
            int expected = mod < 0 ? mod + n : mod;
            assertEquals(router.route(key, segments), expected,
                    "key=" + key + " expected v4 mod-N partition " + expected);
        }
    }

    @Test
    public void testMixedLegacyAndRegularStillUsesHashRouting() {
        // After migration, the DAG has sealed legacy parents + active range-based children.
        // Routing on the active set is range-based — the all-legacy special-case only
        // triggers when *every* active segment is a legacy segment.
        SegmentRouter router = new SegmentRouter();
        List<ActiveSegment> mixed = List.of(
                legacySeg(0, 0x0000, 0x7FFF, "persistent://t/n/x-partition-0"),
                seg(1, 0x8000, 0xFFFF));

        // pick a key whose hash falls in the range owned by the *regular* segment;
        // it must land there, not on segment_id=mod.
        String regularKey = findKeyInRange(0x8000, 0xFFFF);
        assertEquals(router.route(regularKey, mixed), 1L);
    }

    @Test
    public void testAllLegacyRoutingIsDeterministic() {
        SegmentRouter router = new SegmentRouter();
        List<ActiveSegment> segments = List.of(
                legacySeg(0, 0x0000, 0x7FFF, "persistent://t/n/x-partition-0"),
                legacySeg(1, 0x8000, 0xFFFF, "persistent://t/n/x-partition-1"));
        long first = router.route("stable", segments);
        for (int i = 0; i < 20; i++) {
            assertEquals(router.route("stable", segments), first);
        }
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
