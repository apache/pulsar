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
package org.apache.pulsar.broker.service.persistent;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import java.util.List;
import org.apache.pulsar.client.api.Range;
import org.apache.pulsar.common.api.proto.KeySharedMeta;
import org.apache.pulsar.common.api.proto.KeySharedMode;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class PersistentEntryBucketDispatcherMultipleConsumersTest {

    private static KeySharedMeta ksmWithRanges(int[][] ranges) {
        KeySharedMeta ksm = new KeySharedMeta().setKeySharedMode(KeySharedMode.STICKY)
                .setEntryBucketDispatch(true);
        for (int[] range : ranges) {
            ksm.addHashRange().setStart(range[0]).setEnd(range[1]);
        }
        return ksm;
    }

    @Test
    public void testValidBoundaries() {
        List<Range> ranges = PersistentEntryBucketDispatcherMultipleConsumers.validateBucketBoundaries(
                ksmWithRanges(new int[][]{{0x0000, 0x3FFF}, {0x4000, 0x7FFF}, {0x8000, 0xBFFF}, {0xC000, 0xFFFF}}));
        assertEquals(ranges, List.of(
                Range.of(0x0000, 0x3FFF),
                Range.of(0x4000, 0x7FFF),
                Range.of(0x8000, 0xBFFF),
                Range.of(0xC000, 0xFFFF)));

        // A single bucket spanning the whole ring is valid.
        assertEquals(PersistentEntryBucketDispatcherMultipleConsumers.validateBucketBoundaries(
                ksmWithRanges(new int[][]{{0x0000, 0xFFFF}})), List.of(Range.of(0x0000, 0xFFFF)));
    }

    @Test
    public void testInvalidBoundariesAreRejected() {
        // No boundaries at all.
        assertThrows(IllegalArgumentException.class, () ->
                PersistentEntryBucketDispatcherMultipleConsumers.validateBucketBoundaries(
                        ksmWithRanges(new int[][]{})));
        // Not starting at 0.
        assertThrows(IllegalArgumentException.class, () ->
                PersistentEntryBucketDispatcherMultipleConsumers.validateBucketBoundaries(
                        ksmWithRanges(new int[][]{{0x0001, 0xFFFF}})));
        // Gap between buckets.
        assertThrows(IllegalArgumentException.class, () ->
                PersistentEntryBucketDispatcherMultipleConsumers.validateBucketBoundaries(
                        ksmWithRanges(new int[][]{{0x0000, 0x3FFF}, {0x5000, 0xFFFF}})));
        // Overlapping buckets.
        assertThrows(IllegalArgumentException.class, () ->
                PersistentEntryBucketDispatcherMultipleConsumers.validateBucketBoundaries(
                        ksmWithRanges(new int[][]{{0x0000, 0x7FFF}, {0x4000, 0xFFFF}})));
        // End before start.
        assertThrows(IllegalArgumentException.class, () ->
                PersistentEntryBucketDispatcherMultipleConsumers.validateBucketBoundaries(
                        ksmWithRanges(new int[][]{{0x0000, 0x3FFF}, {0x4000, 0x2000}})));
        // Not tiling the whole 16-bit ring.
        assertThrows(IllegalArgumentException.class, () ->
                PersistentEntryBucketDispatcherMultipleConsumers.validateBucketBoundaries(
                        ksmWithRanges(new int[][]{{0x0000, 0x3FFF}, {0x4000, 0x7FFF}})));
        // Bucket 0 too narrow to hold the canonical hash 1.
        assertThrows(IllegalArgumentException.class, () ->
                PersistentEntryBucketDispatcherMultipleConsumers.validateBucketBoundaries(
                        ksmWithRanges(new int[][]{{0x0000, 0x0000}, {0x0001, 0xFFFF}})));
    }
}
