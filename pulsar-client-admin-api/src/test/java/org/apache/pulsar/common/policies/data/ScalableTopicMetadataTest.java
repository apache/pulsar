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
package org.apache.pulsar.common.policies.data;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertTrue;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

public class ScalableTopicMetadataTest {

    // --- outer class: ScalableTopicMetadata ---

    @Test
    public void testNoArgsConstructorDefaults() {
        ScalableTopicMetadata md = new ScalableTopicMetadata();
        assertEquals(md.getEpoch(), 0L);
        assertEquals(md.getNextSegmentId(), 0L);
        assertNotNull(md.getSegments());
        assertTrue(md.getSegments().isEmpty());
        // Properties default is an immutable empty map.
        assertNotNull(md.getProperties());
        assertTrue(md.getProperties().isEmpty());
    }

    @Test
    public void testBuilderDefaultsAreFreshSegmentsMapPerInstance() {
        ScalableTopicMetadata a = ScalableTopicMetadata.builder().build();
        ScalableTopicMetadata b = ScalableTopicMetadata.builder().build();
        assertNotSame(a.getSegments(), b.getSegments(),
                "@Builder.Default must create a fresh segments map per instance");
    }

    @Test
    public void testBuilderPopulatesFields() {
        ScalableTopicMetadata.SegmentInfo seg0 = activeSegment(0L, 0x0000, 0x7FFF, 0L);
        ScalableTopicMetadata.SegmentInfo seg1 = activeSegment(1L, 0x8000, 0xFFFF, 0L);

        ScalableTopicMetadata md = ScalableTopicMetadata.builder()
                .epoch(5L)
                .nextSegmentId(2L)
                .segments(mapOf(seg0, seg1))
                .properties(Map.of("k", "v"))
                .build();

        assertEquals(md.getEpoch(), 5L);
        assertEquals(md.getNextSegmentId(), 2L);
        assertEquals(md.getSegments().size(), 2);
        assertEquals(md.getSegments().get(0L), seg0);
        assertEquals(md.getSegments().get(1L), seg1);
        assertEquals(md.getProperties().get("k"), "v");
    }

    @Test
    public void testAllArgsConstructor() {
        Map<Long, ScalableTopicMetadata.SegmentInfo> segments = new LinkedHashMap<>();
        segments.put(0L, activeSegment(0L, 0, 0xFFFF, 0L));

        Map<String, String> props = Map.of("retention", "7d");

        ScalableTopicMetadata md = new ScalableTopicMetadata(3L, 1L, segments, props);

        assertEquals(md.getEpoch(), 3L);
        assertEquals(md.getNextSegmentId(), 1L);
        assertEquals(md.getSegments(), segments);
        assertEquals(md.getProperties(), props);
    }

    @Test
    public void testEqualsAndHashCode() {
        ScalableTopicMetadata a = ScalableTopicMetadata.builder().epoch(1L).nextSegmentId(1L).build();
        ScalableTopicMetadata b = ScalableTopicMetadata.builder().epoch(1L).nextSegmentId(1L).build();
        ScalableTopicMetadata c = ScalableTopicMetadata.builder().epoch(2L).nextSegmentId(1L).build();

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertFalse(a.equals(c));
    }

    // --- nested SegmentInfo ---

    @Test
    public void testSegmentInfoActiveHelpers() {
        ScalableTopicMetadata.SegmentInfo seg = activeSegment(0L, 0x0000, 0x7FFF, 0L);
        assertTrue(seg.isActive());
        assertFalse(seg.isSealed());
    }

    @Test
    public void testSegmentInfoSealedHelpers() {
        ScalableTopicMetadata.SegmentInfo seg = sealedSegment(0L, 0x0000, 0x7FFF,
                List.of(), List.of(2L, 3L), 0L, 5L);
        assertTrue(seg.isSealed());
        assertFalse(seg.isActive());
    }

    @Test
    public void testSegmentInfoHelpersForUnknownStateAreFalse() {
        ScalableTopicMetadata.SegmentInfo seg = new ScalableTopicMetadata.SegmentInfo(
                0L, hashRange(0, 0xFFFF), "UNKNOWN", List.of(), List.of(), 0L, -1L);
        assertFalse(seg.isActive());
        assertFalse(seg.isSealed());
    }

    @Test
    public void testSegmentInfoGetters() {
        ScalableTopicMetadata.SegmentInfo seg = sealedSegment(
                7L, 0x0000, 0x3FFF, List.of(0L), List.of(8L, 9L), 2L, 3L);
        assertEquals(seg.getSegmentId(), 7L);
        assertEquals(seg.getHashRange().getStart(), 0x0000);
        assertEquals(seg.getHashRange().getEnd(), 0x3FFF);
        assertEquals(seg.getState(), "SEALED");
        assertEquals(seg.getParentIds(), List.of(0L));
        assertEquals(seg.getChildIds(), List.of(8L, 9L));
        assertEquals(seg.getCreatedAtEpoch(), 2L);
        assertEquals(seg.getSealedAtEpoch(), 3L);
    }

    // --- nested HashRange ---

    @Test
    public void testHashRangeGettersAndEquality() {
        ScalableTopicMetadata.HashRange a = hashRange(0, 0x7FFF);
        ScalableTopicMetadata.HashRange b = hashRange(0, 0x7FFF);
        ScalableTopicMetadata.HashRange c = hashRange(0, 0x8000);

        assertEquals(a.getStart(), 0);
        assertEquals(a.getEnd(), 0x7FFF);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertFalse(a.equals(c));
    }

    @Test
    public void testHashRangeToStringIsLowercaseFourDigitHex() {
        assertEquals(hashRange(0, 0xFFFF).toString(), "[0000, ffff]");
        assertEquals(hashRange(0x0, 0x7FFF).toString(), "[0000, 7fff]");
        assertEquals(hashRange(0x8000, 0xFFFF).toString(), "[8000, ffff]");
        assertEquals(hashRange(0x1234, 0xabcd).toString(), "[1234, abcd]");
    }

    // --- helpers ---

    private static ScalableTopicMetadata.SegmentInfo activeSegment(long id, int start, int end,
                                                                    long createdAtEpoch) {
        return new ScalableTopicMetadata.SegmentInfo(
                id, hashRange(start, end), "ACTIVE",
                List.of(), List.of(), createdAtEpoch, -1L);
    }

    private static ScalableTopicMetadata.SegmentInfo sealedSegment(long id, int start, int end,
                                                                    List<Long> parents,
                                                                    List<Long> children,
                                                                    long createdAtEpoch,
                                                                    long sealedAtEpoch) {
        return new ScalableTopicMetadata.SegmentInfo(
                id, hashRange(start, end), "SEALED",
                parents, children, createdAtEpoch, sealedAtEpoch);
    }

    private static ScalableTopicMetadata.HashRange hashRange(int start, int end) {
        return new ScalableTopicMetadata.HashRange(start, end);
    }

    private static Map<Long, ScalableTopicMetadata.SegmentInfo> mapOf(
            ScalableTopicMetadata.SegmentInfo... segs) {
        Map<Long, ScalableTopicMetadata.SegmentInfo> m = new LinkedHashMap<>();
        for (ScalableTopicMetadata.SegmentInfo s : segs) {
            m.put(s.getSegmentId(), s);
        }
        return m;
    }
}
