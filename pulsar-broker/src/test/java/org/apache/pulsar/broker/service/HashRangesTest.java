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

import static org.assertj.core.api.Assertions.assertThat;
import java.util.NavigableSet;
import java.util.TreeSet;
import org.apache.pulsar.client.api.Range;
import org.testng.annotations.Test;

@Test(groups = "broker-impl")
public class HashRangesTest {

    @Test
    public void testMergeOverlappingRanges() {
        NavigableSet<Range> ranges = new TreeSet<>();
        ranges.add(Range.of(1, 5));
        ranges.add(Range.of(6, 10));
        ranges.add(Range.of(8, 12));
        ranges.add(Range.of(15, 20));
        ranges.add(Range.of(21, 25));

        NavigableSet<Range> expectedMergedRanges = new TreeSet<>();
        expectedMergedRanges.add(Range.of(1, 12));
        expectedMergedRanges.add(Range.of(15, 25));

        NavigableSet<Range> mergedRanges = HashRanges.mergeOverlappingRanges(ranges);

        assertThat(mergedRanges).containsExactlyElementsOf(expectedMergedRanges);
    }

}