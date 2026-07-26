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
package org.apache.pulsar.client.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import java.util.List;
import org.apache.pulsar.client.api.BatchMessageContainer;
import org.testng.annotations.Test;

public class EntryBucketBatchContainerTest {

    @Test
    public void testBucketOfEmptySplitsIsAlwaysZero() {
        int[] none = new int[0];
        for (int h : new int[]{0x0000, 0x4000, 0x8000, 0xFFFF}) {
            assertEquals(EntryBucketBatchContainer.bucketOf(none, h), 0);
        }
    }

    @Test
    public void testBucketOfEvenSplits() {
        int[] splits = {0x4000, 0x8000, 0xC000}; // 4 equal buckets
        assertEquals(EntryBucketBatchContainer.bucketOf(splits, 0x0000), 0);
        assertEquals(EntryBucketBatchContainer.bucketOf(splits, 0x3FFF), 0);
        assertEquals(EntryBucketBatchContainer.bucketOf(splits, 0x4000), 1);
        assertEquals(EntryBucketBatchContainer.bucketOf(splits, 0x7FFF), 1);
        assertEquals(EntryBucketBatchContainer.bucketOf(splits, 0x8000), 2);
        assertEquals(EntryBucketBatchContainer.bucketOf(splits, 0xBFFF), 2);
        assertEquals(EntryBucketBatchContainer.bucketOf(splits, 0xC000), 3);
        assertEquals(EntryBucketBatchContainer.bucketOf(splits, 0xFFFF), 3);
    }

    @Test
    public void testBucketOfUnevenSplits() {
        // 3 uneven buckets: [0x0000,0x0FFF], [0x1000,0xEFFF], [0xF000,0xFFFF]
        int[] splits = {0x1000, 0xF000};
        assertEquals(EntryBucketBatchContainer.bucketOf(splits, 0x0000), 0);
        assertEquals(EntryBucketBatchContainer.bucketOf(splits, 0x0FFF), 0);
        assertEquals(EntryBucketBatchContainer.bucketOf(splits, 0x1000), 1);
        assertEquals(EntryBucketBatchContainer.bucketOf(splits, 0xEFFF), 1);
        assertEquals(EntryBucketBatchContainer.bucketOf(splits, 0xF000), 2);
        assertEquals(EntryBucketBatchContainer.bucketOf(splits, 0xFFFF), 2);
    }

    @Test
    public void testBuilderBuildsMultiBatchContainer() {
        BatchMessageContainer c = new EntryBucketBatcherBuilder(List.of(0x8000)).build();
        assertTrue(c instanceof EntryBucketBatchContainer);
        assertTrue(((EntryBucketBatchContainer) c).isMultiBatches());
    }
}
