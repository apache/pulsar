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
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class InMemoryRedeliveryTrackerTest {

    @DataProvider(name = "firstKey")
    public Object[][] firstKey() {
        // Cover both fastutil's special zero-key slot and its regular hash lookup path.
        return new Object[][] {{0L}, {1L}};
    }

    @Test(dataProvider = "firstKey", timeOut = 30000)
    public void testReadDuringInnerMapExpansion(long firstKey) throws Exception {
        assertReadDuringExpansion(false, firstKey);
    }

    @Test(dataProvider = "firstKey", timeOut = 30000)
    public void testReadDuringOuterMapExpansion(long firstKey) throws Exception {
        assertReadDuringExpansion(true, firstKey);
    }

    private static void assertReadDuringExpansion(boolean expandOuterMap, long firstKey) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2, new DefaultThreadFactory("redelivery-tracker-test"));
        try {
            for (int round = 0; round < 500; round++) {
                InMemoryRedeliveryTracker tracker = new InMemoryRedeliveryTracker();
                // Fill the default table to its first resize threshold using only normal tracker operations.
                for (int key = 0; key < 24; key++) {
                    tracker.incrementAndGetRedeliveryCount(position(expandOuterMap, firstKey + key));
                }
                CyclicBarrier start = new CyclicBarrier(2);
                AtomicBoolean writerDone = new AtomicBoolean();
                Future<?> writer = executor.submit(() -> {
                    try {
                        start.await(10, TimeUnit.SECONDS);
                        for (int key = 24; key < 128; key++) {
                            tracker.incrementAndGetRedeliveryCount(position(expandOuterMap, firstKey + key));
                        }
                    } finally {
                        writerDone.set(true);
                    }
                    return null;
                });
                Future<?> reader = executor.submit(() -> {
                    start.await(10, TimeUnit.SECONDS);
                    Position existing = position(expandOuterMap, firstKey);
                    int reads = 0;
                    // Keep reading a position the writer never changes until all insertions are done.
                    do {
                        assertThat(tracker.getRedeliveryCount(existing.getLedgerId(), existing.getEntryId()))
                                .isEqualTo(1);
                    } while (++reads < 256 || !writerDone.get());
                    return null;
                });
                writer.get(10, TimeUnit.SECONDS);
                reader.get(10, TimeUnit.SECONDS);
                for (int key = 0; key < 128; key++) {
                    Position existing = position(expandOuterMap, firstKey + key);
                    assertThat(tracker.getRedeliveryCount(existing.getLedgerId(), existing.getEntryId()))
                            .isEqualTo(1);
                }
            }
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static Position position(boolean expandOuterMap, long key) {
        return expandOuterMap ? PositionFactory.create(key, 0) : PositionFactory.create(7, key);
    }
}
