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
package org.apache.bookkeeper.mledger.impl;

import static org.apache.bookkeeper.mledger.util.ManagedLedgerTestUtil.defaultConfig;
import static org.assertj.core.api.Assertions.assertThat;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Bounded progress coverage for ordinary cursor lifecycle, position and container read APIs.
 * A passing run does not prove absence of deadlocks or performance regressions.
 */
public class ManagedCursorLifecycleProgressTest extends MockedBookKeeperTestCase {
    private static final int ROUNDS = 80;

    @DataProvider(name = "cacheEvictionModes")
    public Object[][] cacheEvictionModes() {
        return new Object[][]{{false}, {true}};
    }

    @Test(dataProvider = "cacheEvictionModes", timeOut = 30000)
    public void testLifecycleCompletesAlongsidePositionUpdatesAndContainerReads(boolean expectedReadCount)
            throws Exception {
        ManagedLedgerConfig config = defaultConfig();
        config.setCacheEvictionByExpectedReadCount(expectedReadCount);
        config.setCacheEvictionByMarkDeletedPosition(false);
        config.setReadAheadEnabled(false);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("cursor-lifecycle-progress", config);
        List<ManagedCursorImpl> cursors = new ArrayList<>();
        for (int index = 0; index < 3; index++) {
            cursors.add((ManagedCursorImpl) ledger.openCursor("cursor-" + index));
        }
        List<Position> positions = new ArrayList<>();
        for (int index = 0; index < 4; index++) {
            positions.add(ledger.addEntry(new byte[]{(byte) index}));
        }
        ActiveManagedCursorContainer activeCursors = ledger.getActiveCursors();
        CountDownLatch ready = new CountDownLatch(5);
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService callers = Executors.newFixedThreadPool(5,
                new DefaultThreadFactory("cursor-lifecycle-progress", true));
        List<Future<Void>> futures = new ArrayList<>();
        try {
            futures.add(callers.submit(startTogether(ready, start, () -> {
                for (int round = 0; round < ROUNDS; round++) {
                    for (ManagedCursorImpl cursor : cursors) {
                        cursor.setActive();
                        cursor.setInactive();
                    }
                }
            })));
            futures.add(callers.submit(startTogether(ready, start, () -> {
                for (int round = 0; round < ROUNDS; round++) {
                    for (int index = cursors.size() - 1; index >= 0; index--) {
                        cursors.get(index).setInactive();
                        cursors.get(index).setActive();
                    }
                }
            })));
            futures.add(callers.submit(startTogether(ready, start, () -> {
                for (int round = 0; round < ROUNDS; round++) {
                    cursors.get(2).setAlwaysInactive();
                    cursors.get(2).setActive();
                }
            })));
            futures.add(callers.submit(startTogether(ready, start, () -> {
                for (int round = 0; round < ROUNDS; round++) {
                    for (int index = 0; index < cursors.size(); index++) {
                        cursors.get(index).seek(positions.get((round + index) % positions.size()));
                    }
                }
            })));
            futures.add(callers.submit(startTogether(ready, start, () -> {
                for (int round = 0; round < ROUNDS * 2; round++) {
                    activeCursors.size();
                    activeCursors.isEmpty();
                    activeCursors.getSlowestCursorPosition();
                    assertThat(activeCursors.toString()).startsWith("[").endsWith("]");
                    for (ManagedCursor cursor : activeCursors) {
                        assertThat(cursor).isNotNull();
                        assertThat(cursor.getName()).startsWith("cursor-");
                    }
                    for (ManagedCursorImpl cursor : cursors) {
                        assertThat(cursor.toString()).contains(cursor.getName());
                        if (expectedReadCount) {
                            activeCursors.getNumberOfCursorsAtSamePositionOrBefore(cursor);
                            activeCursors.getNumberOfCursorsAtPositionOrBefore(cursor, positions.get(3));
                        }
                    }
                }
            })));
            assertThat(ready.await(3, TimeUnit.SECONDS)).as("all ordinary API callers are ready").isTrue();
            start.countDown();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            for (Future<Void> future : futures) {
                future.get(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
            }
        } finally {
            start.countDown();
            futures.forEach(future -> future.cancel(true));
            callers.shutdownNow();
            assertThat(callers.awaitTermination(5, TimeUnit.SECONDS)).as("all lifecycle callers stopped").isTrue();
        }

        // Check the concurrent histories before applying a deterministic final lifecycle state.
        for (ManagedCursorImpl cursor : cursors) {
            assertThat(cursor.isActive()).as("activity flag and membership agree for %s", cursor.getName())
                    .isEqualTo(ledger.isCursorActive(cursor));
        }
        cursors.get(2).setActive();
        assertThat(cursors.get(2).isActive()).as("permanently inactive cursor cannot be reactivated").isFalse();
        assertThat(ledger.isCursorActive(cursors.get(2))).isFalse();
        for (int index = 0; index < 2; index++) {
            ManagedCursorImpl cursor = cursors.get(index);
            cursor.setInactive();
            cursor.seek(positions.get(index + 1));
            cursor.setActive();
            assertThat(cursor.isActive()).isTrue();
            assertThat(ledger.isCursorActive(cursor)).isTrue();
        }
        assertThat(activeCursors.size()).isEqualTo(2);
        assertThat(activeCursors.getSlowestCursorPosition()).isEqualTo(positions.get(1));
        assertThat(activeCursors).containsExactlyInAnyOrder(cursors.get(0), cursors.get(1));
        if (expectedReadCount) {
            assertThat(activeCursors.getNumberOfCursorsAtSamePositionOrBefore(cursors.get(0))).isEqualTo(1);
            assertThat(activeCursors.getNumberOfCursorsAtSamePositionOrBefore(cursors.get(1))).isEqualTo(2);
            assertThat(activeCursors.getNumberOfCursorsAtSamePositionOrBefore(cursors.get(2))).isZero();
        }
        ledger.close();
    }

    private static Callable<Void> startTogether(CountDownLatch ready, CountDownLatch start, Runnable operation) {
        return () -> {
            ready.countDown();
            if (!start.await(3, TimeUnit.SECONDS)) {
                throw new AssertionError("ordinary API callers were not started");
            }
            operation.run();
            return null;
        };
    }
}
