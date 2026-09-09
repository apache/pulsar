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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.bookkeeper.mledger.impl.MockManagedCursor.addCursor;
import static org.apache.bookkeeper.mledger.impl.MockManagedCursor.createCursor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ActiveManagedCursorContainerRetentionTest {
    private static final Position POSITION = PositionFactory.create(1, 1);

    @DataProvider
    public Object[][] tracked() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "tracked")
    public void testRemovedCursorReleasedBeforeCleanup(boolean tracked) {
        ActiveManagedCursorContainerImpl container = new ActiveManagedCursorContainerImpl();
        ManagedCursor survivor = createCursor(container, "survivor", POSITION);
        container.add(survivor, POSITION);
        addCursor(container, "removed", POSITION);
        if (tracked) {
            container.getSlowestCursorPosition();
        }
        container.removeCursor("removed");

        // Inspect retention without triggering a position flush or relying on GC timing.
        assertThat(container.getRetainedCursors()).containsExactlyInAnyOrder(survivor, null);
        assertThat(container.getNumberOfCursorsAtSamePositionOrBefore(survivor)).isEqualTo(1);
        container.checkOrderingAndNumberOfCursorsState();
    }

    @Test(dataProvider = "tracked")
    public void testLastRemovalClearsRetainedNodes(boolean tracked) {
        ActiveManagedCursorContainerImpl container = new ActiveManagedCursorContainerImpl();
        for (int i = 0; i < 100; i++) {
            addCursor(container, "cursor", POSITION);
            if (tracked) {
                container.getSlowestCursorPosition();
            }
            assertThat(container.removeCursor("cursor")).isTrue();
            assertThat(container.getRetainedCursors()).isEmpty();
            assertThat(container.size()).isZero();
        }
    }

    @Test
    public void testPendingNodesBoundedWithoutPositionQueries() {
        ActiveManagedCursorContainerImpl container = new ActiveManagedCursorContainerImpl();
        ManagedCursor survivor = createCursor(container, "survivor", POSITION);
        container.add(survivor, POSITION);
        for (int i = 0; i < 1000; i++) {
            addCursor(container, "cursor" + i, POSITION);
            container.removeCursor("cursor" + i);
            List<ManagedCursor> retained = container.getRetainedCursors();
            assertThat(retained).hasSizeLessThanOrEqualTo(65);
            assertThat(retained.stream().filter(cursor -> cursor != null).toList()).containsExactly(survivor);
        }
        assertThat(container.getNumberOfCursorsAtSamePositionOrBefore(survivor)).isEqualTo(1);
        container.checkOrderingAndNumberOfCursorsState();
    }

    @Test
    public void testTrackedRemovalsTriggerCleanupWithoutPendingUpdates() {
        ActiveManagedCursorContainerImpl container = new ActiveManagedCursorContainerImpl();
        for (int i = 0; i < 1000; i++) {
            addCursor(container, "cursor" + i, PositionFactory.create(1, i));
        }
        container.getSlowestCursorPosition();
        for (int i = 0; i < 900; i++) {
            container.removeCursor("cursor" + i);
            assertThat(container.getRetainedCursors()).hasSizeLessThanOrEqualTo(
                    container.size() + Math.max(64, container.size()));
            if (i == 63) {
                // A large live set should not be rebuilt every fixed-size batch of removals.
                assertThat(container.getRetainedCursors()).hasSize(1000);
            }
        }
        assertThat(container.getSlowestCursorPosition()).isEqualTo(PositionFactory.create(1, 900));
        assertThat(container.getNumberOfCursorsAtSamePositionOrBefore(container.get("cursor999"))).isEqualTo(100);
        container.checkOrderingAndNumberOfCursorsState();
    }

    @Test
    public void testCompactionPreservesPendingUpdatesAndSharedCounters() {
        ActiveManagedCursorContainerImpl container = new ActiveManagedCursorContainerImpl();
        for (int i = 0; i < 200; i++) {
            addCursor(container, "cursor" + i, PositionFactory.create(1, i / 10));
        }
        container.getSlowestCursorPosition();
        ManagedCursor moved = container.get("cursor199");
        container.updateCursor(moved, PositionFactory.create(1, 0));
        for (int i = 0; i < 200; i += 2) {
            container.removeCursor("cursor" + i);
        }
        assertThat(container.getRetainedCursors()).hasSize(100).doesNotContainNull();
        assertThat(container.getPendingPositionUpdatesCount()).isEqualTo(1);
        assertThat(container.getNumberOfCursorsAtSamePositionOrBefore(moved)).isEqualTo(6);
        for (int i = 1; i < 199; i += 2) {
            assertThat(container.getNumberOfCursorsAtSamePositionOrBefore(container.get("cursor" + i)))
                    .as("rank of cursor%s after compaction and the pending move", i)
                    .isEqualTo(Math.min(100, (i / 10 + 1) * 5 + 1));
        }
        container.checkOrderingAndNumberOfCursorsState();
    }

    @Test
    public void testPositionQueryResetsCleanupBatch() {
        ActiveManagedCursorContainerImpl container = new ActiveManagedCursorContainerImpl();
        addCursor(container, "survivor", POSITION);
        for (int i = 0; i < 63; i++) {
            addCursor(container, "cursor" + i, POSITION);
            container.removeCursor("cursor" + i);
        }
        container.getSlowestCursorPosition();
        addCursor(container, "removed", POSITION);
        container.removeCursor("removed");
        // The query already reclaimed the old batch; this removal should remain deferred.
        assertThat(container.getRetainedCursors()).hasSize(2).containsNull();
    }

    @Test
    public void testReusedNodeUsesCurrentCursorInstance() {
        ActiveManagedCursorContainerImpl container = new ActiveManagedCursorContainerImpl();
        addCursor(container, "survivor", POSITION);
        addCursor(container, "recreated", POSITION);
        container.getSlowestCursorPosition();
        container.removeCursor("recreated");
        ManagedCursor replacement = createCursor(container, "recreated", POSITION);
        container.add(replacement, POSITION);

        assertThat(container.get("recreated")).isSameAs(replacement);
        assertThat(container.getRetainedCursors()).doesNotContainNull().contains(replacement);
        assertThat(container.getNumberOfCursorsAtSamePositionOrBefore(replacement)).isEqualTo(2);
        container.checkOrderingAndNumberOfCursorsState();
    }

    @Test
    public void testReactivationDoesNotDuplicatePendingUpdates() {
        ActiveManagedCursorContainerImpl container = new ActiveManagedCursorContainerImpl();
        addCursor(container, "first", POSITION);
        addCursor(container, "second", POSITION);
        container.getSlowestCursorPosition();
        for (int i = 0; i < 1000; i++) {
            container.removeCursor("first");
            addCursor(container, "first", POSITION);
            container.removeCursor("second");
            addCursor(container, "second", POSITION);
            if (i < 31) {
                assertThat(container.getPendingPositionUpdatesCount()).isEqualTo(2);
            } else {
                // Compaction can discard the removed node before reactivation, leaving new orphan
                // nodes until the next batch. Reused active nodes must not accumulate heap entries.
                assertThat(container.getPendingPositionUpdatesCount()).isLessThanOrEqualTo(66);
            }
        }
        container.checkOrderingAndNumberOfCursorsState();
        assertThat(container.getNumberOfCursorsAtSamePositionOrBefore(container.get("first"))).isEqualTo(2);
        assertThat(container.getNumberOfCursorsAtSamePositionOrBefore(container.get("second"))).isEqualTo(2);
    }

    @Test
    public void testSlowestPositionReadDoesNotExcludeOtherReaders() throws Exception {
        ActiveManagedCursorContainerImpl container = new ActiveManagedCursorContainerImpl();
        ManagedCursor cursor = mock(ManagedCursor.class);
        when(cursor.getName()).thenReturn("cursor");
        container.add(cursor, POSITION);
        assertThat(container.getSlowestCursorPosition()).isEqualTo(POSITION);

        CountDownLatch readLockHeld = new CountDownLatch(1);
        CountDownLatch releaseReadLock = new CountDownLatch(1);
        when(cursor.toString()).thenAnswer(invocation -> {
            // toString calls the cursor while holding the container's read lock.
            readLockHeld.countDown();
            assertThat(releaseReadLock.await(30, SECONDS)).isTrue();
            return "cursor";
        });
        ExecutorService executor = Executors.newFixedThreadPool(2,
                new DefaultThreadFactory("cursor-container-read-test"));
        try {
            Future<String> reader = executor.submit(container::toString);
            assertThat(readLockHeld.await(10, SECONDS)).isTrue();
            Future<Position> slowest = executor.submit(container::getSlowestCursorPosition);
            assertThat(slowest.get(10, SECONDS)).isEqualTo(POSITION);
            releaseReadLock.countDown();
            assertThat(reader.get(10, SECONDS)).isEqualTo("[cursor]");
        } finally {
            releaseReadLock.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    @Test
    public void testSlowestPositionFlushesPendingChanges() {
        ActiveManagedCursorContainerImpl container = new ActiveManagedCursorContainerImpl();
        assertThat(container.getSlowestCursorPosition()).isNull();
        Position later = PositionFactory.create(1, 2);
        Position latest = PositionFactory.create(1, 3);
        addCursor(container, "later", later);
        assertThat(container.getSlowestCursorPosition()).isEqualTo(later);
        assertThat(container.getSlowestCursorPosition()).isEqualTo(later);
        addCursor(container, "earlier", POSITION);
        assertThat(container.getSlowestCursorPosition()).isEqualTo(POSITION);
        container.updateCursor(container.get("earlier"), latest);
        assertThat(container.getSlowestCursorPosition()).isEqualTo(later);
        container.removeCursor("later");
        assertThat(container.getSlowestCursorPosition()).isEqualTo(latest);
        assertThat(container.getSlowestCursorPosition()).isEqualTo(latest);
        container.removeCursor("earlier");
        assertThat(container.getSlowestCursorPosition()).isNull();
    }

    @Test
    public void testIteratorSkipsRemovedNodes() {
        ActiveManagedCursorContainerImpl container = new ActiveManagedCursorContainerImpl();
        addCursor(container, "first", POSITION);
        addCursor(container, "second", POSITION);
        Iterator<ManagedCursor> iterator = container.iterator();
        container.removeCursor("first");
        container.removeCursor("second");
        assertThat(iterator.hasNext()).isFalse();
        assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);

        addCursor(container, "third", POSITION);
        Iterator<ManagedCursor> prefetched = container.iterator();
        assertThat(prefetched.hasNext()).isTrue();
        assertThat(prefetched.hasNext()).isTrue();
        container.removeCursor("third");
        assertThat(prefetched.next().getName()).isEqualTo("third");
        assertThat(prefetched.hasNext()).isFalse();
    }
}
