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

import static org.apache.bookkeeper.mledger.impl.MockManagedCursor.createCursor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import com.google.common.collect.Lists;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.testng.annotations.Test;

public class ManagedCursorContainerTest {
    @Test
    public void testSlowestReadPositionForActiveCursors() {
        ManagedCursorContainerImpl container = new ManagedCursorContainerImpl();
        assertNull(container.getSlowestCursorPosition());

        // Add no durable cursor
        Position position = PositionFactory.create(5, 5);
        ManagedCursor cursor1 = spy(new MockManagedCursor(container, "test1", position));
        doReturn(false).when(cursor1).isDurable();
        doReturn(position).when(cursor1).getReadPosition();
        container.add(cursor1, position);
        assertEquals(container.getSlowestCursorPosition(), PositionFactory.create(5, 5));

        // Add no durable cursor
        position = PositionFactory.create(1, 1);
        ManagedCursor cursor2 = spy(new MockManagedCursor(container, "test2", position));
        doReturn(false).when(cursor2).isDurable();
        doReturn(position).when(cursor2).getReadPosition();
        container.add(cursor2, position);
        assertEquals(container.getSlowestCursorPosition(), PositionFactory.create(1, 1));

        // Move forward cursor, cursor1 = 5:5, cursor2 = 5:6, slowest is 5:5
        position = PositionFactory.create(5, 6);
        container.cursorUpdated(cursor2, position);
        doReturn(position).when(cursor2).getReadPosition();
        assertEquals(container.getSlowestCursorPosition(), PositionFactory.create(5, 5));

        // Move forward cursor, cursor1 = 5:8, cursor2 = 5:6, slowest is 5:6
        position = PositionFactory.create(5, 8);
        doReturn(position).when(cursor1).getReadPosition();
        container.cursorUpdated(cursor1, position);
        assertEquals(container.getSlowestCursorPosition(), PositionFactory.create(5, 6));

        // Remove cursor, only cursor1 left, cursor1 = 5:8
        container.removeCursor(cursor2.getName());
        assertEquals(container.getSlowestCursorPosition(), PositionFactory.create(5, 8));
    }

    @Test
    public void simple() throws Exception {
        ManagedCursorContainerImpl container = new ManagedCursorContainerImpl();
        assertNull(container.getSlowestCursorPosition());

        ManagedCursor cursor1 = new MockManagedCursor(container, "test1", PositionFactory.create(5, 5));
        container.add(cursor1, cursor1.getMarkDeletedPosition());
        assertEquals(container.getSlowestCursorPosition(), PositionFactory.create(5, 5));
        assertEqualsCursorAndPosition(container.getCursorWithOldestPosition(),
                cursor1, PositionFactory.create(5, 5));

        ManagedCursor cursor2 = new MockManagedCursor(container, "test2", PositionFactory.create(2, 2));
        container.add(cursor2, cursor2.getMarkDeletedPosition());
        assertEquals(container.getSlowestCursorPosition(), PositionFactory.create(2, 2));
        assertEqualsCursorAndPosition(container.getCursorWithOldestPosition(),
                cursor2, PositionFactory.create(2, 2));

        ManagedCursor cursor3 = new MockManagedCursor(container, "test3", PositionFactory.create(2, 0));
        container.add(cursor3, cursor3.getMarkDeletedPosition());
        assertEquals(container.getSlowestCursorPosition(), PositionFactory.create(2, 0));
        assertEqualsCursorAndPosition(container.getCursorWithOldestPosition(),
                cursor3, PositionFactory.create(2, 0));

        assertEquals(container.toString(), "[test1=5:5/null, test2=2:2/null, test3=2:0/null]");

        ManagedCursor cursor4 = new MockManagedCursor(container, "test4", PositionFactory.create(4, 0));
        container.add(cursor4, cursor4.getMarkDeletedPosition());
        assertEquals(container.getSlowestCursorPosition(), PositionFactory.create(2, 0));

        ManagedCursor cursor5 = new MockManagedCursor(container, "test5", PositionFactory.create(3, 5));
        container.add(cursor5, cursor5.getMarkDeletedPosition());
        assertEquals(container.getSlowestCursorPosition(), PositionFactory.create(2, 0));

        cursor3.markDelete(PositionFactory.create(3, 0));
        assertEquals(container.getSlowestCursorPosition(), PositionFactory.create(2, 2));
        assertEqualsCursorAndPosition(container.getCursorWithOldestPosition(),
                cursor2, PositionFactory.create(2, 2));

        cursor2.markDelete(PositionFactory.create(10, 5));
        assertEquals(container.getSlowestCursorPosition(), PositionFactory.create(3, 0));

        container.removeCursor(cursor3.getName());
        assertEquals(container.getSlowestCursorPosition(), PositionFactory.create(3, 5));

        container.removeCursor(cursor2.getName());
        container.removeCursor(cursor5.getName());
        container.removeCursor(cursor1.getName());
        assertEquals(container.getSlowestCursorPosition(), PositionFactory.create(4, 0));
        assertEqualsCursorAndPosition(container.getCursorWithOldestPosition(),
                cursor4, PositionFactory.create(4, 0));

        assertTrue(container.hasDurableCursors());

        container.removeCursor(cursor4.getName());
        assertNull(container.getSlowestCursorPosition());

        assertFalse(container.hasDurableCursors());

        ManagedCursor cursor6 = new MockManagedCursor(container, "test6", PositionFactory.create(6, 5));
        container.add(cursor6, cursor6.getMarkDeletedPosition());
        assertEquals(container.getSlowestCursorPosition(), PositionFactory.create(6, 5));

        assertEquals(container.toString(), "[test6=6:5/null]");
    }

    @Test
    public void updatingCursorOutsideContainer() {
        ManagedCursorContainerImpl container = new ManagedCursorContainerImpl();

        ManagedCursor cursor1 = new MockManagedCursor(container, "test1", PositionFactory.create(5, 5));
        container.add(cursor1, cursor1.getMarkDeletedPosition());
        assertEquals(container.getSlowestCursorPosition(), PositionFactory.create(5, 5));

        MockManagedCursor cursor2 = new MockManagedCursor(container, "test2", PositionFactory.create(2, 2));
        container.add(cursor2, cursor2.getMarkDeletedPosition());
        assertEquals(container.getSlowestCursorPosition(), PositionFactory.create(2, 2));

        cursor2.markDeletePosition = PositionFactory.create(8, 8);

        // Until we don't update the container, the ordering will not change
        assertEquals(container.getSlowestCursorPosition(), PositionFactory.create(2, 2));

        container.cursorUpdated(cursor2, cursor2.markDeletePosition);

        assertEquals(container.getSlowestCursorPosition(), PositionFactory.create(5, 5));
        assertEqualsCursorAndPosition(container.getCursorWithOldestPosition(),
                cursor1, PositionFactory.create(5, 5));
    }

    private void assertEqualsCursorAndPosition(ManagedCursorContainer.CursorInfo cursorInfo,
                                               ManagedCursor expectedCursor,
                                               Position expectedPosition) {
        assertThat(cursorInfo.getCursor().getName()).isEqualTo(expectedCursor.getName());
        assertThat(cursorInfo.getPosition()).isEqualTo(expectedPosition);
    }

    @Test
    public void removingCursor() {
        ManagedCursorContainerImpl container = new ManagedCursorContainerImpl();

        ManagedCursor cursor1 = new MockManagedCursor(container, "test1", PositionFactory.create(5, 5));
        container.add(cursor1, cursor1.getMarkDeletedPosition());
        assertEquals(container.getSlowestCursorPosition(), PositionFactory.create(5, 5));
        assertEquals(container.get("test1"), cursor1);

        MockManagedCursor cursor2 = new MockManagedCursor(container, "test2", PositionFactory.create(2, 2));
        container.add(cursor2, cursor2.getMarkDeletedPosition());
        assertEquals(container.getSlowestCursorPosition(), PositionFactory.create(2, 2));
        assertEquals(container.get("test2"), cursor2);

        MockManagedCursor cursor3 = new MockManagedCursor(container, "test3", PositionFactory.create(1, 1));
        container.add(cursor3, cursor3.getMarkDeletedPosition());
        assertEquals(container.getSlowestCursorPosition(), PositionFactory.create(1, 1));
        assertEquals(container.get("test3"), cursor3);

        assertEquals(container, Lists.newArrayList(cursor1, cursor2, cursor3));

        // Remove the cursor in the middle
        container.removeCursor("test2");

        assertEquals(container, Lists.newArrayList(cursor1, cursor3));

        assertNull(container.get("test2"));

        assertEquals(container.getSlowestCursorPosition(), PositionFactory.create(1, 1));

        container.removeCursor("test3");

        assertEquals(container, Lists.newArrayList(cursor1));

        assertEquals(container.getSlowestCursorPosition(), PositionFactory.create(5, 5));
    }

    @Test
    public void ordering() throws Exception {
        ManagedCursorContainerImpl container = new ManagedCursorContainerImpl();

        ManagedCursor cursor1 = new MockManagedCursor(container, "test1", PositionFactory.create(5, 5));
        ManagedCursor cursor2 = new MockManagedCursor(container, "test2", PositionFactory.create(5, 1));
        ManagedCursor cursor3 = new MockManagedCursor(container, "test3", PositionFactory.create(7, 1));
        ManagedCursor cursor4 = new MockManagedCursor(container, "test4", PositionFactory.create(6, 4));
        ManagedCursor cursor5 = new MockManagedCursor(container, "test5", PositionFactory.create(7, 0));

        container.add(cursor1, cursor1.getMarkDeletedPosition());
        container.add(cursor2, cursor2.getMarkDeletedPosition());
        container.add(cursor3, cursor3.getMarkDeletedPosition());
        container.add(cursor4, cursor4.getMarkDeletedPosition());
        container.add(cursor5, cursor5.getMarkDeletedPosition());

        assertEquals(container.getSlowestCursorPosition(), PositionFactory.create(5, 1));
        container.removeCursor("test2");

        assertEquals(container.getSlowestCursorPosition(), PositionFactory.create(5, 5));
        container.removeCursor("test1");

        assertEquals(container.getSlowestCursorPosition(), PositionFactory.create(6, 4));
        container.removeCursor("test4");

        assertEquals(container.getSlowestCursorPosition(), PositionFactory.create(7, 0));
        container.removeCursor("test5");

        assertEquals(container.getSlowestCursorPosition(), PositionFactory.create(7, 1));
        container.removeCursor("test3");

        assertFalse(container.hasDurableCursors());
    }

    @Test
    public void orderingWithUpdates() {
        ManagedCursorContainerImpl container = new ManagedCursorContainerImpl();

        MockManagedCursor c1 = new MockManagedCursor(container, "test1", PositionFactory.create(5, 5));
        MockManagedCursor c2 = new MockManagedCursor(container, "test2", PositionFactory.create(5, 1));
        MockManagedCursor c3 = new MockManagedCursor(container, "test3", PositionFactory.create(7, 1));
        MockManagedCursor c4 = new MockManagedCursor(container, "test4", PositionFactory.create(6, 4));
        MockManagedCursor c5 = new MockManagedCursor(container, "test5", PositionFactory.create(7, 0));

        container.add(c1, c1.getMarkDeletedPosition());
        container.add(c2, c2.getMarkDeletedPosition());
        container.add(c3, c3.getMarkDeletedPosition());
        container.add(c4, c4.getMarkDeletedPosition());
        container.add(c5, c5.getMarkDeletedPosition());

        assertEquals(container.getSlowestCursorPosition(), PositionFactory.create(5, 1));

        c1.markDeletePosition = PositionFactory.create(5, 8);
        container.cursorUpdated(c1, c1.markDeletePosition);
        assertEquals(container.getSlowestCursorPosition(), PositionFactory.create(5, 1));

        c2.markDeletePosition = PositionFactory.create(5, 6);
        container.cursorUpdated(c2, c2.markDeletePosition);
        assertEquals(container.getSlowestCursorPosition(), PositionFactory.create(5, 6));

        c1.markDeletePosition = PositionFactory.create(6, 8);
        container.cursorUpdated(c1, c1.markDeletePosition);
        assertEquals(container.getSlowestCursorPosition(), PositionFactory.create(5, 6));

        c3.markDeletePosition = PositionFactory.create(8, 5);
        container.cursorUpdated(c3, c3.markDeletePosition);
        assertEquals(container.getSlowestCursorPosition(), PositionFactory.create(5, 6));

        c1.markDeletePosition = PositionFactory.create(8, 4);
        container.cursorUpdated(c1, c1.markDeletePosition);
        assertEquals(container.getSlowestCursorPosition(), PositionFactory.create(5, 6));

        c2.markDeletePosition = PositionFactory.create(8, 4);
        container.cursorUpdated(c2, c2.markDeletePosition);
        assertEquals(container.getSlowestCursorPosition(), PositionFactory.create(6, 4));

        c4.markDeletePosition = PositionFactory.create(7, 1);
        container.cursorUpdated(c4, c4.markDeletePosition);

        // ////

        assertEquals(container.getSlowestCursorPosition(), PositionFactory.create(7, 0));
        container.removeCursor("test5");

        assertEquals(container.getSlowestCursorPosition(), PositionFactory.create(7, 1));
        container.removeCursor("test4");

        assertEquals(container.getSlowestCursorPosition(), PositionFactory.create(8, 4));
        container.removeCursor("test1");

        assertEquals(container.getSlowestCursorPosition(), PositionFactory.create(8, 4));
        container.removeCursor("test2");

        assertEquals(container.getSlowestCursorPosition(), PositionFactory.create(8, 5));
        container.removeCursor("test3");

        assertFalse(container.hasDurableCursors());
    }

    @Test
    public void orderingWithUpdatesAndReset() {
        ManagedCursorContainerImpl container = new ManagedCursorContainerImpl();

        MockManagedCursor c1 = new MockManagedCursor(container, "test1", PositionFactory.create(5, 5));
        MockManagedCursor c2 = new MockManagedCursor(container, "test2", PositionFactory.create(5, 1));
        MockManagedCursor c3 = new MockManagedCursor(container, "test3", PositionFactory.create(7, 1));
        MockManagedCursor c4 = new MockManagedCursor(container, "test4", PositionFactory.create(6, 4));
        MockManagedCursor c5 = new MockManagedCursor(container, "test5", PositionFactory.create(7, 0));

        container.add(c1, c1.getMarkDeletedPosition());
        container.add(c2, c2.getMarkDeletedPosition());
        container.add(c3, c3.getMarkDeletedPosition());
        container.add(c4, c4.getMarkDeletedPosition());
        container.add(c5, c5.getMarkDeletedPosition());

        assertEquals(container.getSlowestCursorPosition(), PositionFactory.create(5, 1));

        c1.markDeletePosition = PositionFactory.create(5, 8);
        container.cursorUpdated(c1, c1.markDeletePosition);
        assertEquals(container.getSlowestCursorPosition(), PositionFactory.create(5, 1));

        c1.markDeletePosition = PositionFactory.create(5, 6);
        container.cursorUpdated(c1, c1.markDeletePosition);
        assertEquals(container.getSlowestCursorPosition(), PositionFactory.create(5, 1));

        c2.markDeletePosition = PositionFactory.create(6, 8);
        container.cursorUpdated(c2, c2.markDeletePosition);
        assertEquals(container.getSlowestCursorPosition(), PositionFactory.create(5, 6));

        c3.markDeletePosition = PositionFactory.create(8, 5);
        container.cursorUpdated(c3, c3.markDeletePosition);
        assertEquals(container.getSlowestCursorPosition(), PositionFactory.create(5, 6));

        c1.markDeletePosition = PositionFactory.create(8, 4);
        container.cursorUpdated(c1, c1.markDeletePosition);
        assertEquals(container.getSlowestCursorPosition(), PositionFactory.create(6, 4));

        c2.markDeletePosition = PositionFactory.create(4, 4);
        container.cursorUpdated(c2, c2.markDeletePosition);
        assertEquals(container.getSlowestCursorPosition(), PositionFactory.create(4, 4));

        c4.markDeletePosition = PositionFactory.create(7, 1);
        container.cursorUpdated(c4, c4.markDeletePosition);

        // ////

        assertEquals(container.getSlowestCursorPosition(), PositionFactory.create(4, 4));
        container.removeCursor("test2");

        assertEquals(container.getSlowestCursorPosition(), PositionFactory.create(7, 0));
        container.removeCursor("test5");

        assertEquals(container.getSlowestCursorPosition(), PositionFactory.create(7, 1));
        container.removeCursor("test1");

        assertEquals(container.getSlowestCursorPosition(), PositionFactory.create(7, 1));
        container.removeCursor("test4");

        assertEquals(container.getSlowestCursorPosition(), PositionFactory.create(8, 5));
        container.removeCursor("test3");

        assertFalse(container.hasDurableCursors());
    }

    @Test
    public void testDataVersion() {
        assertThat(ManagedCursorContainer.DataVersion.compareVersions(1L, 3L)).isNegative();
        assertThat(ManagedCursorContainer.DataVersion.compareVersions(3L, 1L)).isPositive();
        assertThat(ManagedCursorContainer.DataVersion.compareVersions(3L, 3L)).isZero();

        long v1 = Long.MAX_VALUE - 1;
        long v2 = ManagedCursorContainer.DataVersion.getNextVersion(v1);

        assertThat(ManagedCursorContainer.DataVersion.compareVersions(v1, v2)).isNegative();

        v2 = ManagedCursorContainer.DataVersion.getNextVersion(v2);
        assertThat(ManagedCursorContainer.DataVersion.compareVersions(v1, v2)).isNegative();

        v1 = ManagedCursorContainer.DataVersion.getNextVersion(v1);
        assertThat(ManagedCursorContainer.DataVersion.compareVersions(v1, v2)).isNegative();

        v1 = ManagedCursorContainer.DataVersion.getNextVersion(v1);
        assertThat(ManagedCursorContainer.DataVersion.compareVersions(v1, v2)).isZero();

        v1 = ManagedCursorContainer.DataVersion.getNextVersion(v1);
        assertThat(ManagedCursorContainer.DataVersion.compareVersions(v1, v2)).isPositive();
    }

    @Test
    public void testVersions() {
        ManagedCursorContainerImpl container = new ManagedCursorContainerImpl();

        MockManagedCursor c1 = new MockManagedCursor(container, "test1", PositionFactory.create(5, 5));
        MockManagedCursor c2 = new MockManagedCursor(container, "test2", PositionFactory.create(5, 1));

        container.add(c1, c1.getMarkDeletedPosition());
        long version = container.getCursorWithOldestPosition().getVersion();

        container.add(c2, c2.getMarkDeletedPosition());
        long newVersion = container.getCursorWithOldestPosition().getVersion();
        // newVersion > version
        assertThat(ManagedCursorContainer.DataVersion.compareVersions(newVersion, version)).isPositive();
        version = newVersion;

        container.cursorUpdated(c2, PositionFactory.create(5, 8));
        newVersion = container.getCursorWithOldestPosition().getVersion();
        // newVersion > version
        assertThat(ManagedCursorContainer.DataVersion.compareVersions(newVersion, version)).isPositive();
        version = newVersion;

        container.removeCursor("test2");
        newVersion = container.getCursorWithOldestPosition().getVersion();
        // newVersion > version
        assertThat(ManagedCursorContainer.DataVersion.compareVersions(newVersion, version)).isPositive();
    }

    @Test
    public void testSlowestReader() {
        // test 100 times
        for (int i = 0; i < 100; i++) {
            ManagedCursorContainerImpl container = new ManagedCursorContainerImpl();
            List<ManagedCursor> cursors = IntStream.rangeClosed(1, 100)
                    .mapToObj(idx -> createCursor(container, "cursor" + idx, PositionFactory.create(0, idx)))
                    .collect(Collectors.toList());
            // randomize adding order
            Collections.shuffle(cursors);
            cursors.forEach(cursor -> container.add(cursor, cursor.getReadPosition()));
            assertEquals(container.getSlowestCursor().getName(), "cursor1");
        }
    }
}
