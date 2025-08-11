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

import static org.apache.bookkeeper.mledger.impl.MockManagedCursor.addCursor;
import static org.apache.bookkeeper.mledger.impl.MockManagedCursor.createCursor;
import static org.assertj.core.api.Assertions.assertThat;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

public class ActiveManagedCursorContainerTest {
    private final Supplier<ActiveManagedCursorContainer> containerSupplier;

    @Factory
    public static Object[] createTestInstances() {
        return new Object[]{new ActiveManagedCursorContainerTest(ActiveManagedCursorContainerImpl::new),
                new ActiveManagedCursorContainerTest(ActiveManagedCursorContainerNavigableSetImpl::new)};
    }

    ActiveManagedCursorContainerTest(Supplier<ActiveManagedCursorContainer> containerSupplier) {
        this.containerSupplier = containerSupplier;
    }

    @Test
    public void testAddingInReverseOrder() {
        int numberOfCursors = 3;
        ActiveManagedCursorContainer container = containerSupplier.get();
        List<MockManagedCursor> cursors = IntStream.rangeClosed(1, numberOfCursors)
                .mapToObj(idx -> createCursor(container, "cursor" + idx, PositionFactory.create(0, idx)))
                .collect(Collectors.toList());
        Collections.reverse(cursors);
        cursors.forEach(cursor -> container.add(cursor, cursor.getReadPosition()));
        for (int i = 1; i <= numberOfCursors; i++) {
            ManagedCursor cursor = container.get("cursor" + i);
            int numberOfCursorsBefore = container.getNumberOfCursorsAtSamePositionOrBefore(cursor);
            assertThat(numberOfCursorsBefore).describedAs("cursor:%s", cursor).isEqualTo(i);
        }
    }

    @Test
    public void testAddingMultipleWithSamePosition() {
        int numberOfCursors = 1000;
        ActiveManagedCursorContainer container = containerSupplier.get();
        List<MockManagedCursor> cursors = IntStream.rangeClosed(1, numberOfCursors)
                .mapToObj(idx -> createCursor(container, "cursor" + idx, PositionFactory.create(0, idx / 2)))
                .collect(Collectors.toList());
        Collections.shuffle(cursors);
        cursors.forEach(cursor -> container.add(cursor, cursor.getReadPosition()));
        for (int i = 1; i <= numberOfCursors; i++) {
            ManagedCursor cursor = container.get("cursor" + i);
            assertThat(container.getNumberOfCursorsAtSamePositionOrBefore(cursor))
                    .describedAs("cursor:%s", cursor)
                    .isEqualTo(i > 1 && i < numberOfCursors ? i + ((i + 1) % 2) : i);
        }
    }

    @Test
    public void testCountNumberOfCursorsAtSamePositionOrBefore() {
        ActiveManagedCursorContainer container = containerSupplier.get();
        List<MockManagedCursor> cursors = IntStream.rangeClosed(1, 1000)
                .mapToObj(idx -> createCursor(container, "cursor" + idx, PositionFactory.create(0, idx)))
                .collect(Collectors.toList());
        // randomize adding order
        Collections.shuffle(cursors);
        cursors.forEach(cursor -> container.add(cursor, cursor.getReadPosition()));
        for (int i = 1; i <= 1000; i++) {
            ManagedCursor cursor = container.get("cursor" + i);
            int numberOfCursorsBefore = container.getNumberOfCursorsAtSamePositionOrBefore(cursor);
            assertThat(numberOfCursorsBefore).describedAs("cursor:%s", cursor).isEqualTo(i);
        }

        // test updating mark delete position
        // cursor500 will be updated to position 0:1001
        ManagedCursor cursor500 = container.get("cursor500");
        cursor500.seek(PositionFactory.create(0, 1001));
        assertThat(container.getNumberOfCursorsAtSamePositionOrBefore(cursor500)).describedAs(
                "cursor500 should have 1000 at same position or before it").isEqualTo(1000);
        for (int i = 1; i <= 499; i++) {
            ManagedCursor cursor = container.get("cursor" + i);
            int numberOfCursorsBefore = container.getNumberOfCursorsAtSamePositionOrBefore(cursor);
            assertThat(numberOfCursorsBefore).describedAs("cursor:%s", cursor).isEqualTo(i);
        }
        for (int i = 501; i <= 1000; i++) {
            ManagedCursor cursor = container.get("cursor" + i);
            int numberOfCursorsBefore = container.getNumberOfCursorsAtSamePositionOrBefore(cursor);
            assertThat(numberOfCursorsBefore).describedAs("cursor:%s", cursor).isEqualTo(i - 1);
        }
    }

    @Test
    public void testCountNumberOfCursorsAtSamePositionOrBefore_SamePosition() {
        ActiveManagedCursorContainer container = containerSupplier.get();
        addCursor(container, "cursor1", PositionFactory.create(0, 1));
        addCursor(container, "cursor2", PositionFactory.create(0, 2));
        for (int i = 3; i <= 998; i++) {
            addCursor(container, "cursor" + i, PositionFactory.create(0, 3));
        }
        addCursor(container, "cursor999", PositionFactory.create(0, 4));
        addCursor(container, "cursor1000", PositionFactory.create(0, 5));
        ManagedCursor cursor = container.get("cursor4");
        assertThat(container.getNumberOfCursorsAtSamePositionOrBefore(cursor)).isEqualTo(998);
    }

    private void checkState(ActiveManagedCursorContainer container) {
        if (container instanceof ActiveManagedCursorContainerImpl c) {
            c.checkOrderingAndNumberOfCursorsState();
        } else if (container instanceof ActiveManagedCursorContainerNavigableSetImpl c) {
            // checking state for navigable set implementation is very slow, disable it by default
            //c.checkOrderingAndNumberOfCursorsState();
        }
    }

    @Test
    public void testRandomSeekingForward() {
        ActiveManagedCursorContainer container = containerSupplier.get();
        List<MockManagedCursor> cursors = IntStream.rangeClosed(1, 1000)
                .mapToObj(idx -> createCursor(container, "cursor" + idx, PositionFactory.create(0, idx)))
                .collect(Collectors.toList());
        // randomize adding order
        Collections.shuffle(cursors);
        cursors.forEach(cursor -> container.add(cursor, cursor.getReadPosition()));
        checkState(container);

        // seek randomly forward in random cursors
        Random random = new Random(1);
        for (int j = 0; j < 10000; j++) {
            int i = random.nextInt(1, 1000);
            ManagedCursor cursor = container.get("cursor" + i);
            Position currentPosition = cursor.getReadPosition();
            Position newPosition = currentPosition.getPositionAfterEntries(random.nextInt(1, 100));
            cursor.seek(newPosition);
            checkState(container);
        }

        // seek to original positions
        for (int i = 1; i <= 1000; i++) {
            ManagedCursor cursor = container.get("cursor" + i);
            cursor.seek(PositionFactory.create(0, i));
            checkState(container);
        }

        // check that the number of cursors at same position or before are still correct
        for (int i = 1; i <= 1000; i++) {
            ManagedCursor cursor = container.get("cursor" + i);
            int numberOfCursorsBefore = container.getNumberOfCursorsAtSamePositionOrBefore(cursor);
            assertThat(numberOfCursorsBefore).describedAs("cursor:%s", cursor).isEqualTo(i);
        }
    }

    @Test
    public void testRandomSeeking() {
        ActiveManagedCursorContainer container = containerSupplier.get();
        List<MockManagedCursor> cursors = IntStream.rangeClosed(1, 1000)
                .mapToObj(idx -> createCursor(container, "cursor" + idx, PositionFactory.create(1, idx)))
                .collect(Collectors.toList());
        // randomize adding order
        Collections.shuffle(cursors);
        cursors.forEach(cursor -> container.add(cursor, cursor.getReadPosition()));
        checkState(container);

        // seek randomly forward in random cursors
        Random random = new Random(1);
        for (int j = 0; j < 10000; j++) {
            int i = random.nextInt(1, 1000);
            ManagedCursor cursor = container.get("cursor" + i);
            Position currentPosition = cursor.getReadPosition();
            Position newPosition = PositionFactory.create(currentPosition.getLedgerId(),
                    Math.max(currentPosition.getEntryId() + random.nextInt(1, 100) - 25, 1));
            cursor.seek(newPosition);
            checkState(container);
        }

        for (int j = 0; j < 10000; j++) {
            int i = random.nextInt(1, 1000);
            ManagedCursor cursor = container.get("cursor" + i);
            Position originalPosition = cursor.getReadPosition();
            // seek to first entry
            cursor.seek(PositionFactory.create(0, 1));
            checkState(container);
            // seek to last entry
            cursor.seek(PositionFactory.create(2, Long.MAX_VALUE));
            checkState(container);
            cursor.seek(originalPosition);
            checkState(container);
        }

        // random seeking with a lot of same positions
        for (int j = 0; j < 100; j++) {
            for (int i = 1; i <= 1000; i++) {
                ManagedCursor cursor = container.get("cursor" + i);
                int entryId = random.nextInt(1, 10);
                cursor.seek(PositionFactory.create(0, entryId));
                checkState(container);
            }
        }

        // seek to original positions
        for (int i = 1; i <= 1000; i++) {
            ManagedCursor cursor = container.get("cursor" + i);
            cursor.seek(PositionFactory.create(0, i));
            checkState(container);
        }

        // check that the number of cursors at same position or before are still correct
        for (int i = 1; i <= 1000; i++) {
            ManagedCursor cursor = container.get("cursor" + i);
            int numberOfCursorsBefore = container.getNumberOfCursorsAtSamePositionOrBefore(cursor);
            assertThat(numberOfCursorsBefore).describedAs("cursor:%s", cursor).isEqualTo(i);
        }
    }
}