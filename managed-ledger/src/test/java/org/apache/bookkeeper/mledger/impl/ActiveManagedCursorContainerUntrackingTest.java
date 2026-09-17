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
import java.util.ArrayList;
import java.util.List;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ActiveManagedCursorContainerUntrackingTest {
    private ActiveManagedCursorContainerImpl container;
    private final List<ManagedCursor> cursors = new ArrayList<>();
    private final List<Position> positions = new ArrayList<>();

    @BeforeMethod(alwaysRun = true)
    public void createContainer() {
        container = new ActiveManagedCursorContainerImpl();
        positions.clear();
        cursors.clear();
        for (int i = 0; i < 7; i++) {
            positions.add(PositionFactory.create(1, i));
        }
        for (int i = 0; i < 5; i++) {
            Position position = positions.get(i + 1);
            ManagedCursor cursor = createCursor(container, "cursor" + i, position);
            container.add(cursor, position);
            cursors.add(cursor);
        }
        assertThat(container.size()).isEqualTo(5);
        assertThat(container.getSlowestCursorPosition()).isEqualTo(positions.get(1));
    }

    @DataProvider
    public Object[][] untrackingPaths() {
        return new Object[][] {{false, false}, {false, true}, {true, false}, {true, true}};
    }

    @Test(dataProvider = "untrackingPaths")
    public void testFlushedUntrackingRemainsUntracked(boolean rebuild, boolean useAdd) {
        ManagedCursor cursor = cursors.get(2);
        untrack(cursor, useAdd);
        flushUntracking(rebuild);

        assertUntracked(cursor);
        // Updating two of the four remaining nodes forces a rebuild. It must not resurrect the cursor.
        queueRebuild();
        assertUntracked(cursor);
        assertThat(container.getNumberOfCursorsAtSamePositionOrBefore(cursors.get(4))).isEqualTo(4);
        container.checkOrderingAndNumberOfCursorsState();
    }

    @DataProvider
    public Object[][] reactivationPaths() {
        return new Object[][] {
                {false, false, false}, {false, true, false}, {true, false, false}, {true, true, false},
                {false, false, true}, {false, true, true}, {true, false, true}, {true, true, true}
        };
    }

    @Test(dataProvider = "reactivationPaths", timeOut = 30000)
    public void testRetrackAfterFlush(boolean rebuild, boolean moveForward, boolean useAdd) {
        ManagedCursor cursor = cursors.get(2);
        untrack(cursor, false);
        flushUntracking(rebuild);
        Position restored = positions.get(moveForward ? 6 : 0);
        if (useAdd) {
            container.add(cursor, restored);
        } else {
            container.updateCursor(cursor, restored);
        }
        // One update among four tracked nodes forces incremental insertion after either removal path.
        assertThat(container.getNumberOfCursorsAtSamePositionOrBefore(cursor)).isEqualTo(moveForward ? 5 : 1);
        assertThat(container.getSlowestCursorPosition()).isEqualTo(moveForward ? positions.get(1) : restored);
        for (int i = 0; i < cursors.size(); i++) {
            if (i != 2) {
                int rankWithoutRestored = i < 2 ? i + 1 : i;
                assertThat(container.getNumberOfCursorsAtSamePositionOrBefore(cursors.get(i)))
                        .as("rank of cursor%s", i).isEqualTo(rankWithoutRestored + (moveForward ? 0 : 1));
            }
        }
        container.checkOrderingAndNumberOfCursorsState();
    }

    @Test
    public void testRepeatedUntrackingAfterFlush() {
        ManagedCursor cursor = cursors.get(2);
        container.updateCursor(cursor, null);
        container.getSlowestCursorPosition();
        container.updateCursor(cursor, null);

        assertThat(container.getSlowestCursorPosition()).isEqualTo(positions.get(1));
        assertUntracked(cursor);
        container.checkOrderingAndNumberOfCursorsState();
    }

    @Test
    public void testUntrackingBeforeFirstPositionFlush() {
        // The last cursor never enters this container's ordered list.
        ActiveManagedCursorContainerImpl other = new ActiveManagedCursorContainerImpl();
        for (int i = 0; i < 4; i++) {
            other.add(cursors.get(i), positions.get(i + 1));
        }
        other.getSlowestCursorPosition();
        ManagedCursor cursor = cursors.get(4);
        other.add(cursor, null);
        other.updateCursor(cursor, null);

        assertThat(other.getSlowestCursorPosition()).isEqualTo(positions.get(1));
        assertThat(other.getNumberOfCursorsAtSamePositionOrBefore(cursor)).isZero();
        assertThat(other.getNumberOfCursorsAtSamePositionOrBefore(cursors.get(3))).isEqualTo(4);
        other.checkOrderingAndNumberOfCursorsState();
    }

    private void untrack(ManagedCursor cursor, boolean useAdd) {
        // Normal broker deactivation removes the cursor. Null untracking is a separate container API operation.
        if (useAdd) {
            container.add(cursor, null);
        } else {
            container.updateCursor(cursor, null);
        }
    }

    private void flushUntracking(boolean rebuild) {
        if (rebuild) {
            queueRebuild();
        }
        // No position updates selects incremental removal; two updates among five nodes select a rebuild.
        assertThat(container.getSlowestCursorPosition()).isEqualTo(positions.get(1));
    }

    private void queueRebuild() {
        container.updateCursor(cursors.get(0), positions.get(1));
        container.updateCursor(cursors.get(4), positions.get(5));
    }

    private void assertUntracked(ManagedCursor cursor) {
        assertThat(container.getNumberOfCursorsAtSamePositionOrBefore(cursor)).isZero();
        assertThat(container.size()).isEqualTo(5);
        assertThat(container.get(cursor.getName())).isSameAs(cursor);
    }
}
