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

import static org.assertj.core.api.Assertions.assertThat;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.pulsar.common.api.proto.CommandSubscribe.InitialPosition;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ActiveManagedCursorContainerUntrackingTest extends BookKeeperClusterTestCase {
    private ManagedLedgerFactoryImpl factory;
    private ManagedLedgerImpl ledger;
    private ActiveManagedCursorContainerImpl container;
    private final List<ManagedCursor> cursors = new ArrayList<>();
    private final List<Position> positions = new ArrayList<>();

    public ActiveManagedCursorContainerUntrackingTest() {
        super(1);
    }

    @BeforeMethod(alwaysRun = true)
    public void createLedger() throws Exception {
        factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedgerConfig config = new ManagedLedgerConfig()
                .setEnsembleSize(1).setWriteQuorumSize(1).setAckQuorumSize(1);
        config.setCacheEvictionByExpectedReadCount(true);
        config.setCacheEvictionByMarkDeletedPosition(false);
        config.setPulsarMessageEntries(false);
        ledger = (ManagedLedgerImpl) factory.open("cursor-untracking-" + UUID.randomUUID(), config);
        container = (ActiveManagedCursorContainerImpl) ledger.getActiveCursors();
        positions.clear();
        cursors.clear();
        for (int i = 0; i < 7; i++) {
            positions.add(ledger.addEntry(("entry-" + i).getBytes(StandardCharsets.UTF_8)));
        }
        for (int i = 0; i < 5; i++) {
            ManagedCursor cursor = ledger.openCursor("cursor" + i, InitialPosition.Earliest);
            cursor.seek(positions.get(i + 1));
            cursor.setActive();
            cursors.add(cursor);
        }
        assertThat(container.size()).isEqualTo(5);
        assertThat(container.getSlowestCursorPosition()).isEqualTo(positions.get(1));
    }

    @AfterMethod(alwaysRun = true)
    public void closeFactory() throws Exception {
        if (factory != null) {
            factory.shutdown();
            factory = null;
        }
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
        assertThat(ledger.getNumberOfCursorsAtSamePositionOrBefore(cursors.get(4))).isEqualTo(4);
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
            // Exercise ManagedCursorImpl -> ManagedLedgerImpl -> the ledger's active cursor container.
            cursor.seek(restored);
        }
        // One update among four tracked nodes forces incremental insertion after either removal path.
        assertThat(ledger.getNumberOfCursorsAtSamePositionOrBefore(cursor)).isEqualTo(moveForward ? 5 : 1);
        assertThat(container.getSlowestCursorPosition()).isEqualTo(moveForward ? positions.get(1) : restored);
        for (int i = 0; i < cursors.size(); i++) {
            if (i != 2) {
                int rankWithoutRestored = i < 2 ? i + 1 : i;
                assertThat(ledger.getNumberOfCursorsAtSamePositionOrBefore(cursors.get(i)))
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
        // Real cursors, registered through the public container API without ever entering its ordered list.
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
        cursors.get(0).seek(positions.get(1));
        cursors.get(4).seek(positions.get(5));
    }

    private void assertUntracked(ManagedCursor cursor) {
        assertThat(ledger.getNumberOfCursorsAtSamePositionOrBefore(cursor)).isZero();
        assertThat(container.size()).isEqualTo(5);
        assertThat(container.get(cursor.getName())).isSameAs(cursor);
    }
}
