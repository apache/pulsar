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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import lombok.Cleanup;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException.InvalidCursorPositionException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.proto.MessageRange;
import org.apache.bookkeeper.mledger.proto.PositionInfo;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.impl.FaultInjectionMetadataStore.OperationType;
import org.testng.annotations.Test;

public class ManagedCursorResetFailedRebuildTest extends MockedBookKeeperTestCase {

    @Test(timeOut = 30000)
    public void testRebuildingCursorLedgerAfterFailedResetPreservesAcknowledgments() throws Exception {
        ManagedLedgerConfig config = defaultConfig().setThrottleMarkDelete(0)
                .setPersistIndividualAckAsLongArray(false);
        @Cleanup
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("reset-failed-cursor-ledger-rebuild", config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("sub");
        Position markDeletePosition = ledger.addEntry(new byte[] {0});
        Position oldReadPosition = ledger.addEntry(new byte[] {1});
        Position resetPosition = ledger.addEntry(new byte[] {2});
        Position previouslyAcknowledged = ledger.addEntry(new byte[] {3});
        cursor.markDelete(markDeletePosition);
        cursor.delete(previouslyAcknowledged);

        bkc.addEntryFailAfter(0, BKException.Code.NoBookieAvailableException);
        metadataStore.failConditional(new MetadataStoreException("reset metadata-store write failed"),
                (operation, path) -> operation == OperationType.PUT
                        && path.equals("/managed-ledgers/" + ledger.getName() + "/sub"));
        assertThat(ledger.getPreviousPosition(resetPosition)).isGreaterThan(markDeletePosition);
        assertThatThrownBy(() -> cursor.resetCursor(resetPosition))
                .isInstanceOf(InvalidCursorPositionException.class);
        assertThat(cursor.getMarkDeletedPosition()).isEqualTo(markDeletePosition);
        assertThat(cursor.getReadPosition()).isEqualTo(oldReadPosition);
        assertThat(cursor.isMessageDeleted(previouslyAcknowledged)).isTrue();
        assertThat(cursor.getState()).isEqualTo("NoLedger");

        LedgerHandle failedCursorLedger = cursor.cursorLedger;
        // A synchronous ordinary write waits for both replacement-ledger creation and its queued append.
        cursor.markDelete(markDeletePosition);

        LedgerHandle replacementLedger = cursor.cursorLedger;
        assertThat(replacementLedger.getId()).isNotEqualTo(failedCursorLedger.getId());
        // Entry 0 is the immutable rebuild checkpoint; the queued ordinary write is appended afterwards.
        LedgerEntry entry = replacementLedger.readEntries(0, 0).nextElement();
        PositionInfo initialSnapshot = new PositionInfo();
        initialSnapshot.parseFrom(entry.getEntry());
        assertThat(initialSnapshot.getLedgerId()).isEqualTo(markDeletePosition.getLedgerId());
        assertThat(initialSnapshot.getEntryId()).isEqualTo(markDeletePosition.getEntryId());
        assertThat(initialSnapshot.getIndividualDeletedMessagesCount())
                .as("the durable initial snapshot must preserve ACKs from before the failed reset")
                .isEqualTo(1);
        MessageRange range = initialSnapshot.getIndividualDeletedMessageAt(0);
        assertThat(range.getLowerEndpoint().getLedgerId()).isEqualTo(previouslyAcknowledged.getLedgerId());
        assertThat(range.getLowerEndpoint().getEntryId()).isEqualTo(previouslyAcknowledged.getEntryId() - 1);
        assertThat(range.getUpperEndpoint().getLedgerId()).isEqualTo(previouslyAcknowledged.getLedgerId());
        assertThat(range.getUpperEndpoint().getEntryId()).isEqualTo(previouslyAcknowledged.getEntryId());
    }
}
