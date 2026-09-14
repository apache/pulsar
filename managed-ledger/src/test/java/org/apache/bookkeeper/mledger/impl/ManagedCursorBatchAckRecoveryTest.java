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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException.MetaStoreException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.MetaStore.MetaStoreCallback;
import org.apache.bookkeeper.mledger.proto.BatchedEntryDeletionIndexInfo;
import org.apache.bookkeeper.mledger.proto.ManagedCursorInfo;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.metadata.api.Stat;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/** Verifies recovery of partial batch acknowledgments from cursor metadata. */
@Test(timeOut = 30000)
public class ManagedCursorBatchAckRecoveryTest extends MockedBookKeeperTestCase {

    @DataProvider(name = "batchRecovery")
    public Object[][] batchRecovery() {
        return new Object[][] {{1, false}, {2, false}, {3, false}, {1, true}, {2, true}, {3, true}};
    }

    @DataProvider(name = "booleans")
    public Object[][] booleans() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "batchRecovery")
    public void testPartialBatchAckPersistenceRetainsConfiguredCount(int batchCount, boolean metadataStore)
            throws Exception {
        ManagedLedgerConfig config = new ManagedLedgerConfig()
                .setMaxUnackedRangesToPersistInMetadataStore(metadataStore ? 20 : -1);
        config.setDeletionAtBatchIndexLevelEnabled(true);
        config.setMaxBatchDeletedIndexToPersist(2);
        String name = "tenant/ns/persistent/partial-batch-recovery-" + batchCount + "-" + metadataStore;
        @Cleanup
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open(name, config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("sub");
        List<Position> positions = new ArrayList<>();
        for (int i = 0; i <= batchCount; i++) {
            positions.add(ledger.addEntry(new byte[] {(byte) i}));
        }
        long[][] ackSets = {{2L}, {Long.MIN_VALUE, 1L}, {5L, 0L, 3L}};
        for (int i = 1; i < positions.size(); i++) {
            Position position = positions.get(i);
            cursor.delete(AckSetStateUtil.createPositionWithAckSet(
                    position.getLedgerId(), position.getEntryId(), ackSets[i - 1]));
        }

        // Advance mark-delete, then close to persist and reload the actual durable state.
        cursor.delete(positions.get(0));
        ledger.close();
        ManagedCursorInfo persisted = storedCursorInfo(ledger);
        if (metadataStore) {
            assertThat(persisted.getCursorsLedgerId()).isEqualTo(-1L);
            int expectedPersistedCount = Math.min(batchCount, 2);
            assertThat(persisted.getBatchedEntryDeletionIndexInfosCount())
                    .as("the partial ACK records were actually written before recovery")
                    .isEqualTo(expectedPersistedCount);
            for (int i = 0; i < expectedPersistedCount; i++) {
                BatchedEntryDeletionIndexInfo indexInfo = persisted.getBatchedEntryDeletionIndexInfoAt(i);
                assertThat(indexInfo.getPosition().getLedgerId()).isEqualTo(positions.get(i + 1).getLedgerId());
                assertThat(indexInfo.getPosition().getEntryId()).isEqualTo(positions.get(i + 1).getEntryId());
                assertThat(deleteSet(indexInfo)).containsExactly(ackSets[i]);
            }
        } else {
            assertThat(persisted.getCursorsLedgerId()).isGreaterThanOrEqualTo(0);
        }

        @Cleanup
        ManagedLedgerImpl reopened = (ManagedLedgerImpl) factory.open(name, config);
        ManagedCursorImpl recovered = (ManagedCursorImpl) reopened.openCursor("sub");
        assertThat(recovered).isNotSameAs(cursor);
        for (int i = 1; i <= batchCount; i++) {
            long[] recoveredAckSet = recovered.getBatchPositionAckSet(positions.get(i));
            if (i <= 2) {
                assertThat(recoveredAckSet).containsExactly(ackSets[i - 1]);
            } else {
                assertThat(recoveredAckSet).as("ACK state above the configured batch cap is truncated").isNull();
            }
        }
    }

    @Test
    public void testMetadataStoreRecoveryPreservesAllCursorState() throws Exception {
        ManagedLedgerConfig config = new ManagedLedgerConfig()
                .setMaxUnackedRangesToPersistInMetadataStore(20);
        config.setDeletionAtBatchIndexLevelEnabled(true);
        String name = "tenant/ns/persistent/combined-metadata-store-recovery";
        @Cleanup
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open(name, config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("sub");
        List<Position> positions = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            positions.add(ledger.addEntry(new byte[] {(byte) i}));
        }

        cursor.markDelete(positions.get(0), Map.of("mark-delete-property", 7L));
        cursor.setCursorProperties(Map.of("cursor-property", "value")).get(5, TimeUnit.SECONDS);
        cursor.delete(positions.get(2));
        long[] ackSet = {Long.MIN_VALUE, 1L};
        cursor.delete(AckSetStateUtil.createPositionWithAckSet(
                positions.get(4).getLedgerId(), positions.get(4).getEntryId(), ackSet));
        ledger.close();

        ManagedCursorInfo persisted = storedCursorInfo(ledger);
        assertThat(persisted.getCursorsLedgerId()).isEqualTo(-1L);
        assertThat(persisted.getIndividualDeletedMessagesCount()).isEqualTo(1);
        assertThat(persisted.getBatchedEntryDeletionIndexInfosCount()).isEqualTo(1);
        assertThat(deleteSet(persisted.getBatchedEntryDeletionIndexInfoAt(0))).containsExactly(ackSet);

        @Cleanup
        ManagedLedgerImpl reopened = (ManagedLedgerImpl) factory.open(name, config);
        ManagedCursorImpl recovered = (ManagedCursorImpl) reopened.openCursor("sub");
        assertThat(recovered).isNotSameAs(cursor);
        assertThat(recovered.getMarkDeletedPosition()).isEqualTo(positions.get(0));
        assertThat(recovered.getProperties()).containsExactlyEntriesOf(Map.of("mark-delete-property", 7L));
        assertThat(recovered.getCursorProperties()).containsExactlyEntriesOf(Map.of("cursor-property", "value"));
        assertThat(recovered.isMessageDeleted(positions.get(1))).isFalse();
        assertThat(recovered.isMessageDeleted(positions.get(2))).isTrue();
        assertThat(recovered.getBatchPositionAckSet(positions.get(4))).containsExactly(ackSet);
    }

    @Test(dataProvider = "booleans")
    public void testMetadataStoreRecoveryHandlesMissingOrDisabledBatchIndexAck(boolean recoverBatchIndexAck)
            throws Exception {
        ManagedLedgerConfig writeConfig = new ManagedLedgerConfig()
                .setMaxUnackedRangesToPersistInMetadataStore(20);
        writeConfig.setDeletionAtBatchIndexLevelEnabled(!recoverBatchIndexAck);
        String name = "tenant/ns/persistent/batch-metadata-compatibility-" + recoverBatchIndexAck;
        @Cleanup
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open(name, writeConfig);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("sub");
        Position position = ledger.addEntry(new byte[] {1});
        if (!recoverBatchIndexAck) {
            cursor.delete(AckSetStateUtil.createPositionWithAckSet(
                    position.getLedgerId(), position.getEntryId(), new long[] {2L}));
        }
        ledger.close();

        ManagedCursorInfo persisted = storedCursorInfo(ledger);
        assertThat(persisted.getCursorsLedgerId()).isEqualTo(-1L);
        assertThat(persisted.getBatchedEntryDeletionIndexInfosCount())
                .isEqualTo(recoverBatchIndexAck ? 0 : 1);

        ManagedLedgerConfig recoveryConfig = new ManagedLedgerConfig()
                .setMaxUnackedRangesToPersistInMetadataStore(20);
        recoveryConfig.setDeletionAtBatchIndexLevelEnabled(recoverBatchIndexAck);
        @Cleanup
        ManagedLedgerImpl reopened = (ManagedLedgerImpl) factory.open(name, recoveryConfig);
        ManagedCursorImpl recovered = (ManagedCursorImpl) reopened.openCursor("sub");
        assertThat(recovered.getBatchPositionAckSet(position)).isNull();
    }

    private static long[] deleteSet(BatchedEntryDeletionIndexInfo indexInfo) {
        long[] result = new long[indexInfo.getDeleteSetsCount()];
        for (int i = 0; i < result.length; i++) {
            result[i] = indexInfo.getDeleteSetAt(i);
        }
        return result;
    }

    private static ManagedCursorInfo storedCursorInfo(ManagedLedgerImpl ledger) throws Exception {
        CompletableFuture<ManagedCursorInfo> result = new CompletableFuture<>();
        ledger.getStore().asyncGetCursorInfo(ledger.getName(), "sub", new MetaStoreCallback<>() {
            @Override
            public void operationComplete(ManagedCursorInfo info, Stat stat) {
                result.complete(info);
            }

            @Override
            public void operationFailed(MetaStoreException exception) {
                result.completeExceptionally(exception);
            }
        });
        return result.get(5, TimeUnit.SECONDS);
    }
}
