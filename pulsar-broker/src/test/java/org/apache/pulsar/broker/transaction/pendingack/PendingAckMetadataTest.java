/**
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
package org.apache.pulsar.broker.transaction.pendingack;

import lombok.Cleanup;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.broker.transaction.pendingack.impl.MLPendingAckStore;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.CommandAck;
import org.testng.annotations.Test;
import java.lang.reflect.Field;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import static org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.State.WriteFailed;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.fail;

public class PendingAckMetadataTest extends MockedBookKeeperTestCase {

    public PendingAckMetadataTest() {
        super(3);
    }

    @Test
    public void testPendingAckManageLedgerWriteFailState() throws Exception {
        ManagedLedgerFactoryConfig factoryConf = new ManagedLedgerFactoryConfig();
        factoryConf.setMaxCacheSize(0);

        String pendingAckTopicName = MLPendingAckStore
                .getTransactionPendingAckStoreSuffix("test", "test");
        @Cleanup("shutdown")
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc, factoryConf);

        CompletableFuture<ManagedLedger> completableFuture = new CompletableFuture<>();
        factory.asyncOpen(pendingAckTopicName, new AsyncCallbacks.OpenLedgerCallback() {
            @Override
            public void openLedgerComplete(ManagedLedger ledger, Object ctx) {
                completableFuture.complete(ledger);
            }

            @Override
            public void openLedgerFailed(ManagedLedgerException exception, Object ctx) {

            }
        }, null);

        ManagedCursor cursor = completableFuture.get().openCursor("test");
        ManagedCursor subCursor = completableFuture.get().openCursor("test");
        MLPendingAckStore pendingAckStore =
                new MLPendingAckStore(completableFuture.get(), cursor, subCursor, 500);

        Field field = MLPendingAckStore.class.getDeclaredField("managedLedger");
        field.setAccessible(true);
        ManagedLedgerImpl managedLedger = (ManagedLedgerImpl) field.get(pendingAckStore);
        field = ManagedLedgerImpl.class.getDeclaredField("STATE_UPDATER");
        field.setAccessible(true);
        AtomicReferenceFieldUpdater<ManagedLedgerImpl, ManagedLedgerImpl.State> state =
                (AtomicReferenceFieldUpdater<ManagedLedgerImpl, ManagedLedgerImpl.State>) field.get(managedLedger);
        state.set(managedLedger, WriteFailed);
        try {
            pendingAckStore.appendAbortMark(new TxnID(1, 1), CommandAck.AckType.Cumulative).get();
            fail();
        } catch (ExecutionException e) {
            assertTrue(e.getCause().getCause() instanceof ManagedLedgerException.ManagedLedgerAlreadyClosedException);
        }
        pendingAckStore.appendAbortMark(new TxnID(1, 1), CommandAck.AckType.Cumulative).get();

        completableFuture.get().close();
        cursor.close();
        subCursor.close();
    }

}
