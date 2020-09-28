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
package org.apache.pulsar.broker.transaction.buffer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.google.common.collect.Lists;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.pulsar.broker.transaction.buffer.impl.InMemTransactionBufferProvider;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.broker.transaction.buffer.exceptions.TransactionNotFoundException;
import org.apache.pulsar.broker.transaction.buffer.exceptions.TransactionNotSealedException;
import org.apache.pulsar.broker.transaction.buffer.exceptions.TransactionStatusException;
import org.apache.pulsar.transaction.impl.common.TxnStatus;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

/**
 * Unit test different {@link TransactionBufferProvider}.
 */
public class TransactionBufferTest {

    @DataProvider(name = "providers")
    public static Object[][] providers() {
        return new Object[][] {
            { InMemTransactionBufferProvider.class.getName() }
        };
    }

    private final TxnID txnId = new TxnID(1234L, 2345L);
    private final String providerClassName;
    private TransactionBufferProvider provider;
    private TransactionBuffer buffer;

    @Factory(dataProvider = "providers")
    public TransactionBufferTest(String providerClassName) throws Exception {
        this.providerClassName = providerClassName;
        this.provider = TransactionBufferProvider.newProvider(providerClassName);
    }

    @BeforeMethod
    public void setup() throws Exception {
        this.buffer = this.provider.newTransactionBuffer().get();
    }

    @AfterMethod
    public void teardown() throws Exception {
        this.buffer.closeAsync();
    }

    @Test
    public void testOpenReaderOnNonExistentTxn() throws Exception {
        try {
            buffer.openTransactionBufferReader(txnId, 0L).get();
            fail("Should fail to open reader if a transaction doesn't exist");
        } catch (ExecutionException ee) {
            assertTrue(ee.getCause() instanceof TransactionNotFoundException);
        }
    }

    @Test
    public void testOpenReaderOnAnOpenTxn() throws Exception {
        final int numEntries = 10;
        appendEntries(txnId, numEntries, 0L);
        TransactionMeta txnMeta = buffer.getTransactionMeta(txnId).get();
        assertEquals(txnId, txnMeta.id());
        assertEquals(TxnStatus.OPEN, txnMeta.status());

        try {
            buffer.openTransactionBufferReader(txnId, 0L).get();
            fail("Should fail to open a reader on an OPEN transaction");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof TransactionNotSealedException);
        }
    }

    @Test
    public void testOpenReaderOnCommittedTxn() throws Exception {
        final int numEntries = 10;
        appendEntries(txnId, numEntries, 0L);
        TransactionMeta txnMeta = buffer.getTransactionMeta(txnId).get();
        assertEquals(txnId, txnMeta.id());
        assertEquals(TxnStatus.OPEN, txnMeta.status());

        // commit the transaction
        buffer.commitTxn(txnId, 22L, 33L);
        txnMeta = buffer.getTransactionMeta(txnId).get();
        assertEquals(txnId, txnMeta.id());
        assertEquals(TxnStatus.COMMITTED, txnMeta.status());

        // open reader
        try (TransactionBufferReader reader = buffer.openTransactionBufferReader(
            txnId, 0L
        ).get()) {
            // read 10 entries
            List<TransactionEntry> txnEntries = reader.readNext(numEntries).get();
            verifyAndReleaseEntries(txnEntries, txnId, 0L, numEntries);
        }
    }

    @Test
    public void testCommitNonExistentTxn() throws Exception {
        try {
            buffer.commitTxn(txnId, 22L, 33L).get();
            fail("Should fail to commit a transaction if it doesn't exist");
        } catch (ExecutionException ee) {
            assertTrue(ee.getCause() instanceof TransactionNotFoundException);
        }
    }

    @Test
    public void testCommitTxn() throws Exception {
        final int numEntries = 10;
        appendEntries(txnId, numEntries, 0L);
        TransactionMeta txnMeta = buffer.getTransactionMeta(txnId).get();
        assertEquals(txnId, txnMeta.id());
        assertEquals(TxnStatus.OPEN, txnMeta.status());
        // commit the transaction
        buffer.commitTxn(txnId, 22L, 33L);
        txnMeta = buffer.getTransactionMeta(txnId).get();
        assertEquals(txnId, txnMeta.id());
        assertEquals(TxnStatus.COMMITTED, txnMeta.status());
    }

    @Test
    public void testAbortNonExistentTxn() throws Exception {
        try {
            buffer.abortTxn(txnId).get();
            fail("Should fail to abort a transaction if it doesn't exist");
        } catch (ExecutionException ee) {
            assertTrue(ee.getCause() instanceof TransactionNotFoundException);
        }
    }

    @Test
    public void testAbortCommittedTxn() throws Exception {
        final int numEntries = 10;
        appendEntries(txnId, numEntries, 0L);
        TransactionMeta txnMeta = buffer.getTransactionMeta(txnId).get();
        assertEquals(txnId, txnMeta.id());
        assertEquals(TxnStatus.OPEN, txnMeta.status());
        // commit the transaction
        buffer.commitTxn(txnId, 22L, 33L);
        txnMeta = buffer.getTransactionMeta(txnId).get();
        assertEquals(txnId, txnMeta.id());
        assertEquals(TxnStatus.COMMITTED, txnMeta.status());
        // abort the transaction. it should be discarded from the buffer
        try {
            buffer.abortTxn(txnId).get();
            fail("Should fail to abort a committed transaction");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof TransactionStatusException);
        }
        txnMeta = buffer.getTransactionMeta(txnId).get();
        assertEquals(txnId, txnMeta.id());
        assertEquals(TxnStatus.COMMITTED, txnMeta.status());
    }

    @Test
    public void testAbortTxn() throws Exception {
        final int numEntries = 10;
        appendEntries(txnId, numEntries, 0L);
        TransactionMeta txnMeta = buffer.getTransactionMeta(txnId).get();
        assertEquals(txnId, txnMeta.id());
        assertEquals(TxnStatus.OPEN, txnMeta.status());
        // abort the transaction. it should be discarded from the buffer
        buffer.abortTxn(txnId).get();
        verifyTxnNotExist(txnId);
    }

    @Test
    public void testPurgeTxns() throws Exception {
        final int numEntries = 10;
        // create an OPEN txn
        TxnID txnId1 = new TxnID(1234L, 3456L);
        appendEntries(txnId1, numEntries, 0L);
        TransactionMeta txnMeta = buffer.getTransactionMeta(txnId1).get();
        assertEquals(txnId1, txnMeta.id());
        assertEquals(TxnStatus.OPEN, txnMeta.status());

        // create two committed txns
        TxnID txnId2 = new TxnID(1234L, 4567L);
        appendEntries(txnId2, numEntries, 0L);
        buffer.commitTxn(txnId2, 22L, 0L);
        TransactionMeta txnMeta2 = buffer.getTransactionMeta(txnId2).get();
        assertEquals(txnId2, txnMeta2.id());
        assertEquals(TxnStatus.COMMITTED, txnMeta2.status());

        TxnID txnId3 = new TxnID(1234L, 5678L);
        appendEntries(txnId3, numEntries, 0L);
        buffer.commitTxn(txnId3, 23L, 0L);
        TransactionMeta txnMeta3 = buffer.getTransactionMeta(txnId3).get();
        assertEquals(txnId3, txnMeta3.id());
        assertEquals(TxnStatus.COMMITTED, txnMeta3.status());

        // purge the transaction committed on ledger `22L`
        buffer.purgeTxns(Lists.newArrayList(Long.valueOf(22L))).get();

        // txnId2 should be purged
        verifyTxnNotExist(txnId2);

        // txnId1 should still be OPEN
        txnMeta = buffer.getTransactionMeta(txnId1).get();
        assertEquals(txnId1, txnMeta.id());
        assertEquals(TxnStatus.OPEN, txnMeta.status());

        // txnId3 should still be COMMITTED
        txnMeta3 = buffer.getTransactionMeta(txnId3).get();
        assertEquals(txnId3, txnMeta3.id());
        assertEquals(TxnStatus.COMMITTED, txnMeta3.status());
    }

    private void appendEntries(TxnID txnId, int numEntries, long startSequenceId) {
        for (int i = 0; i < numEntries; i++) {
            long sequenceId = startSequenceId + i;
            buffer.appendBufferToTxn(
                txnId,
                sequenceId,
                1,
                Unpooled.copiedBuffer("message-" + sequenceId, UTF_8)
            ).join();
        }
    }

    private void verifyAndReleaseEntries(List<TransactionEntry> txnEntries,
                                         TxnID txnID,
                                         long startSequenceId,
                                         int numEntriesToRead) {
        assertEquals(txnEntries.size(), numEntriesToRead);
        for (int i = 0; i < numEntriesToRead; i++) {
            try (TransactionEntry txnEntry = txnEntries.get(i)) {
                assertEquals(txnEntry.committedAtLedgerId(), 22L);
                assertEquals(txnEntry.committedAtEntryId(), 33L);
                assertEquals(txnEntry.txnId(), txnID);
                assertEquals(txnEntry.sequenceId(), startSequenceId + i);
                assertEquals(new String(
                    ByteBufUtil.getBytes(txnEntry.getEntry().getDataBuffer()),
                    UTF_8
                ), "message-" + i);
            }
        }
    }

    private void verifyTxnNotExist(TxnID txnID) throws Exception {
        try {
            buffer.getTransactionMeta(txnID).get();
            fail("Should fail to get transaction metadata if it doesn't exist");
        } catch (ExecutionException ee) {
            assertTrue(ee.getCause() instanceof TransactionNotFoundException);
        }
    }
}
