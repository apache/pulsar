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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import org.apache.pulsar.broker.transaction.buffer.exceptions.EndOfTransactionException;
import org.apache.pulsar.broker.transaction.buffer.impl.InMemTransactionBufferReader;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.testng.annotations.Test;

/**
 * Unit test {@link InMemTransactionBufferReader}.
 */
@Test(groups = "broker")
public class InMemTransactionBufferReaderTest {

    private final TxnID txnID = new TxnID(1234L, 5678L);

    @Test
    public void testInvalidNumEntriesArgument() {
        try (InMemTransactionBufferReader reader = new InMemTransactionBufferReader(
            txnID,
            Collections.<Long, ByteBuf>emptySortedMap().entrySet().iterator(),
            22L,
            33L
        )) {
            try {
                reader.readNext(-1).join();
                fail("Should fail to readNext if `numEntries` is invalid");
            } catch (CompletionException ce) {
                assertTrue(ce.getCause() instanceof IllegalArgumentException);
            }
        }
    }

    @Test
    public void testCloseReleaseAllEntries() throws Exception {
        SortedMap<Long, ByteBuf> entries = new TreeMap<>();
        final int numEntries = 100;
        for (int i = 0; i < numEntries; i++) {
            entries.put((long) i, Unpooled.copiedBuffer("message-" + i, UTF_8));
        }

        try (InMemTransactionBufferReader reader = new InMemTransactionBufferReader(
            txnID,
            entries.entrySet().iterator(),
            22L,
            33L
        )) {

            int numEntriesToRead = 10;
            // read 10 entries
            List<TransactionEntry> txnEntries = reader.readNext(numEntriesToRead).get();
            verifyAndReleaseEntries(txnEntries, txnID, 0L, numEntriesToRead);
            verifyEntriesReleased(entries, 0L, numEntriesToRead);
        }

        // verify all entries are released after reader is closed.
        verifyEntriesReleased(entries, 10L, numEntries - 10);
    }

    @Test
    public void testEndOfTransactionException() throws Exception {
        SortedMap<Long, ByteBuf> entries = new TreeMap<>();
        final int numEntries = 100;
        for (int i = 0; i < numEntries; i++) {
            entries.put((long) i, Unpooled.copiedBuffer("message-" + i, UTF_8));
        }

        try (InMemTransactionBufferReader reader = new InMemTransactionBufferReader(
            txnID,
            entries.entrySet().iterator(),
            22L,
            33L
        )) {

            int numEntriesToRead = numEntries + 10;
            // read all entries
            List<TransactionEntry> txnEntries = reader.readNext(numEntriesToRead).get();
            verifyAndReleaseEntries(txnEntries, txnID, 0L, numEntries);
            verifyEntriesReleased(entries, 0L, numEntries);

            // since all the entries has been read, the read next operation will be
            // failed with EndOfTransactionException
            try {
                reader.readNext(1).get();
                fail("should fail to read entries if there is no more in the transaction buffer");
            } catch (ExecutionException ee) {
                assertTrue(ee.getCause() instanceof EndOfTransactionException);
            }
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

    private void verifyEntriesReleased(SortedMap<Long, ByteBuf> entries,
                                       long startSequenceId,
                                       int numEntriesToRead) {
        for (int i = 0; i < numEntriesToRead; i++) {
            long sequenceId = startSequenceId + i;
            ByteBuf bb = entries.get(sequenceId);
            assertNotNull(bb);
            assertEquals(bb.refCnt(), 0);
        }
    }
}
