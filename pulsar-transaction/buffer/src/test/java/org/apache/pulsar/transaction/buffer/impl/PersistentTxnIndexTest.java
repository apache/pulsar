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
package org.apache.pulsar.transaction.buffer.impl;

import static org.apache.pulsar.common.protocol.Commands.serializeMetadataAndPayload;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.api.proto.PulsarMarkers;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Markers;
import org.apache.pulsar.transaction.buffer.TransactionMeta;
import org.apache.pulsar.transaction.buffer.exceptions.TransactionNotFoundException;
import org.apache.pulsar.transaction.impl.common.TxnID;
import org.apache.pulsar.transaction.proto.TransactionBufferDataFormats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

public class PersistentTxnIndexTest extends MockedBookKeeperTestCase {

    private Random randomGenerator = new Random(System.currentTimeMillis());

    @Test
    public void testTakeSnapshot() throws ManagedLedgerException, InterruptedException, ExecutionException,
                                          BKException {
        ManagedLedger txnlog = factory.open("test_takesnapshot");
        TransactionCursorImpl cursor = TransactionCursorImpl.createTransactionCursor(txnlog).get();
        List<TransactionMetaImpl> metaList = createExampleData(20);
        metaList.forEach(cursor::addToTxnIndex);
        List<TxnID> txnIDList = metaList.stream().map(TransactionMetaImpl::getTxnID).collect(Collectors.toList());
        cursor.takeSnapshot(PositionImpl.get(-1L, -1L)).get();

        LedgerHandle readLedger = cursor.getCursorLedger();
        assertEquals(readLedger.getLastAddConfirmed(), 22);

        Enumeration<LedgerEntry> entryList = readLedger.readEntries(0, readLedger.getLastAddConfirmed());

        int count = -1;
        while (entryList.hasMoreElements()) {
            if (count == -1) {
                entryList.nextElement();
                count++;
                continue;
            }
            LedgerEntry ledgerEntry = entryList.nextElement();
            byte[] data = ledgerEntry.getEntry();
            TransactionBufferDataFormats.StoredTxnIndexEntry txn = DataFormat.parseStoredTxn(data);
            if (count == 0) {
                assertEquals(txn.getStoredStatus(), TransactionBufferDataFormats.StoredSnapshotStatus.START);
                assertEquals(txn.getPosition().getLedgerId(), -1L);
                assertEquals(txn.getPosition().getEntryId(), -1L);
                count++;
                continue;
            }

            if (count == metaList.size() + 1) {
                assertEquals(txn.getStoredStatus(), TransactionBufferDataFormats.StoredSnapshotStatus.END);
                assertEquals(txn.getPosition().getLedgerId(), readLedger.getId());
                assertEquals(txn.getPosition().getEntryId(), 1);
                count++;
                continue;
            }
            assertEquals(txn.getStoredStatus(), TransactionBufferDataFormats.StoredSnapshotStatus.MIDDLE);
            assertEquals(txn.getPosition().getLedgerId(), readLedger.getId());
            assertEquals(txn.getPosition().getEntryId(), 1);
            TransactionMetaImpl meta = (TransactionMetaImpl) DataFormat.parseToTransactionMeta(data);
            assertTrue(txnIDList.remove(meta.getTxnID()));
            count++;
        }
    }

    // test the recover as this flow:
    //      a. write some snapshot messages in the cursor ledger
    //      b. execute the recover logic to recover the transaction index
    //      c. verify the transaction index
    @Test
    public void testRecoverNormalTxnIndex() throws Exception {

        ManagedLedger txnLog = factory.open("test_recover_txnindex");
        Logger logger = LoggerFactory.getLogger(TransactionCursorImpl.class);

        TransactionCursorImpl cursor = TransactionCursorImpl.createTransactionCursor(txnLog).get();
        LedgerHandle write = cursor.getCursorLedger();
        List<TransactionMetaImpl> metaList = createExampleData(10);
        for (TransactionMetaImpl transactionMeta : metaList) {
            TransactionMeta meta = cursor.findInIndex(transactionMeta.getTxnID());
            assertNull(meta);
        }

        writeExampleDataToLedger(write, metaList, PositionImpl.earliest);

        cursor.recover().get();

        for (TransactionMetaImpl transactionMeta : metaList) {
            TransactionMeta meta = cursor.findInIndex(transactionMeta.getTxnID());
            assertEquals(((TransactionMetaImpl) meta).getTxnID(), transactionMeta.getTxnID());
        }
    }

    // test the  recover as this flow:
    //      a. write some transaction messages in the transaction log
    //      b. no snapshot messages in the cursor ledger
    //      c. make recover the transaction index from the cursor ledger failed
    //      d. should recover from the transaction log
    @Test
    public void testRecoverTxnIndexFromTxnLog1() throws ManagedLedgerException, InterruptedException,
                                                       ExecutionException {
        ManagedLedger txnLedger = factory.open("test_recover1");
        TransactionCursorImpl cursor = TransactionCursorImpl.createTransactionCursor(txnLedger).get();
        long committedAtLedgerId = 1234L;
        long committedAtEntryId = 2345L;
        long mostBits = randomGenerator.nextInt(1000);
        long leastBits = randomGenerator.nextInt(1000);
        TxnID txnID = new TxnID(mostBits, leastBits);
        List<Position> positionList = generateNormalDataInTxnLog(txnID, txnLedger, committedAtLedgerId,
                                                                 committedAtEntryId, 0, true, false);

        assertEquals(positionList.size(), 11);

        try {
            cursor.getTxnMeta(txnID, false).get();
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof TransactionNotFoundException);
        }

        cursor.recover().get();

        TransactionMetaImpl meta = (TransactionMetaImpl) cursor.getTxnMeta(txnID, false).get();
        SortedMap<Long, Position> entries = meta.getEntries();

        assertEquals(entries.size(), 10);
        int count = 0;
        for (Map.Entry<Long, Position> longPositionEntry : entries.entrySet()) {
            int c = count++;
            assertEquals(longPositionEntry.getKey().longValue(), c);
            assertEquals(longPositionEntry.getValue(), positionList.get(c));
        }
    }

    // test the  recover as this flow:
    //      a. write some transaction messages in the transaction log
    //      b. write some snapshot messages in the cursor ledger, and the position of the start snapshot point to the
    //      last entry in the transaction log.
    //      c. make recover the transaction index from the cursor ledger failed
    //      d. should recover from the transaction log
    @Test
    public void testRecoverTxnIndexFromTxnLog2() throws Exception {
        ManagedLedger txnLedger = factory.open("test_recover2");
        TransactionCursorImpl cursor = TransactionCursorImpl.createTransactionCursor(txnLedger).get();
        long committedAtLedgerId = randomGenerator.nextInt(1000);
        long committedAtEntryId = randomGenerator.nextInt(1000);

        long mostBits = randomGenerator.nextInt(1000);
        long leastBits = randomGenerator.nextInt(1000);

        TxnID txnID = new TxnID(mostBits, leastBits);
        List<Position> appendOne = generateNormalDataInTxnLog(txnID, txnLedger, committedAtLedgerId,
                                                                 committedAtEntryId, 0, false, false);
        assertEquals(appendOne.size(), 10);

        writeSnapshotStartToLedger(cursor.getCursorLedger(), appendOne.get(3));

        try {
            cursor.getTxnMeta(txnID, false).get();
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof TransactionNotFoundException);
        }

        List<Position> appendTwo = generateNormalDataInTxnLog(txnID, txnLedger, committedAtLedgerId, committedAtEntryId,
                                                              appendOne.size(), true, false);
        assertEquals(appendTwo.size(), 11);

        cursor.recover().get();

        TransactionMetaImpl meta = (TransactionMetaImpl) cursor.getTxnMeta(txnID, false).get();
        SortedMap<Long, Position> entries = meta.getEntries();

        assertEquals(entries.size(), 20);

        appendOne.addAll(appendTwo);
        int count = 0;
        for (Map.Entry<Long, Position> longPositionEntry : entries.entrySet()) {
            int c = count++;
            assertEquals(longPositionEntry.getKey().longValue(), c);
            assertEquals(longPositionEntry.getValue(), appendOne.get(c));
        }

    }

    // test the recover as this flow:
    //      a. write some transaction messages in the transaction log
    //      b. write some snapshot messages in the cursor ledger, and the position of the start snapshot point  to
    //      the middle entry in the transaction log
    //      c. the transaction index should recover from snapshot and then replay all entries in the transaction
    //      log
    @Test
    public void testRecoverTxnIndexFromTxnLog3() throws Exception {
        ManagedLedger txnLedger = factory.open("test_recover3");
        TransactionCursorImpl cursor = TransactionCursorImpl.createTransactionCursor(txnLedger).get();

        long committedAtLedgerId = randomGenerator.nextInt(1000);
        long committedAtEntryId = randomGenerator.nextInt(1000);

        long mostBits = randomGenerator.nextInt(1000);
        long leastBits = randomGenerator.nextInt(1000);

        TxnID txnID = new TxnID(mostBits, leastBits);
        List<Position> appendOne = generateNormalDataInTxnLog(txnID, txnLedger, committedAtLedgerId, committedAtEntryId,
                                                              0, false, false);
        assertEquals(appendOne.size(), 10);

        List<TransactionMetaImpl> metaList = createExampleData(10);
        writeExampleDataToLedger(cursor.getCursorLedger(), metaList, appendOne.get(5));

        cursor.recover().get();

        TransactionMetaImpl meta = (TransactionMetaImpl) cursor.getTxnMeta(txnID, false).get();
        SortedMap<Long, Position> entries = meta.getEntries();

        // there only have 10 entries in the txnID,
        assertEquals(entries.size(), 5);
        int count = 5;
        for (Map.Entry<Long, Position> longPositionEntry : entries.entrySet()) {
            int c = count++;
            assertEquals(longPositionEntry.getKey().longValue(), c);
            assertEquals(longPositionEntry.getValue(), appendOne.get(c));
        }

        for (TransactionMetaImpl transactionMeta : metaList) {
            try {
                cursor.getTxnMeta(transactionMeta.getTxnID(), false).get();
            } catch (Exception e) {
                fail("Should not have any error");
            }
        }
    }

    private List<Position> generateNormalDataInTxnLog(TxnID txnID, ManagedLedger ledger, long commitAtLedgerId,
                                                     long commitAtEntryId, long sequenceBase, boolean commit,
                                                      boolean abort)
        throws ManagedLedgerException, InterruptedException {
        List<Position> positions = new ArrayList<>();
        List<ByteBuf> messages = generateMessageMetaData(txnID, 10, commitAtLedgerId, commitAtEntryId, sequenceBase,
                                                         commit, abort);
        for (ByteBuf message : messages) {
            Position position = ledger.addEntry(message.array());
            positions.add(position);
        }

        return positions;
    }

    private List<ByteBuf> generateMessageMetaData(TxnID txnID, int numMessage, long commitAtLedgerId,
                                                  long commitAtEntryId, long sequenceBase, boolean commit,
                                                  boolean abort) {
        List<ByteBuf> txnMessage = new ArrayList<>();

        IntStream.range(0, numMessage).forEach(i -> {
            PulsarApi.MessageMetadata.Builder messageBuilder = PulsarApi.MessageMetadata.newBuilder();
            messageBuilder.setTxnidMostBits(txnID.getMostSigBits());
            messageBuilder.setTxnidLeastBits(txnID.getLeastSigBits());
            messageBuilder.setSequenceId(i + sequenceBase);
            messageBuilder.setProducerName(txnID.toString());
            messageBuilder.setPublishTime(System.currentTimeMillis());
            PulsarApi.MessageMetadata messageMetadata = messageBuilder.build();
            ByteBuf payload = Unpooled.copiedBuffer("message-" + i + sequenceBase, StandardCharsets.UTF_8);
            ByteBuf byteBuf = serializeMetadataAndPayload(Commands.ChecksumType.Crc32c, messageMetadata, payload);

            txnMessage.add(byteBuf);
            messageBuilder.recycle();
            messageMetadata.recycle();
        });

        if (commit) {
            PulsarMarkers.MessageIdData messageIdData = PulsarMarkers.MessageIdData.newBuilder()
                                                                                   .setLedgerId(commitAtLedgerId)
                                                                                   .setEntryId(commitAtEntryId).build();
            ByteBuf commitMarker = Markers.newTxnCommitMarker(numMessage, txnID.getMostSigBits(), txnID.getLeastSigBits(),
                                                              messageIdData);
            txnMessage.add(commitMarker);
        }

        return txnMessage;
    }

    private Position writeSnapshotStartToLedger(LedgerHandle ledgerHandle, Position position) throws Exception {
        TransactionBufferDataFormats.StoredTxnIndexEntry startStore = DataFormat.newSnapshotStartEntry(position);
        long entryId = ledgerHandle.addEntry(startStore.toByteArray());
        return PositionImpl.get(ledgerHandle.getId(), entryId);
    }

    private void writeSnapshotMiddleToLedger(LedgerHandle ledgerHandle, Position position,
                                             List<TransactionMetaImpl> metaList) throws Exception {
        for (TransactionMetaImpl meta : metaList) {
            TransactionBufferDataFormats.StoredTxnIndexEntry middleStore = DataFormat
                                                                               .newSnapshotMiddleEntry(position, meta);
            ledgerHandle.addEntry(middleStore.toByteArray());
        }
    }

    private void writeSnapshotEndToLedger(LedgerHandle ledgerHandle, Position position) throws Exception {
        TransactionBufferDataFormats.StoredTxnIndexEntry endStore = DataFormat.newSnapshotEndEntry(position);
        ledgerHandle.addEntry(endStore.toByteArray());
    }

    private void writeExampleDataToLedger(LedgerHandle ledgerHandle, List<TransactionMetaImpl> metaList,
                                          Position position) throws Exception {
        Position startPos = writeSnapshotStartToLedger(ledgerHandle, position);
        writeSnapshotMiddleToLedger(ledgerHandle, startPos, metaList);
        writeSnapshotEndToLedger(ledgerHandle, startPos);
    }

    private List<TransactionMetaImpl> createExampleData(int num) {
        List<TransactionMetaImpl> metaList = new ArrayList<>(num);
        for (int i = 0; i < num; i++) {
            long mostBits = randomGenerator.nextInt(1000);
            long leastBits = randomGenerator.nextInt(1000);
            TxnID txnID = new TxnID(mostBits, leastBits);
            TransactionMetaImpl meta = new TransactionMetaImpl(txnID);
            metaList.add(meta);
        }
        return metaList;
    }

}
