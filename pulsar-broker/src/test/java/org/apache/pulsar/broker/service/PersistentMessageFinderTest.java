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
package org.apache.pulsar.broker.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.AsyncCallbacks.MarkDeleteCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo.LedgerInfo;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.service.persistent.PersistentMessageExpiryMonitor;
import org.apache.pulsar.broker.service.persistent.PersistentMessageFinder;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.ResetCursorData;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.proto.BrokerEntryMetadata;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.intercept.BrokerEntryMetadataInterceptor;
import org.apache.pulsar.common.intercept.BrokerEntryMetadataUtils;
import org.apache.pulsar.common.protocol.ByteBufPair;
import org.apache.pulsar.common.protocol.Commands;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

@Test(groups = "broker")
@Slf4j
public class PersistentMessageFinderTest extends MockedBookKeeperTestCase {

    public static byte[] createMessageWrittenToLedger(String msg) {
        return createMessageWrittenToLedger(msg, System.currentTimeMillis());
    }
    public static byte[] createMessageWrittenToLedger(String msg, long messageTimestamp) {
        MessageMetadata messageMetadata = new MessageMetadata()
                    .setPublishTime(messageTimestamp)
                    .setProducerName("createMessageWrittenToLedger")
                    .setSequenceId(1);
        ByteBuf data = UnpooledByteBufAllocator.DEFAULT.heapBuffer().writeBytes(msg.getBytes());

        int msgMetadataSize = messageMetadata.getSerializedSize();
        int payloadSize = data.readableBytes();
        int totalSize = 4 + msgMetadataSize + payloadSize;

        ByteBuf headers = PulsarByteBufAllocator.DEFAULT.heapBuffer(totalSize, totalSize);
        headers.writeInt(msgMetadataSize);
        messageMetadata.writeTo(headers);
        ByteBuf headersAndPayload = ByteBufPair.coalesce(ByteBufPair.get(headers, data));
        byte[] byteMessage = new byte[headersAndPayload.readableBytes()];
        headersAndPayload.readBytes(byteMessage);
        headersAndPayload.release();
        return byteMessage;
    }

    public static ByteBuf createMessageByteBufWrittenToLedger(String msg) throws Exception {
        MessageMetadata messageMetadata = new MessageMetadata()
                .setPublishTime(System.currentTimeMillis())
                .setProducerName("createMessageWrittenToLedger")
                .setSequenceId(1);
        ByteBuf data = UnpooledByteBufAllocator.DEFAULT.heapBuffer().writeBytes(msg.getBytes());

        int msgMetadataSize = messageMetadata.getSerializedSize();
        int payloadSize = data.readableBytes();
        int totalSize = 4 + msgMetadataSize + payloadSize;

        ByteBuf headers = PulsarByteBufAllocator.DEFAULT.heapBuffer(totalSize, totalSize);
        headers.writeInt(msgMetadataSize);
        messageMetadata.writeTo(headers);
        return ByteBufPair.coalesce(ByteBufPair.get(headers, data));
    }

    public static byte[] appendBrokerTimestamp(ByteBuf headerAndPayloads) throws Exception {
        ByteBuf msgWithEntryMeta =
                Commands.addBrokerEntryMetadata(headerAndPayloads, getBrokerEntryMetadataInterceptors(), 1);
        byte[] byteMessage = new byte[msgWithEntryMeta.readableBytes()];
        msgWithEntryMeta.readBytes(byteMessage);
        msgWithEntryMeta.release();
        return byteMessage;
    }

    static class Result {
        ManagedLedgerException exception = null;
        Position position = null;

        void reset() {
            this.exception = null;
            this.position = null;
        }
    }

    CompletableFuture<Void> findMessage(final Result result, final ManagedCursor c1, final long timestamp) {
        return findMessage(result, c1, timestamp, 0);
    }

    CompletableFuture<Void> findMessage(final Result result, final ManagedCursor c1, final long timestamp,
                                        int ledgerCloseTimestampMaxClockSkewMillis) {
        PersistentMessageFinder messageFinder = new PersistentMessageFinder("topicname", c1, ledgerCloseTimestampMaxClockSkewMillis);

        final CompletableFuture<Void> future = new CompletableFuture<>();
        messageFinder.findMessages(timestamp, new AsyncCallbacks.FindEntryCallback() {
            @Override
            public void findEntryComplete(Position position, Object ctx) {
                result.position = position;
                future.complete(null);
            }

            @Override
            public void findEntryFailed(ManagedLedgerException exception, Optional<Position> failedReadPosition,
                    Object ctx) {
                result.exception = exception;
                future.completeExceptionally(exception);
            }
        });
        return future;
    }

    @Test
    void testPersistentMessageFinder() throws Exception {
        final String ledgerAndCursorName = "testPersistentMessageFinder";
        int entriesPerLedger = 2;
        long beginTimestamp = System.currentTimeMillis();

        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setRetentionSizeInMB(10);
        config.setMaxEntriesPerLedger(entriesPerLedger);
        config.setRetentionTime(1, TimeUnit.HOURS);
        ManagedLedger ledger = factory.open(ledgerAndCursorName, config);
        ManagedCursorImpl c1 = (ManagedCursorImpl) ledger.openCursor(ledgerAndCursorName);

        ledger.addEntry(createMessageWrittenToLedger("retained1"));
        // space apart message publish times
        Thread.sleep(100);
        ledger.addEntry(createMessageWrittenToLedger("retained2"));
        Thread.sleep(100);
        Position newPosition = ledger.addEntry(createMessageWrittenToLedger("retained3"));
        Thread.sleep(100);
        long timestamp = System.currentTimeMillis();
        Thread.sleep(10);

        ledger.addEntry(createMessageWrittenToLedger("afterresetposition"));

        Position lastPosition = ledger.addEntry(createMessageWrittenToLedger("not-read"));
        List<Entry> entries = c1.readEntries(3);
        c1.markDelete(entries.get(2).getPosition());
        c1.close();
        ledger.close();
        entries.forEach(Entry::release);
        // give timed ledger trimming a chance to run
        Thread.sleep(1000);

        ledger = factory.open(ledgerAndCursorName, config);
        c1 = (ManagedCursorImpl) ledger.openCursor(ledgerAndCursorName);
        long endTimestamp = System.currentTimeMillis();

        Result result = new Result();

        CompletableFuture<Void> future = findMessage(result, c1, timestamp);
        future.get();
        assertNull(result.exception);
        assertNotNull(result.position);
        assertEquals(result.position, newPosition);

        result.reset();
        future = findMessage(result, c1, beginTimestamp);
        future.get();
        assertNull(result.exception);
        assertNull(result.position);

        result.reset();
        future = findMessage(result, c1, endTimestamp);
        future.get();
        assertNull(result.exception);
        assertNotEquals(result.position, null);
        assertEquals(result.position, lastPosition);

        PersistentMessageFinder messageFinder = new PersistentMessageFinder("topicname", c1, 0);
        final AtomicBoolean ex = new AtomicBoolean(false);
        messageFinder.findEntryFailed(new ManagedLedgerException("failed"), Optional.empty(),
                new AsyncCallbacks.FindEntryCallback() {
                    @Override
                    public void findEntryComplete(Position position, Object ctx) {
                    }

                    @Override
                    public void findEntryFailed(ManagedLedgerException exception, Optional<Position> failedReadPosition,
                            Object ctx) {
                        ex.set(true);
                    }
                });
        assertTrue(ex.get());

        PersistentMessageExpiryMonitor monitor = new PersistentMessageExpiryMonitor("topicname", c1.getName(), c1, null);
        monitor.findEntryFailed(new ManagedLedgerException.ConcurrentFindCursorPositionException("failed"),
                Optional.empty(), null);
        Field field = monitor.getClass().getDeclaredField("expirationCheckInProgress");
        field.setAccessible(true);
        assertEquals(0, field.get(monitor));

        result.reset();
        c1.close();
        ledger.close();
        factory.shutdown();
    }

    @Test
    void testPersistentMessageFinderWhenLastMessageDelete() throws Exception {
        final String ledgerAndCursorName = "testPersistentMessageFinderWhenLastMessageDelete";

        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setRetentionSizeInMB(10);
        config.setMaxEntriesPerLedger(10);
        config.setRetentionTime(1, TimeUnit.HOURS);
        ManagedLedger ledger = factory.open(ledgerAndCursorName, config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor(ledgerAndCursorName);

        ledger.addEntry(createMessageWrittenToLedger("msg1"));
        ledger.addEntry(createMessageWrittenToLedger("msg2"));
        ledger.addEntry(createMessageWrittenToLedger("msg3"));
        Position lastPosition = ledger.addEntry(createMessageWrittenToLedger("last-message"));

        long endTimestamp = System.currentTimeMillis() + 1000;

        Result result = new Result();
        // delete last position message
        cursor.delete(lastPosition);
        CompletableFuture<Void> future = findMessage(result, cursor, endTimestamp);
        future.get();
        assertNull(result.exception);
        assertNotEquals(result.position, null);
        assertEquals(result.position, lastPosition);

        result.reset();
        cursor.close();
        ledger.close();
        factory.shutdown();
    }

    @Test
    void testPersistentMessageFinderWithBrokerTimestampForMessage() throws Exception {

        final String ledgerAndCursorName = "publishTime";
        final String ledgerAndCursorNameForBrokerTimestampMessage = "brokerTimestamp";

        ManagedLedgerConfig config = new ManagedLedgerConfig();
        ManagedLedger ledger = factory.open(ledgerAndCursorName, config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor(ledgerAndCursorName);
        ledger.addEntry(createMessageWrittenToLedger("message1"));
        // space apart message publish times
        Thread.sleep(100);
        ledger.addEntry(createMessageWrittenToLedger("message2"));
        Thread.sleep(100);
        Position position = ledger.addEntry(createMessageWrittenToLedger("message3"));
        Thread.sleep(100);
        long timestamp = System.currentTimeMillis();

        Result result = new Result();

        CompletableFuture<Void> future = findMessage(result, cursor, timestamp);
        future.get();
        assertNull(result.exception);
        assertNotNull(result.position);
        assertEquals(result.position, position);

        List<Entry> entryList = cursor.readEntries(3);
        for (Entry entry : entryList) {
            // entry has no raw metadata if BrokerTimestampForMessage is disable
            assertNull(Commands.parseBrokerEntryMetadataIfExist(entry.getDataBuffer()));
        }

        result.reset();
        cursor.close();
        ledger.close();

        ManagedLedgerConfig configNew = new ManagedLedgerConfig();
        ManagedLedger ledgerNew = factory.open(ledgerAndCursorNameForBrokerTimestampMessage, configNew);
        ManagedCursorImpl cursorNew = (ManagedCursorImpl) ledgerNew.openCursor(ledgerAndCursorNameForBrokerTimestampMessage);
        // build message which has publish time first
        ByteBuf msg1 = createMessageByteBufWrittenToLedger("message1");
        ByteBuf msg2 = createMessageByteBufWrittenToLedger("message2");
        ByteBuf msg3 = createMessageByteBufWrittenToLedger("message3");
        Thread.sleep(10);
        long timeAfterPublishTime = System.currentTimeMillis();
        Thread.sleep(10);

        // append broker timestamp as entry metadata

        ledgerNew.addEntry(appendBrokerTimestamp(msg1));
        // space apart message publish times
        Thread.sleep(100);
        ledgerNew.addEntry(appendBrokerTimestamp(msg2));
        Thread.sleep(100);
        Position newPosition = ledgerNew.addEntry(appendBrokerTimestamp(msg3));
        Thread.sleep(100);
        long timeAfterBrokerTimestamp = System.currentTimeMillis();


        CompletableFuture<Void> publishTimeFuture = findMessage(result, cursorNew, timeAfterPublishTime);
        publishTimeFuture.get();
        assertNull(result.exception);
        // position should be null, since broker timestamp for message is bigger than timeAfterPublishTime
        assertNull(result.position);

        result.reset();

        CompletableFuture<Void> brokerTimestampFuture = findMessage(result, cursorNew, timeAfterBrokerTimestamp);
        brokerTimestampFuture.get();
        assertNull(result.exception);
        assertNotNull(result.position);
        assertEquals(result.position, newPosition);

        List<Entry> entryListNew = cursorNew.readEntries(4);
        for (Entry entry : entryListNew) {
            // entry should have raw metadata since BrokerTimestampForMessage is enable
            BrokerEntryMetadata brokerMetadata = Commands.parseBrokerEntryMetadataIfExist(entry.getDataBuffer());
            assertNotNull(brokerMetadata);
            assertTrue(brokerMetadata.getBrokerTimestamp() > timeAfterPublishTime);
        }

        result.reset();
        cursorNew.close();
        ledgerNew.close();
        factory.shutdown();
    }

    public static Set<BrokerEntryMetadataInterceptor> getBrokerEntryMetadataInterceptors() {
        Set<String> interceptorNames = new HashSet<>();
        interceptorNames.add("org.apache.pulsar.common.intercept.AppendBrokerTimestampMetadataInterceptor");
        interceptorNames.add("org.apache.pulsar.common.intercept.AppendIndexMetadataInterceptor");
        return BrokerEntryMetadataUtils.loadBrokerEntryMetadataInterceptors(interceptorNames,
                Thread.currentThread().getContextClassLoader());
    }
    /**
     * It tests that message expiry doesn't get stuck if it can't read deleted ledger's entry.
     *
     * @throws Exception
     */
    @Test
    void testMessageExpiryWithTimestampNonRecoverableException() throws Exception {

        final String ledgerAndCursorName = "testPersistentMessageExpiryWithNonRecoverableLedgers";
        final int entriesPerLedger = 2;
        final int totalEntries = 10;
        final int ttlSeconds = 1;

        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setRetentionSizeInMB(10);
        config.setMaxEntriesPerLedger(entriesPerLedger);
        config.setRetentionTime(1, TimeUnit.HOURS);
        config.setAutoSkipNonRecoverableData(true);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open(ledgerAndCursorName, config);
        ManagedCursorImpl c1 = (ManagedCursorImpl) ledger.openCursor(ledgerAndCursorName);

        for (int i = 0; i < totalEntries; i++) {
            ledger.addEntry(createMessageWrittenToLedger("msg" + i));
        }
        Awaitility.await().untilAsserted(() ->
                assertEquals(ledger.getState(), ManagedLedgerImpl.State.LedgerOpened));

        List<LedgerInfo> ledgers = ledger.getLedgersInfoAsList();
        LedgerInfo lastLedgerInfo = ledgers.get(ledgers.size() - 1);
        // The `lastLedgerInfo` should be newly opened, and it does not contain any entries.
        // Please refer to: https://github.com/apache/pulsar/pull/22034
        assertEquals(lastLedgerInfo.getEntries(), 0);
        assertEquals(ledgers.size(), totalEntries / entriesPerLedger + 1);

        // this will make sure that all entries should be deleted
        Thread.sleep(TimeUnit.SECONDS.toMillis(ttlSeconds));

        bkc.deleteLedger(ledgers.get(0).getLedgerId());
        bkc.deleteLedger(ledgers.get(1).getLedgerId());
        bkc.deleteLedger(ledgers.get(2).getLedgerId());

        PersistentMessageExpiryMonitor monitor = new PersistentMessageExpiryMonitor("topicname", c1.getName(), c1, null);
        assertTrue(monitor.expireMessages(ttlSeconds));
        Awaitility.await().untilAsserted(() -> {
            PositionImpl markDeletePosition = (PositionImpl) c1.getMarkDeletedPosition();
            // The markDeletePosition points to the last entry of the previous ledger in lastLedgerInfo.
            assertEquals(markDeletePosition.getLedgerId(), lastLedgerInfo.getLedgerId() - 1);
            assertEquals(markDeletePosition.getEntryId(), entriesPerLedger - 1);
        });

        c1.close();
        ledger.close();
        factory.shutdown();

    }

    public void testFindMessageWithTimestampAutoSkipNonRecoverable() throws Exception {

        final String ledgerAndCursorName = "testFindMessageWithTimestampAutoSkipNonRecoverable";
        final int entriesPerLedger = 5;
        final int totalEntries = 50;

        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setRetentionSizeInMB(10);
        config.setMaxEntriesPerLedger(entriesPerLedger);
        config.setRetentionTime(1, TimeUnit.HOURS);
        config.setAutoSkipNonRecoverableData(true);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open(ledgerAndCursorName, config);

        long initTimeMillis = System.currentTimeMillis();
        for (int i = 0; i < totalEntries; i++) {
            ledger.addEntry(createMessageWrittenToLedger("msg" + i, initTimeMillis + i));
        }
        // {0,1,2,3,4} (0) 3 x
        // {5,6,7,8,9} (1) 4
        // {10,11,12,13,14} (2) 5
        // {15,16,17,18,19} (3) 6
        // {20,21,22,23,24} (4) 7 x
        // {25,26,27,28,29} (5) 8 x
        // {30,31,32,33,34} (6) 9
        // {35,36,37,38,39} (7) 10
        // {40,41,42,43,44} (8) 11
        // {45,46,47,48,49} (9) 12 x
        Awaitility.await().untilAsserted(() ->
                assertEquals(ledger.getState(), ManagedLedgerImpl.State.LedgerOpened));

        List<LedgerInfo> ledgers = ledger.getLedgersInfoAsList();
        LedgerInfo lastLedgerInfo = ledgers.get(ledgers.size() - 1);
        // The `lastLedgerInfo` should be newly opened, and it does not contain any entries.
        // Please refer to: https://github.com/apache/pulsar/pull/22034
        assertEquals(lastLedgerInfo.getEntries(), 0);
        assertEquals(ledgers.size(), totalEntries / entriesPerLedger + 1);

        bkc.deleteLedger(ledgers.get(0).getLedgerId());
        bkc.deleteLedger(ledgers.get(4).getLedgerId());
        bkc.deleteLedger(ledgers.get(5).getLedgerId());
        bkc.deleteLedger(ledgers.get(9).getLedgerId());

        MessageId messageId = findMessageIdByPublishTime(initTimeMillis + 17, ledger).join();
        log.info("messageId: {}", messageId);
        assertEquals(messageId, new MessageIdImpl(ledgers.get(3).getLedgerId(), 2, -1));

        messageId = findMessageIdByPublishTime(initTimeMillis + 27, ledger).join();
        log.info("messageId: {}", messageId);
        assertEquals(messageId, new MessageIdImpl(ledgers.get(4).getLedgerId(), 0, -1));

        messageId = findMessageIdByPublishTime(initTimeMillis + 43, ledger).join();
        log.info("messageId: {}", messageId);
        assertEquals(messageId, new MessageIdImpl(ledgers.get(8).getLedgerId(), 3, -1));

        messageId = findMessageIdByPublishTime(initTimeMillis + 48, ledger).join();
        log.info("messageId: {}", messageId);
        assertEquals(messageId, new MessageIdImpl(ledgers.get(9).getLedgerId(), 0, -1));

        ledger.close();
        factory.shutdown();
    }

    public void testFindMessageByCursorWithTimestampAutoSkipNonRecoverable() throws Exception {

        final String ledgerAndCursorName = "testFindMessageByCursorWithTimestampAutoSkipNonRecoverable";
        final int entriesPerLedger = 5;
        final int totalEntries = 50;

        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setRetentionSizeInMB(10);
        config.setMaxEntriesPerLedger(entriesPerLedger);
        config.setRetentionTime(1, TimeUnit.HOURS);
        config.setAutoSkipNonRecoverableData(true);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open(ledgerAndCursorName, config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor(ledgerAndCursorName);

        long initTimeMillis = System.currentTimeMillis();
        for (int i = 0; i < totalEntries; i++) {
            ledger.addEntry(createMessageWrittenToLedger("msg" + i, initTimeMillis + i));
        }
        // {0,1,2,3,4} (0) 3 x
        // {5,6,7,8,9} (1) 4
        // {10,11,12,13,14} (2) 5
        // {15,16,17,18,19} (3) 6
        // {20,21,22,23,24} (4) 7 x
        // {25,26,27,28,29} (5) 8 x
        // {30,31,32,33,34} (6) 9
        // {35,36,37,38,39} (7) 10
        // {40,41,42,43,44} (8) 11
        // {45,46,47,48,49} (9) 12 x
        Awaitility.await().untilAsserted(() ->
                assertEquals(ledger.getState(), ManagedLedgerImpl.State.LedgerOpened));

        List<LedgerInfo> ledgers = ledger.getLedgersInfoAsList();
        LedgerInfo lastLedgerInfo = ledgers.get(ledgers.size() - 1);
        // The `lastLedgerInfo` should be newly opened, and it does not contain any entries.
        // Please refer to: https://github.com/apache/pulsar/pull/22034
        assertEquals(lastLedgerInfo.getEntries(), 0);
        assertEquals(ledgers.size(), totalEntries / entriesPerLedger + 1);

        bkc.deleteLedger(ledgers.get(0).getLedgerId());
        bkc.deleteLedger(ledgers.get(4).getLedgerId());
        bkc.deleteLedger(ledgers.get(5).getLedgerId());
        bkc.deleteLedger(ledgers.get(9).getLedgerId());
        Result result = new Result();

        findMessage(result, cursor, initTimeMillis + 17, -1).join();
        log.info("position: {}", result.position);
        assertNull(result.exception);
        assertEquals(result.position, PositionImpl.get(ledgers.get(3).getLedgerId(), 1));

        result = new Result();
        findMessage(result, cursor, initTimeMillis + 27, -1).join();
        log.info("position: {}", result.position);
        assertNull(result.exception);
        assertEquals(result.position, PositionImpl.get(ledgers.get(3).getLedgerId(), 4));

        result = new Result();
        findMessage(result, cursor, initTimeMillis + 43, -1).join();
        log.info("position: {}", result.position);
        assertNull(result.exception);
        assertEquals(result.position, PositionImpl.get(ledgers.get(8).getLedgerId(), 2));

        ledger.close();
        factory.shutdown();
    }

    private CompletableFuture<MessageId> findMessageIdByPublishTime(long timestamp, ManagedLedger managedLedger) {
        return managedLedger.asyncFindPosition(entry -> {
            try {
                long entryTimestamp = Commands.getEntryTimestamp(entry.getDataBuffer());
                return MessageImpl.isEntryPublishedEarlierThan(entryTimestamp, timestamp);
            } catch (Exception e) {
                log.error("Error deserializing message for message position find", e);
            } finally {
                entry.release();
            }
            return false;
        }).thenApply(position -> {
            if (position == null) {
                return null;
            } else {
                return new MessageIdImpl(position.getLedgerId(), position.getEntryId(), -1);
            }
        });
    }

    @Test
    public void testIncorrectClientClock() throws Exception {
        final String ledgerAndCursorName = "testIncorrectClientClock";
        int maxTTLSeconds = 1;
        int entriesNum = 10;
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(1);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open(ledgerAndCursorName, config);
        ManagedCursorImpl c1 = (ManagedCursorImpl) ledger.openCursor(ledgerAndCursorName);
        // set client clock to 10 days later
        long incorrectPublishTimestamp = System.currentTimeMillis() + TimeUnit.DAYS.toMillis(10);
        for (int i = 0; i < entriesNum; i++) {
            ledger.addEntry(createMessageWrittenToLedger("msg" + i, incorrectPublishTimestamp));
        }
        Awaitility.await().untilAsserted(() ->
                assertEquals(ledger.getState(), ManagedLedgerImpl.State.LedgerOpened));
        // The number of ledgers should be (entriesNum / MaxEntriesPerLedger) + 1
        // Please refer to: https://github.com/apache/pulsar/pull/22034
        assertEquals(ledger.getLedgersInfoAsList().size(), entriesNum + 1);
        PersistentMessageExpiryMonitor monitor = new PersistentMessageExpiryMonitor("topicname", c1.getName(), c1, null);
        Thread.sleep(TimeUnit.SECONDS.toMillis(maxTTLSeconds));
        monitor.expireMessages(maxTTLSeconds);
        assertEquals(c1.getNumberOfEntriesInBacklog(true), 0);
    }

    @Test
    public void testCheckExpiryByLedgerClosureTimeWithAckUnclosedLedger() throws Throwable {
        final String ledgerAndCursorName = "testCheckExpiryByLedgerClosureTimeWithAckUnclosedLedger";
        int maxTTLSeconds = 1;
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(5);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open(ledgerAndCursorName, config);
        ManagedCursorImpl c1 = (ManagedCursorImpl) ledger.openCursor(ledgerAndCursorName);
        // set client clock to 10 days later
        long incorrectPublishTimestamp = System.currentTimeMillis() + TimeUnit.DAYS.toMillis(10);
        for (int i = 0; i < 7; i++) {
            ledger.addEntry(createMessageWrittenToLedger("msg" + i, incorrectPublishTimestamp));
        }
        assertEquals(ledger.getLedgersInfoAsList().size(), 2);
        PersistentMessageExpiryMonitor monitor =
                spy(new PersistentMessageExpiryMonitor("topicname", c1.getName(), c1, null));
        MarkDeleteCallback markDeleteCallback = mock(MarkDeleteCallback.class);
        doReturn(markDeleteCallback).when(monitor).getMarkDeleteCallback(any());
        PositionImpl position = (PositionImpl) ledger.getLastConfirmedEntry();
        c1.markDelete(position);
        Thread.sleep(TimeUnit.SECONDS.toMillis(maxTTLSeconds));
        monitor.expireMessages(maxTTLSeconds);
        assertEquals(c1.getNumberOfEntriesInBacklog(true), 0);
        verify(markDeleteCallback, times(0)).markDeleteFailed(any(), any());
    }

    @Test
    void testMessageExpiryWithPosition() throws Exception {
        final String ledgerAndCursorName = "testPersistentMessageExpiryWithPositionNonRecoverableLedgers";
        final int entriesPerLedger = 5;
        final int totalEntries = 30;
        List<Position> positions = new ArrayList<>();

        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setRetentionSizeInMB(10);
        config.setMaxEntriesPerLedger(entriesPerLedger);
        config.setRetentionTime(1, TimeUnit.HOURS);
        config.setAutoSkipNonRecoverableData(true);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open(ledgerAndCursorName, config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor(ledgerAndCursorName);

        PersistentSubscription subscription = mock(PersistentSubscription.class);
        Topic topic = mock(Topic.class);
        when(subscription.getTopic()).thenReturn(topic);

        for (int i = 0; i < totalEntries; i++) {
            positions.add(ledger.addEntry(createMessageWrittenToLedger("msg" + i)));
        }
        when(topic.getLastPosition()).thenReturn(positions.get(positions.size() - 1));

        PersistentMessageExpiryMonitor monitor = spy(new PersistentMessageExpiryMonitor("topicname",
                cursor.getName(), cursor, subscription));
        assertEquals(cursor.getMarkDeletedPosition(), PositionImpl.get(positions.get(0).getLedgerId(), -1));
        boolean issued;

        // Expire by position and verify mark delete position of cursor.
        issued = monitor.expireMessages(positions.get(15));
        Awaitility.await().untilAsserted(() -> verify(monitor, times(1)).findEntryComplete(any(), any()));
        assertEquals(cursor.getMarkDeletedPosition(), PositionImpl.get(positions.get(15).getLedgerId(), positions.get(15).getEntryId()));
        assertTrue(issued);
        clearInvocations(monitor);

        // Expire by position beyond last position and nothing should happen.
        issued = monitor.expireMessages(PositionImpl.get(100, 100));
        assertEquals(cursor.getMarkDeletedPosition(), PositionImpl.get(positions.get(15).getLedgerId(), positions.get(15).getEntryId()));
        assertFalse(issued);

        // Expire by position again and verify mark delete position of cursor didn't change.
        issued = monitor.expireMessages(positions.get(15));
        Awaitility.await().untilAsserted(() -> verify(monitor, times(1)).findEntryComplete(any(), any()));
        assertEquals(cursor.getMarkDeletedPosition(), PositionImpl.get(positions.get(15).getLedgerId(), positions.get(15).getEntryId()));
        assertTrue(issued);
        clearInvocations(monitor);

        // Expire by position before current mark delete position and verify mark delete position of cursor didn't change.
        issued = monitor.expireMessages(positions.get(10));
        Awaitility.await().untilAsserted(() -> verify(monitor, times(1)).findEntryComplete(any(), any()));
        assertEquals(cursor.getMarkDeletedPosition(), PositionImpl.get(positions.get(15).getLedgerId(), positions.get(15).getEntryId()));
        assertTrue(issued);
        clearInvocations(monitor);

        // Expire by position after current mark delete position and verify mark delete position of cursor move to new position.
        issued = monitor.expireMessages(positions.get(16));
        Awaitility.await().untilAsserted(() -> verify(monitor, times(1)).findEntryComplete(any(), any()));
        assertEquals(cursor.getMarkDeletedPosition(), PositionImpl.get(positions.get(16).getLedgerId(), positions.get(16).getEntryId()));
        assertTrue(issued);
        clearInvocations(monitor);

        ManagedCursorImpl mockCursor = mock(ManagedCursorImpl.class);
        PersistentMessageExpiryMonitor mockMonitor = spy(new PersistentMessageExpiryMonitor("topicname",
                cursor.getName(), mockCursor, subscription));
        // Not calling findEntryComplete to clear expirationCheckInProgress condition, so following call to
        // expire message shouldn't issue.
        doAnswer(invocation -> null).when(mockCursor).asyncFindNewestMatching(any(), any(), any(), any());
        issued = mockMonitor.expireMessages(positions.get(15));
        assertTrue(issued);
        issued = mockMonitor.expireMessages(positions.get(15));
        assertFalse(issued);

        cursor.close();
        ledger.close();
        factory.shutdown();
    }

    @Test
    public void test() {
        ResetCursorData resetCursorData = new ResetCursorData(1, 1);
        resetCursorData.setExcluded(true);
        System.out.println(Entity.entity(resetCursorData, MediaType.APPLICATION_JSON));
    }

    @Test
    public void testGetFindPositionRange_EmptyLedgerInfos() {
        List<LedgerInfo> ledgerInfos = new ArrayList<>();
        PositionImpl lastConfirmedEntry = null;
        long targetTimestamp = 2000;
        Pair<PositionImpl, PositionImpl> range =
                PersistentMessageFinder.getFindPositionRange(ledgerInfos, lastConfirmedEntry, targetTimestamp, 0);

        assertNotNull(range);
        assertNull(range.getLeft());
        assertNull(range.getRight());
    }

    @Test
    public void testGetFindPositionRange_AllTimestampsLessThanTarget() {
        List<LedgerInfo> ledgerInfos = new ArrayList<>();
        ledgerInfos.add(LedgerInfo.newBuilder().setLedgerId(1).setEntries(10).setTimestamp(1000).build());
        ledgerInfos.add(LedgerInfo.newBuilder().setLedgerId(2).setEntries(10).setTimestamp(1500).build());
        PositionImpl lastConfirmedEntry = new PositionImpl(2, 9);

        long targetTimestamp = 2000;
        Pair<PositionImpl, PositionImpl> range = PersistentMessageFinder.getFindPositionRange(ledgerInfos,
                lastConfirmedEntry, targetTimestamp, 0);

        assertNotNull(range);
        assertNotNull(range.getLeft());
        assertNull(range.getRight());
        assertEquals(range.getLeft(), new PositionImpl(2, 9));
    }

    @Test
    public void testGetFindPositionRange_LastTimestampIsZero() {
        List<LedgerInfo> ledgerInfos = new ArrayList<>();
        ledgerInfos.add(LedgerInfo.newBuilder().setLedgerId(1).setEntries(10).setTimestamp(1000).build());
        ledgerInfos.add(LedgerInfo.newBuilder().setLedgerId(2).setEntries(10).setTimestamp(1500).build());
        ledgerInfos.add(LedgerInfo.newBuilder().setLedgerId(3).setEntries(10).setTimestamp(0).build());
        PositionImpl lastConfirmedEntry = new PositionImpl(3, 5);

        long targetTimestamp = 2000;
        Pair<PositionImpl, PositionImpl> range = PersistentMessageFinder.getFindPositionRange(ledgerInfos,
                lastConfirmedEntry, targetTimestamp, 0);

        assertNotNull(range);
        assertNotNull(range.getLeft());
        assertNull(range.getRight());
        assertEquals(range.getLeft(), new PositionImpl(2, 9));
    }

    @Test
    public void testGetFindPositionRange_LastTimestampIsZeroWithNoEntries() {
        List<LedgerInfo> ledgerInfos = new ArrayList<>();
        ledgerInfos.add(LedgerInfo.newBuilder().setLedgerId(1).setEntries(10).setTimestamp(1000).build());
        ledgerInfos.add(LedgerInfo.newBuilder().setLedgerId(2).setEntries(10).setTimestamp(1500).build());
        ledgerInfos.add(LedgerInfo.newBuilder().setLedgerId(3).setEntries(10).setTimestamp(0).build());
        PositionImpl lastConfirmedEntry = new PositionImpl(2, 9);

        long targetTimestamp = 2000;
        Pair<PositionImpl, PositionImpl> range = PersistentMessageFinder.getFindPositionRange(ledgerInfos,
                lastConfirmedEntry, targetTimestamp, 0);

        assertNotNull(range);
        assertNotNull(range.getLeft());
        assertNull(range.getRight());
        assertEquals(range.getLeft(), new PositionImpl(2, 9));
    }

    @Test
    public void testGetFindPositionRange_AllTimestampsGreaterThanTarget() {
        List<LedgerInfo> ledgerInfos = new ArrayList<>();
        ledgerInfos.add(LedgerInfo.newBuilder().setLedgerId(1).setEntries(10).setTimestamp(3000).build());
        ledgerInfos.add(LedgerInfo.newBuilder().setLedgerId(2).setEntries(10).setTimestamp(4000).build());
        PositionImpl lastConfirmedEntry = new PositionImpl(2, 9);

        long targetTimestamp = 2000;
        Pair<PositionImpl, PositionImpl> range = PersistentMessageFinder.getFindPositionRange(ledgerInfos,
                lastConfirmedEntry, targetTimestamp, 0);

        assertNotNull(range);
        assertNull(range.getLeft());
        assertNotNull(range.getRight());
        assertEquals(range.getRight(), new PositionImpl(1, 9));
    }

    @Test
    public void testGetFindPositionRange_MixedTimestamps() {
        List<LedgerInfo> ledgerInfos = new ArrayList<>();
        ledgerInfos.add(LedgerInfo.newBuilder().setLedgerId(1).setEntries(10).setTimestamp(1000).build());
        ledgerInfos.add(LedgerInfo.newBuilder().setLedgerId(2).setEntries(10).setTimestamp(2000).build());
        ledgerInfos.add(LedgerInfo.newBuilder().setLedgerId(3).setEntries(10).setTimestamp(3000).build());
        PositionImpl lastConfirmedEntry = new PositionImpl(3, 9);

        long targetTimestamp = 2500;
        Pair<PositionImpl, PositionImpl> range = PersistentMessageFinder.getFindPositionRange(ledgerInfos,
                lastConfirmedEntry, targetTimestamp, 0);

        assertNotNull(range);
        assertNotNull(range.getLeft());
        assertNotNull(range.getRight());
        assertEquals(range.getLeft(), new PositionImpl(2, 9));
        assertEquals(range.getRight(), new PositionImpl(3, 9));
    }

    @Test
    public void testGetFindPositionRange_TimestampAtBoundary() {
        List<LedgerInfo> ledgerInfos = new ArrayList<>();
        ledgerInfos.add(LedgerInfo.newBuilder().setLedgerId(1).setEntries(10).setTimestamp(1000).build());
        ledgerInfos.add(LedgerInfo.newBuilder().setLedgerId(2).setEntries(10).setTimestamp(2000).build());
        ledgerInfos.add(LedgerInfo.newBuilder().setLedgerId(3).setEntries(10).setTimestamp(3000).build());
        ledgerInfos.add(LedgerInfo.newBuilder().setLedgerId(4).setEntries(10).setTimestamp(4000).build());
        PositionImpl lastConfirmedEntry = new PositionImpl(4, 9);

        long targetTimestamp = 3000;
        Pair<PositionImpl, PositionImpl> range = PersistentMessageFinder.getFindPositionRange(ledgerInfos,
                lastConfirmedEntry, targetTimestamp, 0);

        assertNotNull(range);
        assertNotNull(range.getLeft());
        assertNotNull(range.getRight());
        assertEquals(range.getLeft(), new PositionImpl(3, 9));
        // there might be entries in the next ledger with the same timestamp as the target timestamp, even though
        // the close timestamp of ledger 3 is equals to the target timestamp
        assertEquals(range.getRight(), new PositionImpl(4, 9));
    }

    @Test
    public void testGetFindPositionRange_ClockSkew() {
        List<LedgerInfo> ledgerInfos = new ArrayList<>();
        ledgerInfos.add(LedgerInfo.newBuilder().setLedgerId(1).setEntries(10).setTimestamp(1000).build());
        ledgerInfos.add(LedgerInfo.newBuilder().setLedgerId(2).setEntries(10).setTimestamp(2000).build());
        ledgerInfos.add(LedgerInfo.newBuilder().setLedgerId(3).setEntries(10).setTimestamp(2010).build());
        ledgerInfos.add(LedgerInfo.newBuilder().setLedgerId(4).setEntries(10).setTimestamp(4000).build());
        ledgerInfos.add(LedgerInfo.newBuilder().setLedgerId(5).setTimestamp(0).build());
        PositionImpl lastConfirmedEntry = new PositionImpl(5, 5);

        long targetTimestamp = 2009;
        Pair<PositionImpl, PositionImpl> range = PersistentMessageFinder.getFindPositionRange(ledgerInfos,
                lastConfirmedEntry, targetTimestamp, 10);

        assertNotNull(range);
        assertNotNull(range.getLeft());
        assertNotNull(range.getRight());
        assertEquals(range.getLeft(), new PositionImpl(1, 9));
        assertEquals(range.getRight(), new PositionImpl(4, 9));
    }

    @Test
    public void testGetFindPositionRange_ClockSkewCase2() {
        List<LedgerInfo> ledgerInfos = new ArrayList<>();
        ledgerInfos.add(LedgerInfo.newBuilder().setLedgerId(1).setEntries(10).setTimestamp(1000).build());
        ledgerInfos.add(LedgerInfo.newBuilder().setLedgerId(2).setEntries(10).setTimestamp(2000).build());
        ledgerInfos.add(LedgerInfo.newBuilder().setLedgerId(3).setEntries(10).setTimestamp(3000).build());
        ledgerInfos.add(LedgerInfo.newBuilder().setLedgerId(4).setEntries(10).setTimestamp(4000).build());
        ledgerInfos.add(LedgerInfo.newBuilder().setLedgerId(5).setTimestamp(0).build());
        PositionImpl lastConfirmedEntry = new PositionImpl(5, 5);

        long targetTimestamp = 2995;
        Pair<PositionImpl, PositionImpl> range = PersistentMessageFinder.getFindPositionRange(ledgerInfos,
                lastConfirmedEntry, targetTimestamp, 10);

        assertNotNull(range);
        assertNotNull(range.getLeft());
        assertNotNull(range.getRight());
        assertEquals(range.getLeft(), new PositionImpl(2, 9));
        assertEquals(range.getRight(), new PositionImpl(4, 9));
    }

    @Test
    public void testGetFindPositionRange_ClockSkewCase3() {
        List<LedgerInfo> ledgerInfos = new ArrayList<>();
        ledgerInfos.add(LedgerInfo.newBuilder().setLedgerId(1).setEntries(10).setTimestamp(1000).build());
        ledgerInfos.add(LedgerInfo.newBuilder().setLedgerId(2).setEntries(10).setTimestamp(2000).build());
        ledgerInfos.add(LedgerInfo.newBuilder().setLedgerId(3).setEntries(10).setTimestamp(3000).build());
        ledgerInfos.add(LedgerInfo.newBuilder().setLedgerId(4).setEntries(10).setTimestamp(4000).build());
        ledgerInfos.add(LedgerInfo.newBuilder().setLedgerId(5).setTimestamp(0).build());
        PositionImpl lastConfirmedEntry = new PositionImpl(5, 5);

        long targetTimestamp = 3005;
        Pair<PositionImpl, PositionImpl> range = PersistentMessageFinder.getFindPositionRange(ledgerInfos,
                lastConfirmedEntry, targetTimestamp, 10);

        assertNotNull(range);
        assertNotNull(range.getLeft());
        assertNotNull(range.getRight());
        assertEquals(range.getLeft(), new PositionImpl(2, 9));
        assertEquals(range.getRight(), new PositionImpl(4, 9));
    }

    @Test
    public void testGetFindPositionRange_FeatureDisabledWithNegativeClockSkew() {
        List<LedgerInfo> ledgerInfos = new ArrayList<>();
        ledgerInfos.add(LedgerInfo.newBuilder().setLedgerId(1).setEntries(10).setTimestamp(1000).build());
        ledgerInfos.add(LedgerInfo.newBuilder().setLedgerId(2).setEntries(10).setTimestamp(2000).build());
        ledgerInfos.add(LedgerInfo.newBuilder().setLedgerId(3).setEntries(10).setTimestamp(2010).build());
        ledgerInfos.add(LedgerInfo.newBuilder().setLedgerId(4).setEntries(10).setTimestamp(4000).build());
        ledgerInfos.add(LedgerInfo.newBuilder().setLedgerId(5).setTimestamp(0).build());
        PositionImpl lastConfirmedEntry = new PositionImpl(5, 5);

        long targetTimestamp = 2009;
        Pair<PositionImpl, PositionImpl> range = PersistentMessageFinder.getFindPositionRange(ledgerInfos,
                lastConfirmedEntry, targetTimestamp, -1);

        assertNotNull(range);
        assertNull(range.getLeft());
        assertNull(range.getRight());
    }

    @Test
    public void testGetFindPositionRange_SingleLedger() {
        List<LedgerInfo> ledgerInfos = new ArrayList<>();
        ledgerInfos.add(LedgerInfo.newBuilder().setLedgerId(1).setTimestamp(0).build());
        PositionImpl lastConfirmedEntry = new PositionImpl(1, 5);

        long targetTimestamp = 2500;
        Pair<PositionImpl, PositionImpl> range = PersistentMessageFinder.getFindPositionRange(ledgerInfos,
                lastConfirmedEntry, targetTimestamp, 0);

        assertNotNull(range);
        assertNull(range.getLeft());
        assertNull(range.getRight());
    }

    @Test
    public void testGetFindPositionRange_SingleClosedLedger() {
        List<LedgerInfo> ledgerInfos = new ArrayList<>();
        ledgerInfos.add(LedgerInfo.newBuilder().setLedgerId(1).setEntries(10).setTimestamp(1000).build());
        PositionImpl lastConfirmedEntry = new PositionImpl(1, 9);

        long targetTimestamp = 2500;
        Pair<PositionImpl, PositionImpl> range = PersistentMessageFinder.getFindPositionRange(ledgerInfos,
                lastConfirmedEntry, targetTimestamp, 0);

        assertNotNull(range);
        assertNotNull(range.getLeft());
        assertNull(range.getRight());
        assertEquals(range.getLeft(), new PositionImpl(1, 9));
    }
}
