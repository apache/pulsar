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
package org.apache.pulsar.broker.service;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.broker.service.persistent.PersistentMessageFinderByIndex;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.intercept.BrokerEntryMetadataInterceptor;
import org.apache.pulsar.common.intercept.BrokerEntryMetadataUtils;
import org.apache.pulsar.common.protocol.ByteBufPair;
import org.apache.pulsar.common.protocol.Commands;
import org.testng.Assert;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class PersistentMessageFinderByIndexTest extends MockedBookKeeperTestCase {

    public static ByteBuf createMessageByteBufWrittenToLedger(String msg) {
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

    public static byte[] appendBrokerMetadata(ByteBuf headerAndPayloads,
                                              Set<BrokerEntryMetadataInterceptor> interceptors) {
        ByteBuf msgWithEntryMeta = Commands.addBrokerEntryMetadata(headerAndPayloads, interceptors, 1);
        byte[] byteMessage = msgWithEntryMeta.nioBuffer().array();
        msgWithEntryMeta.release();
        return byteMessage;
    }

    CompletableFuture<Position> findMessage(PersistentMessageFinderByIndex fincer, long index) {
        final CompletableFuture<Position> future = new CompletableFuture<>();
        fincer.findMessagesByIndex(index, new AsyncCallbacks.FindEntryCallback() {
            @Override
            public void findEntryComplete(Position position, Object ctx) {
                future.complete(position);
            }

            @Override
            public void findEntryFailed(ManagedLedgerException exception,
                                        Optional<Position> failedReadPosition, Object ctx) {
                log.error("findEntryFailed, failedReadPosition:{}", failedReadPosition, exception);
                future.completeExceptionally(exception);
            }
        });
        return future;
    }

    public static Set<BrokerEntryMetadataInterceptor> getBrokerEntryMetadataInterceptors() {
        Set<String> interceptorNames = new HashSet<>();
        interceptorNames.add("org.apache.pulsar.common.intercept.AppendBrokerTimestampMetadataInterceptor");
        interceptorNames.add("org.apache.pulsar.common.intercept.AppendIndexMetadataInterceptor");
        return BrokerEntryMetadataUtils.loadBrokerEntryMetadataInterceptors(interceptorNames,
                Thread.currentThread().getContextClassLoader());
    }

    private static long parseIndex(byte[] data) {
        return Commands.parseBrokerEntryMetadataIfExist(
                UnpooledByteBufAllocator.DEFAULT.heapBuffer().writeBytes(data)).getIndex();
    }

    @Test
    public void testFindMessagesByIndex() throws Exception {
        String topicName = "testFindMessagesByIndex";

        Set<BrokerEntryMetadataInterceptor> interceptors = getBrokerEntryMetadataInterceptors();

        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setRetentionSizeInMB(10);
        config.setMaxEntriesPerLedger(2);
        ManagedLedger ledger = factory.open(topicName, config);

        ManagedCursorImpl mc = (ManagedCursorImpl) ledger.openCursor("sub-" + topicName);
        byte[] msg1 = appendBrokerMetadata(createMessageByteBufWrittenToLedger("message1"), interceptors);
        long index1 = parseIndex(msg1);
        byte[] msg2 = appendBrokerMetadata(createMessageByteBufWrittenToLedger("message2"), interceptors);
        long index2 = parseIndex(msg2);
        byte[] msg3 = appendBrokerMetadata(createMessageByteBufWrittenToLedger("message3"), interceptors);
        long index3 = parseIndex(msg3);

        Assert.assertEquals(index1, 0);
        Assert.assertEquals(index2, 1);
        Assert.assertEquals(index3, 2);

        Position p1 = ledger.addEntry(msg1);
        log.info("produce msg, pos={},index={}", p1, index1);
        Position p2 = ledger.addEntry(msg2);
        log.info("produce msg, pos={},index={}", p2, index2);
        Position p3 = ledger.addEntry(msg3);
        log.info("produce msg, pos={},index={}", p3, index3);

        PersistentMessageFinderByIndex finder;
        CompletableFuture<Position> future;

        finder = new PersistentMessageFinderByIndex(topicName, mc);
        future = findMessage(finder, -1);
        Assert.assertNull(future.get());

        finder = new PersistentMessageFinderByIndex(topicName, mc);
        future = findMessage(finder, 0);
        Assert.assertNull(future.get());

        finder = new PersistentMessageFinderByIndex(topicName, mc);
        future = findMessage(finder, 1);
        Assert.assertEquals(future.get(), p1);

        finder = new PersistentMessageFinderByIndex(topicName, mc);
        future = findMessage(finder, 2);
        Assert.assertEquals(future.get(), p2);

        finder = new PersistentMessageFinderByIndex(topicName, mc);
        future = findMessage(finder, 3);
        Assert.assertEquals(future.get(), p3);

        finder = new PersistentMessageFinderByIndex(topicName, mc);
        future = findMessage(finder, 4);
        Assert.assertEquals(future.get(), p3);
    }

    @Test
    public void testFindMessagesByIndexInEmptyLedger() throws Exception {
        String topicName = "testFindMessagesByIndexInEmptyLedger";

        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setRetentionSizeInMB(10);
        config.setMaxEntriesPerLedger(2);
        ManagedLedger ledger = factory.open(topicName, config);

        ManagedCursorImpl mc = (ManagedCursorImpl) ledger.openCursor("sub-" + topicName);

        PersistentMessageFinderByIndex finder;
        CompletableFuture<Position> future;

        finder = new PersistentMessageFinderByIndex(topicName, mc);
        future = findMessage(finder, -1);
        Assert.assertNull(future.get());

        finder = new PersistentMessageFinderByIndex(topicName, mc);
        future = findMessage(finder, 0);
        Assert.assertNull(future.get());
    }

    @Test
    public void testFindMessagesByIndexInOneEntryLedger() throws Exception {
        String topicName = "testFindMessagesByIndexInOneEntryLedger";

        Set<BrokerEntryMetadataInterceptor> interceptors = getBrokerEntryMetadataInterceptors();

        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setRetentionSizeInMB(10);
        config.setMaxEntriesPerLedger(2);
        ManagedLedger ledger = factory.open(topicName, config);

        ManagedCursorImpl mc = (ManagedCursorImpl) ledger.openCursor("sub-" + topicName);
        byte[] msg1 = appendBrokerMetadata(createMessageByteBufWrittenToLedger("message1"), interceptors);
        long index1 = parseIndex(msg1);


        Assert.assertEquals(index1, 0);

        Position p1 = ledger.addEntry(msg1);
        log.info("produce msg, pos={},index={}", p1, index1);

        PersistentMessageFinderByIndex finder;
        CompletableFuture<Position> future;

        finder = new PersistentMessageFinderByIndex(topicName, mc);
        future = findMessage(finder, -1);
        Assert.assertNull(future.get());

        finder = new PersistentMessageFinderByIndex(topicName, mc);
        future = findMessage(finder, 0);
        Assert.assertNull(future.get());

        finder = new PersistentMessageFinderByIndex(topicName, mc);
        future = findMessage(finder, 1);
        Assert.assertEquals(future.get(), p1);

        finder = new PersistentMessageFinderByIndex(topicName, mc);
        future = findMessage(finder, 2);
        Assert.assertEquals(future.get(), p1);

        finder = new PersistentMessageFinderByIndex(topicName, mc);
        future = findMessage(finder, 1000);
        Assert.assertEquals(future.get(), p1);
    }
}
