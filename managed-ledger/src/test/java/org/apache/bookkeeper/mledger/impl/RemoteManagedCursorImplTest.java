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
package org.apache.bookkeeper.mledger.impl;


import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.client.api.CursorClient;
import org.apache.pulsar.client.api.CursorClientBuilder;
import org.apache.pulsar.client.api.CursorData;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.cursor.CursorDataImpl;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.api.proto.CursorPosition;
import org.apache.pulsar.common.api.proto.EntryPosition;
import org.apache.pulsar.common.api.proto.LongProperty;
import org.apache.pulsar.common.naming.TopicName;
import org.testng.Assert;
import org.testng.annotations.Test;


@Slf4j
public class RemoteManagedCursorImplTest extends MockedBookKeeperTestCase {

    @Test(timeOut = 5000)
    public void testReadEntries() throws Exception {
        String sub = "sub-testReadEntries";

        TopicName writerTopic = TopicName.get("persistent://test/writer/testReadEntries");
        ManagedLedger wml = factory.open(writerTopic.getPersistenceNamingEncoding());
        wml.addEntry("data1".getBytes());
        wml.addEntry("data2".getBytes());

        factory.setClientSupplier(() -> mockClient(wml));

        TopicName readerTopic = TopicName.get("persistent://test/readonly/testReadEntries");
        RemoteManagedLedgerImpl rml = RemoteManagedLedgerImplTest.openRemoteManagedLedger(factory,
                readerTopic, writerTopic);

        ManagedCursor mc = rml.openCursor(sub, CommandSubscribe.InitialPosition.Earliest);
        List<Entry> entries = mc.readEntries(2);
        Assert.assertEquals(entries.size(), 2);
        Assert.assertEquals(entries.get(0).getData(), "data1".getBytes());
        Assert.assertEquals(entries.get(1).getData(), "data2".getBytes());
        entries.forEach(Entry::release);
    }

    @Test(timeOut = 5000)
    public void testReadMultiLedgers() throws Exception {
        String sub = "sub-testReadMultiLedgers";

        TopicName writerTopic = TopicName.get("persistent://test/writer/testReadMultiLedgers");
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(1);
        ManagedLedger wml = factory.open(writerTopic.getPersistenceNamingEncoding(), config);
        wml.openCursor("test-sub", CommandSubscribe.InitialPosition.Earliest);
        Position p1 = wml.addEntry("data1".getBytes());
        Position p2 = wml.addEntry("data2".getBytes());
        log.info("p1={},p2={}", p1, p2);
        Assert.assertNotEquals(p1.getLedgerId(), p2.getLedgerId());

        factory.setClientSupplier(() -> mockClient(wml));

        TopicName readerTopic = TopicName.get("persistent://test/readonly/testReadMultiLedgers");
        RemoteManagedLedgerImpl rml = RemoteManagedLedgerImplTest.openRemoteManagedLedger(factory,
                readerTopic, writerTopic);

        ManagedCursor mc = rml.openCursor(sub, CommandSubscribe.InitialPosition.Earliest);
        log.info("lac={}", rml.lastConfirmedEntry);

        List<Entry> entries = mc.readEntries(2);
        Assert.assertEquals(entries.size(), 2);
        Assert.assertEquals(entries.get(0).getData(), "data1".getBytes());
        Assert.assertEquals(entries.get(1).getData(), "data2".getBytes());
        entries.forEach(Entry::release);
    }

    @Test(timeOut = 5000)
    public void testRemoteCreateCursor() throws Exception {
        String sub = "sub-testRemoteCreateCursor";

        TopicName writerTopic = TopicName.get("persistent://test/writer/testRemoteCreateCursor");
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(1);
        ManagedLedger wml = factory.open(writerTopic.getPersistenceNamingEncoding(), config);

        factory.setClientSupplier(() -> mockClient(wml));

        TopicName readerTopic = TopicName.get("persistent://test/readonly/testRemoteCreateCursor");
        RemoteManagedLedgerImpl rml = RemoteManagedLedgerImplTest.openRemoteManagedLedger(factory,
                readerTopic, writerTopic);

        ManagedCursor mc = rml.openCursor(sub, CommandSubscribe.InitialPosition.Earliest);
        log.info("lac={}", rml.lastConfirmedEntry);
        Assert.assertFalse(mc.hasMoreEntries());
        List<Entry> entries = mc.readEntries(2);
        Assert.assertEquals(entries.size(), 0);

        entries = mc.readEntries(2);
        Assert.assertEquals(entries.size(), 0);

        for (byte i = 0; i < 10; i++) {
            byte[] data = new byte[]{i};
            wml.addEntry(data);
            entries = mc.readEntriesOrWait(1);
            Assert.assertEquals(entries.size(), 1);
            Assert.assertEquals(entries.get(0).getDataAndRelease(), data);
        }
    }

    @Test(timeOut = 5000)
    public void testRemoteGetCursor() throws Exception {
        String sub = "sub-testRemoteGetCursor";

        TopicName writerTopic = TopicName.get("persistent://test/writer/testRemoteGetCursor");
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(2);
        ManagedLedger wml = factory.open(writerTopic.getPersistenceNamingEncoding(), config);
        ManagedCursor writerCursor = wml.openCursor(sub);
        for (int i = 0; i < 5; i++) {
            Position pos = wml.addEntry(("data" + i).getBytes());
            log.info("write {} pos={}", i, pos);
        }
        //read 1 msg from writer ML
        List<Entry> entries = writerCursor.readEntries(1);
        Assert.assertEquals(entries.size(), 1);
        writerCursor.markDelete(entries.get(0).getPosition());
        Assert.assertEquals(entries.get(0).getDataAndRelease(), "data0".getBytes());

        factory.setClientSupplier(() -> mockClient(wml));

        TopicName readerTopic = TopicName.get("persistent://test/readonly/testRemoteGetCursor");
        RemoteManagedLedgerImpl rml = RemoteManagedLedgerImplTest.openRemoteManagedLedger(factory,
                readerTopic, writerTopic);
        log.info("lac={}", rml.lastConfirmedEntry);

        ManagedCursor mc = rml.openCursor(sub, CommandSubscribe.InitialPosition.Earliest);
        log.info("md={}", mc.getMarkDeletedPosition());

        //read rest msgs from reader ML
        for (byte i = 1; i < 5; i++) {
            entries = mc.readEntriesOrWait(1);
            Assert.assertEquals(entries.size(), 1);
            Assert.assertEquals(entries.get(0).getDataAndRelease(), ("data" + i).getBytes());
        }
    }

    @Test(timeOut = 5000)
    public void testRemoteUpdateCursor() throws Exception {
        String sub = "sub-testRemoteUpdateCursor";
        List<Entry> entries;
        TopicName writerTopic = TopicName.get("persistent://test/writer/testRemoteUpdateCursor");
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(2);
        ManagedLedger wml = factory.open(writerTopic.getPersistenceNamingEncoding(), config);


        factory.setClientSupplier(() -> mockClient(wml));

        TopicName readerTopic = TopicName.get("persistent://test/readonly/testRemoteUpdateCursor");
        RemoteManagedLedgerImpl rml = RemoteManagedLedgerImplTest.openRemoteManagedLedger(factory,
                readerTopic, writerTopic);
        log.info("lac={}", rml.lastConfirmedEntry);

        ManagedCursor mc = rml.openCursor(sub, CommandSubscribe.InitialPosition.Earliest);
        log.info("md={}", mc.getMarkDeletedPosition());

        for (int i = 0; i < 5; i++) {
            Position pos = wml.addEntry(("data" + i).getBytes());
            log.info("addEntry {} pos={}", i, pos);
        }

        //read rest msgs from reader ML
        for (byte i = 0; i < 2; i++) {
            entries = mc.readEntriesOrWait(1);

            Assert.assertEquals(entries.size(), 1);
            Entry entry = entries.get(0);

            Map<String, Long> properties = new HashMap<>();
            properties.put("offset", (long) i);
            mc.markDelete(entry.getPosition(), properties);
            Assert.assertEquals(entry.getData(), ("data" + i).getBytes());
            entry.release();
        }

        ManagedCursor writerCursor = wml.openCursor(sub);
        log.info("writer md={}", writerCursor.getMarkDeletedPosition());
        Map<String, Long> properties = writerCursor.getProperties();
        Long offset = properties.get("offset");
        //read 1 msg from writer ML
        entries = writerCursor.readEntries(1);
        Assert.assertEquals(entries.size(), 1);
        Assert.assertEquals(entries.get(0).getDataAndRelease(), ("data" + (offset + 1)).getBytes());
    }

    @Test(timeOut = 5000)
    public void testRemoteDeleteCursor() throws Exception {
        String sub = "sub-testRemoteDeleteCursor";
        List<Entry> entries;
        TopicName writerTopic = TopicName.get("persistent://test/writer/testRemoteDeleteCursor");
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(2);
        ManagedLedger wml = factory.open(writerTopic.getPersistenceNamingEncoding(), config);

        factory.setClientSupplier(() -> mockClient(wml));

        TopicName readerTopic = TopicName.get("persistent://test/readonly/testRemoteDeleteCursor");
        RemoteManagedLedgerImpl rml = RemoteManagedLedgerImplTest.openRemoteManagedLedger(factory,
                readerTopic, writerTopic);

        ManagedCursor mc = rml.openCursor(sub, CommandSubscribe.InitialPosition.Earliest);
        log.info("md={}", mc.getMarkDeletedPosition());

        Assert.assertNotNull(wml.getCursor(sub));

        rml.deleteCursor(sub);

        Assert.assertNull(wml.getCursor(sub));
    }


    private PulsarClient mockClient(ManagedLedger wml) {
        log.info("mocking client");
        CursorClientBuilderForTest builder = new CursorClientBuilderForTest();
        builder.setWriterManagedLedger(wml);

        PulsarClient client = mock(PulsarClient.class);
        when(client.newCursorClient()).thenReturn(builder);
        return client;
    }

    public static class CursorClientBuilderForTest implements CursorClientBuilder {
        String topic;
        ManagedLedger writerManagedLedger;

        public void setWriterManagedLedger(ManagedLedger writerManagedLedger) {
            this.writerManagedLedger = writerManagedLedger;
        }

        @Override
        public CursorClientBuilder topic(String topicName) {
            this.topic = topicName;
            return this;
        }

        @Override
        public CursorClient create() throws PulsarClientException {
            return null;
        }

        @Override
        public CompletableFuture<CursorClient> createAsync() {
            CursorClientForTest cursorClient = new CursorClientForTest();
            cursorClient.setWriterManagedLedger(writerManagedLedger);
            return CompletableFuture.completedFuture(cursorClient);
        }
    }

    public static class CursorClientForTest implements CursorClient {
        ManagedLedger writerManagedLedger;

        public void setWriterManagedLedger(ManagedLedger writerManagedLedger) {
            this.writerManagedLedger = writerManagedLedger;
        }

        @Override
        public String getTopic() {
            return null;
        }

        @Override
        public CompletableFuture<CursorData> getCursorAsync(String subscription) {
            log.info("CursorConsumerForTest.getCursorAsync called, sub={}", subscription);
            ManagedCursor cursor = writerManagedLedger.getCursor(subscription);
            Position lac = writerManagedLedger.getLastConfirmedEntry();
            CursorDataImpl result = new CursorDataImpl(writerManagedLedger.getVersion(),
                    new EntryPosition().setLedgerId(lac.getLedgerId()).setEntryId(lac.getEntryId()),
                    cursor.getCursorPosition());
            log.info("Sub {} CursorConsumerForTest.getCursorAsync returned {}", subscription, result);
            return CompletableFuture.completedFuture(result);
        }

        @Override
        public CompletableFuture<CursorData> createCursorAsync(String subscription, CursorData cursorData) {
            log.info("CursorConsumerForTest.createCursorAsync called,sub={}, cursorData={}", subscription, cursorData);
            CursorPosition position = ((CursorDataImpl) cursorData).getPosition();
            CommandSubscribe.InitialPosition initPosition = CommandSubscribe.InitialPosition.Earliest;
            if (new PositionImpl(position).equals(PositionImpl.latest)) {
                initPosition = CommandSubscribe.InitialPosition.Latest;
            }
            Map<String, Long> properties = position.getPropertiesList().stream().collect(
                    Collectors.toMap(LongProperty::getName, LongProperty::getValue));
            CompletableFuture<CursorData> future = new CompletableFuture<>();
            writerManagedLedger.asyncOpenCursor(subscription,
                    initPosition, properties, new AsyncCallbacks.OpenCursorCallback() {

                        @Override
                        public void openCursorComplete(ManagedCursor cursor, Object ctx) {
                            CursorPosition position = cursor.getCursorPosition();
                            Position lac = writerManagedLedger.getLastConfirmedEntry();
                            CursorDataImpl ret = new CursorDataImpl(writerManagedLedger.getVersion(),
                                    new EntryPosition().setLedgerId(lac.getLedgerId()).setEntryId(lac.getEntryId()),
                                    position);
                            log.info("Sub {} CursorConsumerForTest.createCursorAsync returned {}", subscription, ret);
                            future.complete(ret);
                        }

                        @Override
                        public void openCursorFailed(ManagedLedgerException exception, Object ctx) {
                            log.info("Sub {} CursorConsumerForTest.createCursorAsync returned exception", subscription,
                                    exception);
                            future.completeExceptionally(exception);
                        }
                    }, null);
            return future;
        }

        @Override
        public CompletableFuture<Void> deleteCursorAsync(String subscription) {
            log.info("CursorConsumerForTest.deleteCursorAsync called, sub={}", subscription);
            CompletableFuture<Void> future = new CompletableFuture<>();
            writerManagedLedger.asyncDeleteCursor(subscription, new AsyncCallbacks.DeleteCursorCallback() {
                @Override
                public void deleteCursorComplete(Object ctx) {
                    log.info("Sub {} CursorConsumerForTest.deleteCursorAsync success", subscription);
                    future.complete(null);
                }

                @Override
                public void deleteCursorFailed(ManagedLedgerException exception, Object ctx) {
                    log.error("Sub {} CursorConsumerForTest.deleteCursorAsync failed", subscription, exception);
                    future.completeExceptionally(exception);
                }
            }, null);
            return future;
        }

        @Override
        public CompletableFuture<Void> updateCursorAsync(String subscription, CursorData cursorData) {
            log.info("CursorConsumerForTest.updateCursorAsync called, sub={}, cursorData={}", subscription, cursorData);
            ManagedCursor cursor = writerManagedLedger.getCursor(subscription);
            return cursor.updateCursorPositionAsync(((CursorDataImpl) cursorData).getPosition()).thenAccept(v -> {
                log.info("Sub {} CursorConsumerForTest.updateCursorAsync success", subscription);
            }).exceptionally(throwable -> {
                log.error("Sub {} CursorConsumerForTest.updateCursorAsync failed", subscription, throwable);
                return null;
            });
        }

        @Override
        public void close() throws PulsarClientException {

        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            return null;
        }
    }

}