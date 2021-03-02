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
package org.apache.pulsar.sql.presto;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import io.prestosql.spi.connector.ConnectorContext;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.testing.TestingConnectorContext;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ReadOnlyCursor;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.stats.NullStatsProvider;
import org.apache.commons.lang3.RandomUtils;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.sql.presto.util.NullCacheSizeAllocator;
import org.jctools.queues.SpscArrayQueue;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Test cache size allocator.
 */
@Slf4j
public class TestCacheSizeAllocator extends MockedPulsarServiceBaseTest {

    private final int singleEntrySize = 500;

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();

        admin.clusters().createCluster("test", new ClusterData(brokerUrl.toString()));

        // so that clients can test short names
        admin.tenants().createTenant("public",
                new TenantInfo(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace("public/default");
        admin.namespaces().setNamespaceReplicationClusters("public/default", Sets.newHashSet("test"));
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @DataProvider(name = "cacheSizeProvider")
    private Object[][] dataProvider() {
        return new Object[][] {
                {-1}, {0}, {2000}, {5000}
        };
    }

    @Test(dataProvider = "cacheSizeProvider")
    public void cacheSizeAllocatorTest(long entryQueueSizeBytes) throws Exception {
        TopicName topicName = TopicName.get(
                "public/default/cache-size-" + entryQueueSizeBytes + "test_" + + RandomUtils.nextInt()) ;
        int splitSize = 21;
        MessageIdImpl firstMessageId = prepareData(topicName, splitSize);

        PositionImpl lastPosition = null;
        ReadOnlyCursor readOnlyCursor = pulsar.getManagedLedgerFactory().openReadOnlyCursor(
                topicName.getPersistenceNamingEncoding(),
                PositionImpl.get(firstMessageId.getLedgerId(), firstMessageId.getEntryId()),
                new ManagedLedgerConfig());
        readOnlyCursor.skipEntries(splitSize);
        lastPosition = (PositionImpl) readOnlyCursor.getReadPosition();

        ObjectMapper objectMapper = new ObjectMapper();

        PulsarSplit pulsarSplit = new PulsarSplit(
                0,
                "connector-id",
                topicName.getNamespace(),
                topicName.getLocalName(),
                topicName.getLocalName(),
                splitSize,
                new String(Schema.BYTES.getSchemaInfo().getSchema()),
                Schema.BYTES.getSchemaInfo().getType(),
                firstMessageId.getEntryId(),
                lastPosition.getEntryId(),
                firstMessageId.getLedgerId(),
                lastPosition.getLedgerId(),
                TupleDomain.all(),
                objectMapper.writeValueAsString(new HashMap<>()),
                null);

        List<PulsarColumnHandle> pulsarColumnHandles = TestPulsarConnector.getColumnColumnHandles(
                topicName, Schema.BYTES.getSchemaInfo(), PulsarColumnHandle.HandleKeyValueType.NONE, true);

        PulsarConnectorConfig connectorConfig = new PulsarConnectorConfig();
        connectorConfig.setMaxSplitEntryQueueSizeBytes(entryQueueSizeBytes);

        ConnectorContext prestoConnectorContext = new TestingConnectorContext();
        PulsarRecordCursor pulsarRecordCursor = new PulsarRecordCursor(
                pulsarColumnHandles, pulsarSplit, connectorConfig, pulsar.getManagedLedgerFactory(),
                new ManagedLedgerConfig(), new PulsarConnectorMetricsTracker(new NullStatsProvider()),
                new PulsarDispatchingRowDecoderFactory(prestoConnectorContext.getTypeManager()));

        Class<PulsarRecordCursor> recordCursorClass = PulsarRecordCursor.class;
        Field entryQueueField = recordCursorClass.getDeclaredField("entryQueue");
        entryQueueField.setAccessible(true);
        SpscArrayQueue<Entry> queue = (SpscArrayQueue<Entry>) entryQueueField.get(pulsarRecordCursor);

        Field entriesProcessedField = recordCursorClass.getDeclaredField("entriesProcessed");
        entriesProcessedField.setAccessible(true);

        Class<PulsarRecordCursor.ReadEntries> readEntriesClass = PulsarRecordCursor.ReadEntries.class;
        Field readEntriesField = recordCursorClass.getDeclaredField("readEntries");
        readEntriesField.setAccessible(true);
        PulsarRecordCursor.ReadEntries readEntries = (PulsarRecordCursor.ReadEntries)
                readEntriesClass.getDeclaredConstructors()[0].newInstance(pulsarRecordCursor);
        readEntriesField.set(pulsarRecordCursor, readEntries);

        Class<PulsarRecordCursor.DeserializeEntries> deserializeEntriesClass =
                PulsarRecordCursor.DeserializeEntries.class;
        Field deserializeEntriesField = recordCursorClass.getDeclaredField("deserializeEntries");
        deserializeEntriesField.setAccessible(true);
        PulsarRecordCursor.DeserializeEntries deserializeEntries = (PulsarRecordCursor.DeserializeEntries)
                deserializeEntriesClass.getDeclaredConstructors()[0].newInstance(pulsarRecordCursor);
        deserializeEntriesField.set(pulsarRecordCursor, deserializeEntries);

        Field isRunningField = deserializeEntriesClass.getDeclaredField("isRunning");
        isRunningField.setAccessible(true);

        Field outstandingReadsRequestsField = readEntriesClass.getDeclaredField("outstandingReadsRequests");
        outstandingReadsRequestsField.setAccessible(true);
        AtomicLong outstandingReadRequests = (AtomicLong) outstandingReadsRequestsField.get(readEntries);

        int receiveNum = 0;

        readEntries.run();
        if (entryQueueSizeBytes == -1) {
            // use NullCacheSizeAllocator
            checkQueueSize(outstandingReadRequests, queue, splitSize);
            deserializeEntries(queue, isRunningField, deserializeEntries);
        } else {
            // first read only one entry
            checkQueueSize(outstandingReadRequests, queue, 1);
            receiveNum += 1;
            deserializeEntries(queue, isRunningField, deserializeEntries);

            while (receiveNum != splitSize) {
                long maxQueueSize = entryQueueSizeBytes / (singleEntrySize + 46) + 1;
                if (maxQueueSize > (splitSize - receiveNum)) {
                    maxQueueSize = splitSize - receiveNum;
                }

                int num = readAndCheck(readEntries, outstandingReadRequests, queue, (int) maxQueueSize);
                receiveNum += num;
                deserializeEntries(queue, isRunningField, deserializeEntries);
            }
        }

        while (pulsarRecordCursor.advanceNextPosition()) {
            for (int i = 0; i < pulsarColumnHandles.size(); i++) {
                if (pulsarColumnHandles.get(i).getName().equals("__value__")) {
                    Assert.assertEquals(pulsarRecordCursor.getSlice(i).getBytes().length, 500);
                }
            }
        }

        Assert.assertTrue(readEntries.hasFinished());
        readEntries.run();
        checkQueueSize(outstandingReadRequests, queue, 0);
    }

    private int readAndCheck(PulsarRecordCursor.ReadEntries readEntries,
                             AtomicLong outstandingReadsRequests,
                             SpscArrayQueue<Entry> queue,
                             int maxQueueSize) throws Exception {
        int receiveNum = 0;

        // read twice to make sure queue size reach max queue size
        readEntries.run();
        waitReadComplete(outstandingReadsRequests);
        readEntries.run();
        checkQueueSize(outstandingReadsRequests, queue, maxQueueSize);
        receiveNum += maxQueueSize;

        // read again to make sure don't read any more entries
        readEntries.run();
        checkQueueSize(outstandingReadsRequests, queue, maxQueueSize);
        return receiveNum;
    }

    private void checkQueueSize(AtomicLong outstandingReadRequests,
                                SpscArrayQueue<Entry> queue, int expectedNum) throws Exception {
        waitReadComplete(outstandingReadRequests);
        Assert.assertEquals(queue.size(), expectedNum);
    }

    private void waitReadComplete(AtomicLong outstandingReadRequests) throws InterruptedException {
        while (outstandingReadRequests.get() < 1) {
            Thread.sleep(500);
        }
    }

    private void deserializeEntries(SpscArrayQueue<Entry> queue,
                                    Field isRunningField,
                                    PulsarRecordCursor.DeserializeEntries deserializeEntries) throws Exception {
        new Thread(deserializeEntries::run).start();
        while (queue.size() > 0) {
            Thread.sleep(100);
        }
        isRunningField.set(deserializeEntries, false);
    }

    private MessageIdImpl prepareData(TopicName topicName, int messageNum) throws Exception {
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topicName.toString())
                .create();

        MessageIdImpl firstMessageId = null;
        for (int i = 0; i < messageNum; i++) {
            MessageIdImpl messageId = (MessageIdImpl) producer.send(new byte[singleEntrySize]);
            if (firstMessageId == null) {
                firstMessageId = messageId;
            }
        }
        return firstMessageId;
    }

}
