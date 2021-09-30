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
import org.apache.pulsar.common.api.raw.RawMessageImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
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

        admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(brokerUrl.toString()).build());

        // so that clients can test short names
        admin.tenants().createTenant("public",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
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

    @Test(dataProvider = "cacheSizeProvider", timeOut = 1000 * 20)
    public void cacheSizeAllocatorTest(long entryQueueSizeBytes) throws Exception {
        TopicName topicName = TopicName.get(
                "public/default/cache-size-" + entryQueueSizeBytes + "test_" + + RandomUtils.nextInt()) ;
        int totalMsgCnt = 1000;
        MessageIdImpl firstMessageId = prepareData(topicName, totalMsgCnt);

        ReadOnlyCursor readOnlyCursor = pulsar.getManagedLedgerFactory().openReadOnlyCursor(
                topicName.getPersistenceNamingEncoding(),
                PositionImpl.get(firstMessageId.getLedgerId(), firstMessageId.getEntryId()),
                new ManagedLedgerConfig());
        readOnlyCursor.skipEntries(totalMsgCnt);
        PositionImpl lastPosition = (PositionImpl) readOnlyCursor.getReadPosition();

        ObjectMapper objectMapper = new ObjectMapper();

        PulsarSplit pulsarSplit = new PulsarSplit(
                0,
                "connector-id",
                topicName.getNamespace(),
                topicName.getLocalName(),
                topicName.getLocalName(),
                totalMsgCnt,
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
        connectorConfig.setMaxSplitQueueSizeBytes(entryQueueSizeBytes);

        ConnectorContext prestoConnectorContext = new TestingConnectorContext();
        PulsarRecordCursor pulsarRecordCursor = new PulsarRecordCursor(
                pulsarColumnHandles, pulsarSplit, connectorConfig, pulsar.getManagedLedgerFactory(),
                new ManagedLedgerConfig(), new PulsarConnectorMetricsTracker(new NullStatsProvider()),
                new PulsarDispatchingRowDecoderFactory(prestoConnectorContext.getTypeManager()));

        Class<PulsarRecordCursor> recordCursorClass = PulsarRecordCursor.class;
        Field entryQueueField = recordCursorClass.getDeclaredField("entryQueue");
        entryQueueField.setAccessible(true);
        SpscArrayQueue<Entry> entryQueue = (SpscArrayQueue<Entry>) entryQueueField.get(pulsarRecordCursor);

        Field messageQueueField = recordCursorClass.getDeclaredField("messageQueue");
        messageQueueField.setAccessible(true);
        SpscArrayQueue<RawMessageImpl> messageQueue =
                (SpscArrayQueue<RawMessageImpl>) messageQueueField.get(pulsarRecordCursor);

        long maxQueueSize = 0;
        if (entryQueueSizeBytes == -1) {
            maxQueueSize = Long.MAX_VALUE;
        } else if (entryQueueSizeBytes == 0) {
            maxQueueSize = 1;
        } else if (entryQueueSizeBytes > 0) {
            maxQueueSize = entryQueueSizeBytes / 2 / singleEntrySize + 1;
        }

        int receiveCnt = 0;
        while (receiveCnt != totalMsgCnt) {
            if (pulsarRecordCursor.advanceNextPosition()) {
                receiveCnt ++;
            }
            Assert.assertTrue(entryQueue.size() <= maxQueueSize);
            Assert.assertTrue(messageQueue.size() <= maxQueueSize);
        }
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
