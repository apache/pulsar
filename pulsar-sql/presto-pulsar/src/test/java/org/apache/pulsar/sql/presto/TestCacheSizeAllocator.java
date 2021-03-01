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
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.stats.NullStatsProvider;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.jctools.queues.SpscArrayQueue;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;

@Slf4j
public class TestCacheSizeAllocator extends MockedPulsarServiceBaseTest {

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

    @Test
    public void cacheSizeAllocatorTest() throws Exception {
        TopicName topicName = TopicName.get("public/default/cache-size-test");
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topicName.toString())
                .create();

        int entrySize = 100;
        MessageIdImpl firstMessageId = null;
        MessageIdImpl lastMessageId = null;
        for (int i = 0; i < entrySize; i++) {
            MessageIdImpl messageId = (MessageIdImpl) producer.send(generateBytesData(100));
            if (firstMessageId == null) {
                firstMessageId = messageId;
            }
            lastMessageId = messageId;
        }

        ObjectMapper objectMapper = new ObjectMapper();

        PulsarSplit pulsarSplit = new PulsarSplit(
                0,
                "connector-id",
                topicName.getNamespace(),
                topicName.getLocalName(),
                topicName.getLocalName(),
                1,
                new String(Schema.BYTES.getSchemaInfo().getSchema()),
                Schema.BYTES.getSchemaInfo().getType(),
                firstMessageId.getEntryId(),
                lastMessageId.getEntryId(),
                firstMessageId.getLedgerId(),
                lastMessageId.getLedgerId(),
                TupleDomain.all(),
                objectMapper.writeValueAsString(new HashMap<>()),
                null);

        List<PulsarColumnHandle> pulsarColumnHandles = TestPulsarConnector.getColumnColumnHandles(
                topicName, Schema.BYTES.getSchemaInfo(), PulsarColumnHandle.HandleKeyValueType.NONE, true);

        PulsarConnectorConfig connectorConfig = new PulsarConnectorConfig();

        ConnectorContext prestoConnectorContext = new TestingConnectorContext();
        PulsarRecordCursor pulsarRecordCursor = new PulsarRecordCursor(
                pulsarColumnHandles, pulsarSplit, connectorConfig, pulsar.getManagedLedgerFactory(),
                new ManagedLedgerConfig(), new PulsarConnectorMetricsTracker(new NullStatsProvider()),
                new PulsarDispatchingRowDecoderFactory(prestoConnectorContext.getTypeManager()));

        Class<PulsarRecordCursor> recordCursorClass = PulsarRecordCursor.class;
        Field entryQueueField = recordCursorClass.getDeclaredField("entryQueue");

        for (int i = 0; i < entrySize; i++) {
            pulsarRecordCursor.advanceNextPosition();
            SpscArrayQueue queue = (SpscArrayQueue) entryQueueField.get(pulsarRecordCursor);
            log.info("queue size: {}", queue.size());
        }
    }

    private byte[] generateBytesData(int size) {
        byte[] bytes = new byte[size];
        for (int i = 0; i < size; i++) {
            bytes[i] = 1;
        }
        return bytes;
    }

}
