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
package org.apache.pulsar.sql.presto;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.Sets;
import io.airlift.slice.Slice;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.predicate.TupleDomain;
import io.trino.testing.TestingConnectorContext;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.stats.NullStatsProvider;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.admin.LongRunningProcessStatus;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.schema.SchemaType;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
public class TestCompactedQuery extends MockedPulsarServiceBaseTest {

    @BeforeClass
    @Override
    public void setup() throws Exception {
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
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    static class Stock {
        private String no;
        private Double price;
    }

    @DataProvider(name = "compactInfoProvider")
    public Object[][] compactInfoProvider() {
        return new Object[][]{
                {0, 0},
                {0, 100},
                {100, 0},
                {100, 100},
        };
    }

    @Test(dataProvider = "compactInfoProvider")
    public void compactQueryForBatchMessages(int compactedMsgNum, int unCompactedMsgNum) throws Exception {
        compactQuery(true, compactedMsgNum, unCompactedMsgNum);
    }

    @Test(dataProvider = "compactInfoProvider")
    public void compactQueryForNonBatchMessages(int compactedMsgNum, int unCompactedMsgNum) throws Exception {
        compactQuery(false, compactedMsgNum, unCompactedMsgNum);
    }

    private void compactQuery(boolean enableBatch, int compactedMsgNum, int unCompactedMsgNum) throws Exception {
        TopicName topicName = TopicName.get(
                TopicDomain.persistent.toString(), "public", "default",
                RandomStringUtils.randomAlphabetic(5));

        pulsarClient.newConsumer(Schema.AVRO(Stock.class))
                .topic(topicName.toString())
                .readCompacted(true)
                .subscriptionName("sub")
                .subscribe()
                .close();

        Producer<Stock> producer = pulsarClient.newProducer(Schema.AVRO(Stock.class))
                .topic(topicName.toString())
                .enableBatching(enableBatch)
                .batchingMaxMessages(5)
                .batchingMaxPublishDelay(1, TimeUnit.SECONDS)
                .create();

        Map<String, Double> latestPrice = new HashMap<>();
        AtomicReference<MessageIdImpl> firstMessageId = new AtomicReference<>((MessageIdImpl) MessageId.earliest);
        AtomicReference<MessageIdImpl> lastMessageId = new AtomicReference<>((MessageIdImpl) MessageId.latest);
        int divisor = 8;

        prepareData(producer, compactedMsgNum, divisor, latestPrice, firstMessageId, lastMessageId);

        admin.topics().triggerCompaction(topicName.toString());
        Awaitility.await().until(() -> {
            LongRunningProcessStatus status = admin.topics().compactionStatus(topicName.toString());
            return Objects.equals(LongRunningProcessStatus.Status.SUCCESS, status.status);
        });

        Set<String> entrySet =
                prepareData(producer, unCompactedMsgNum, divisor, latestPrice, firstMessageId, lastMessageId);
        if (compactedMsgNum > 0 || unCompactedMsgNum > 0) {
            assertEquals(divisor, latestPrice.size());
        }
        log.info("finish to prepare compaction data");

        ObjectMapper objectMapper = new ObjectMapper();
        PulsarSplit pulsarSplit = new PulsarSplit(
                0,
                "connector-id",
                topicName.getNamespace(),
                topicName.getLocalName(),
                topicName.getLocalName(),
                entrySet.size(),
                new String(Schema.AVRO(Stock.class).getSchemaInfo().getSchema()),
                SchemaType.AVRO,
                firstMessageId.get().getEntryId(),
                lastMessageId.get().getEntryId(),
                firstMessageId.get().getLedgerId(),
                lastMessageId.get().getLedgerId(),
                TupleDomain.all(),
                objectMapper.writeValueAsString(new HashMap<>()),
                null,
                true);

        List<PulsarColumnHandle> pulsarColumnHandles = TestPulsarConnector.getColumnColumnHandles(
                topicName, Schema.AVRO(Stock.class).getSchemaInfo(), PulsarColumnHandle.HandleKeyValueType.NONE, true);

        PulsarConnectorConfig connectorConfig = new PulsarConnectorConfig();
        connectorConfig.setBrokerServiceUrl(admin.getServiceUrl());
        connectorConfig.setMetadataUrl("zk:localhost:2181");
        ConnectorContext prestoConnectorContext = new TestingConnectorContext();
        PulsarRecordCursor pulsarRecordCursor = new PulsarRecordCursor(
                pulsarColumnHandles, pulsarSplit, connectorConfig, pulsar.getManagedLedgerFactory(),
                new ManagedLedgerConfig(), new PulsarConnectorMetricsTracker(new NullStatsProvider()),
                new PulsarDispatchingRowDecoderFactory(prestoConnectorContext.getTypeManager()),
                this.pulsarTestContext.getMockBookKeeper());

        List<PulsarColumnHandle> columns = pulsarRecordCursor.getColumnHandles();
        while (pulsarRecordCursor.advanceNextPosition()) {
            String name = null;
            String key = null;
            Double price = null;
            for (int i = 0; i < columns.size(); i++) {
                if (columns.get(i).getName().equalsIgnoreCase("no")) {
                    Slice slice = pulsarRecordCursor.getSlice(i);
                    name = slice == null ? null : new String(slice.getBytes());
                } else if (columns.get(i).getName().equalsIgnoreCase("price")) {
                    price = pulsarRecordCursor.getDouble(i);
                } else if (columns.get(i).getName().equalsIgnoreCase(PulsarInternalColumn.KEY.getName())) {
                    Slice slice = pulsarRecordCursor.getSlice(i);
                    key = slice == null ? null : new String(slice.getBytes());
                }
            }
            assertNotNull(key);
            assertNotNull(name);
            assertNotNull(price);
            assertTrue(latestPrice.containsKey(key));
            assertEquals(key, name);
            assertEquals(price, latestPrice.remove(key));
        }
        if (compactedMsgNum > 0 || unCompactedMsgNum > 0) {
            assertEquals(latestPrice.size(), 1);
            assertNotNull(latestPrice.remove("stock-7"));
        } else {
            assertEquals(latestPrice.size(), 0);
        }
    }

    private Set<String> prepareData(Producer<Stock> producer, int messageCount, int divisor,
                                    Map<String, Double> latestPrice,
                                    AtomicReference<MessageIdImpl> firstMessageId,
                                    AtomicReference<MessageIdImpl> lastMessageId) throws PulsarClientException {
        Set<String> entrySet = new HashSet<>();
        AtomicInteger sendCount = new AtomicInteger();
        for (int i = 0; i < messageCount; i++) {
            String name = "stock-" + i % divisor;
            Double price = BigDecimal.valueOf(
                    RandomUtils.nextDouble(10, 100)).setScale(4, RoundingMode.HALF_UP).doubleValue();
            final int index = i;
            final Stock stock = new Stock(name, price);
            producer.newMessage().key(name).value(stock).sendAsync()
                    .thenAccept(messageId -> {
                        MessageIdImpl idImpl = (MessageIdImpl) messageId;
                        if (index == 0) {
                            firstMessageId.set(idImpl);
                        }
                        entrySet.add(idImpl.getLedgerId() + ":" + idImpl.getEntryId());
                        latestPrice.put(name, price);
                        sendCount.incrementAndGet();
                    });
        }
        if (messageCount > 0) {
            MessageIdImpl idImpl = (MessageIdImpl) producer.newMessage().key("stock-" + 7).send();
            entrySet.add(idImpl.getLedgerId() + ":" + idImpl.getEntryId() + 1);
            lastMessageId.set(new MessageIdImpl(
                    idImpl.getLedgerId(), idImpl.getEntryId() + 1, idImpl.getPartitionIndex()));
        }
        Awaitility.await()
                .atMost(5, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .until(() -> sendCount.get() == messageCount);
        return entrySet;
    }

}
