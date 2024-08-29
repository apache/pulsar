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
package org.apache.pulsar.compaction;

import static org.apache.pulsar.compaction.Compactor.COMPACTED_TOPIC_LEDGER_PROPERTY;
import static org.apache.pulsar.compaction.Compactor.COMPACTION_SUBSCRIPTION;
import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.fail;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import lombok.Cleanup;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.api.proto.BrokerEntryMetadata;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.protocol.Commands;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TopicCompactionServiceTest extends MockedPulsarServiceBaseTest {

    protected ScheduledExecutorService compactionScheduler;
    protected BookKeeper bk;
    private PublishingOrderCompactor compactor;

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        conf.setExposingBrokerEntryMetadataToClientEnabled(true);

        super.internalSetup();

        admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Set.of("role1", "role2"), Set.of("test"));
        String defaultTenant = "prop-xyz";
        admin.tenants().createTenant(defaultTenant, tenantInfo);
        String defaultNamespace = defaultTenant + "/ns1";
        admin.namespaces().createNamespace(defaultNamespace, Set.of("test"));

        compactionScheduler = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("compactor").setDaemon(true).build());
        bk = pulsar.getBookKeeperClientFactory().create(
                this.conf, null, null, Optional.empty(), null).get();
        compactor = new PublishingOrderCompactor(conf, pulsarClient, bk, compactionScheduler);
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
        bk.close();
        if (compactionScheduler != null) {
            compactionScheduler.shutdownNow();
        }
    }

    @Test
    public void test() throws Exception {
        String topic = "persistent://prop-xyz/ns1/my-topic";

        PulsarTopicCompactionService service = new PulsarTopicCompactionService(topic, bk, () -> compactor);

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();

        producer.newMessage()
                .key("c")
                .value("C_0".getBytes())
                .send();

        conf.setBrokerEntryMetadataInterceptors(org.assertj.core.util.Sets.newTreeSet(
                "org.apache.pulsar.common.intercept.AppendIndexMetadataInterceptor"
        ));
        restartBroker();

        long startTime = System.currentTimeMillis();

        producer.newMessage()
                .key("a")
                .value("A_1".getBytes())
                .send();
        producer.newMessage()
                .key("b")
                .value("B_1".getBytes())
                .send();
        producer.newMessage()
                .key("a")
                .value("A_2".getBytes())
                .send();
        producer.newMessage()
                .key("b")
                .value("B_2".getBytes())
                .send();
        producer.newMessage()
                .key("b")
                .value("B_3".getBytes())
                .send();

        producer.flush();

        service.compact().join();


        CompactedTopicImpl compactedTopic = service.getCompactedTopic();

        Long compactedLedger = admin.topics().getInternalStats(topic).cursors.get(COMPACTION_SUBSCRIPTION).properties.get(
                COMPACTED_TOPIC_LEDGER_PROPERTY);
        String markDeletePosition =
                admin.topics().getInternalStats(topic).cursors.get(COMPACTION_SUBSCRIPTION).markDeletePosition;
        String[] split = markDeletePosition.split(":");
        compactedTopic.newCompactedLedger(PositionFactory.create(Long.valueOf(split[0]), Long.valueOf(split[1])),
                compactedLedger).join();

        Position lastCompactedPosition = service.getLastCompactedPosition().join();
        assertEquals(admin.topics().getInternalStats(topic).lastConfirmedEntry, lastCompactedPosition.toString());

        List<Entry> entries = service.readCompactedEntries(PositionFactory.EARLIEST, 4).join();
        assertEquals(entries.size(), 3);
        entries.stream().map(e -> {
            try {
                return MessageImpl.deserialize(e.getDataBuffer());
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }).forEach(message -> {
            String data = new String(message.getData());
            if (Objects.equals(message.getKey(), "a")) {
                assertEquals(data, "A_2");
            } else if (Objects.equals(message.getKey(), "b")) {
                assertEquals(data, "B_3");
            } else if (Objects.equals(message.getKey(), "c")) {
                assertEquals(data, "C_0");
            } else {
                fail();
            }
        });

        List<Entry> entries2 = service.readCompactedEntries(PositionFactory.EARLIEST, 1).join();
        assertEquals(entries2.size(), 1);

        Entry entry = service.findEntryByEntryIndex(0).join();
        BrokerEntryMetadata brokerEntryMetadata = Commands.peekBrokerEntryMetadataIfExist(entry.getDataBuffer());
        assertNotNull(brokerEntryMetadata);
        assertEquals(brokerEntryMetadata.getIndex(), 2);
        MessageMetadata metadata = Commands.parseMessageMetadata(entry.getDataBuffer());
        assertEquals(metadata.getPartitionKey(), "a");
        entry.release();

        entry = service.findEntryByEntryIndex(3).join();
        brokerEntryMetadata = Commands.peekBrokerEntryMetadataIfExist(entry.getDataBuffer());
        assertNotNull(brokerEntryMetadata);
        assertEquals(brokerEntryMetadata.getIndex(), 4);
        metadata = Commands.parseMessageMetadata(entry.getDataBuffer());
        assertEquals(metadata.getPartitionKey(), "b");
        entry.release();

        entry = service.findEntryByPublishTime(startTime).join();
        brokerEntryMetadata = Commands.peekBrokerEntryMetadataIfExist(entry.getDataBuffer());
        assertNotNull(brokerEntryMetadata);
        assertEquals(brokerEntryMetadata.getIndex(), 2);
        metadata = Commands.parseMessageMetadata(entry.getDataBuffer());
        assertEquals(metadata.getPartitionKey(), "a");
        entry.release();
    }
}
