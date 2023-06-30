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
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import lombok.Cleanup;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.testng.annotations.Test;

public class TopicCompactionServiceTest extends CompactorTest {

    @Test
    public void test() throws PulsarClientException, PulsarAdminException {
        admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Set.of("role1", "role2"), Set.of("test"));
        String defaultTenant = "prop-xyz";
        admin.tenants().createTenant(defaultTenant, tenantInfo);
        String defaultNamespace = defaultTenant + "/ns1";
        admin.namespaces().createNamespace(defaultNamespace, Set.of("test"));

        String topic = "persistent://prop-xyz/ns1/my-topic";

        PulsarTopicCompactionService service = new PulsarTopicCompactionService(topic, bk, () -> compactor);

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();

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
        compactedTopic.newCompactedLedger(PositionImpl.get(Long.valueOf(split[0]), Long.valueOf(split[1])),
                compactedLedger).join();

        Position lastCompactedPosition = service.getLastCompactedPosition().join();
        assertEquals(admin.topics().getInternalStats(topic).lastConfirmedEntry, lastCompactedPosition.toString());

        List<Entry> entries = service.readCompactedEntries(PositionImpl.EARLIEST, 4).join();
        assertEquals(entries.size(), 2);
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
            } else {
                assertEquals(data, "B_3");
            }
        });
    }
}
