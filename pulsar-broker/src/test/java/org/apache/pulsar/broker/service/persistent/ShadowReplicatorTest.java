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
package org.apache.pulsar.broker.service.persistent;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-replication")
public class ShadowReplicatorTest extends BrokerTestBase {

    @BeforeClass(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
        admin.tenants().createTenant("prop1",
                new TenantInfoImpl(Sets.newHashSet("appid1"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace("prop1/ns-source");
        admin.namespaces().createNamespace("prop1/ns-shadow");
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testShadowReplication() throws Exception {
        String sourceTopicName = "persistent://prop1/ns-source/source-topic";
        String shadowTopicName = "persistent://prop1/ns-shadow/shadow-topic";
        String shadowTopicName2 = "persistent://prop1/ns-shadow/shadow-topic-2";

        admin.topics().createNonPartitionedTopic(sourceTopicName);
        admin.topics().createShadowTopic(shadowTopicName, sourceTopicName);
        admin.topics().createShadowTopic(shadowTopicName2, sourceTopicName);
        admin.topics().setShadowTopics(sourceTopicName, Lists.newArrayList(shadowTopicName, shadowTopicName2));

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(sourceTopicName).create();

        @Cleanup
        Consumer<byte[]> shadowConsumer =
                pulsarClient.newConsumer().topic(shadowTopicName).subscriptionName("shadow-sub")
                        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe();
        @Cleanup
        Consumer<byte[]> shadowConsumer2 =
                pulsarClient.newConsumer().topic(shadowTopicName2).subscriptionName("shadow-sub")
                        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe();

        PersistentTopic sourceTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(sourceTopicName).get().get();

        Awaitility.await().untilAsserted(()->Assert.assertEquals(sourceTopic.getShadowReplicators().size(), 2));

        ShadowReplicator
                replicator = (ShadowReplicator) sourceTopic.getShadowReplicators().get(shadowTopicName);
        Awaitility.await().untilAsserted(() ->
                Assert.assertEquals(String.valueOf(replicator.getState()), "Started"));

        @Cleanup
        Consumer<byte[]> sourceConsumer =
                pulsarClient.newConsumer().topic(sourceTopicName).subscriptionName("source-sub")
                        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe();
        byte[] data = "test-shadow-topic".getBytes(StandardCharsets.UTF_8);
        MessageId sourceMessageId = producer.newMessage()
                .sequenceId(1)
                .key("K")
                .property("PK", "PV")
                .eventTime(123)
                .value(data)
                .send();

        Message<byte[]> sourceMessage = sourceConsumer.receive();
        Assert.assertEquals(sourceMessage.getMessageId(), sourceMessageId);

        //Wait until msg is replicated to shadow topic.
        Awaitility.await().until(() -> {
            replicator.msgOut.calculateRate();
            return replicator.msgOut.getCount() >= 1;
        });
        Awaitility.await().until(() -> PersistentReplicator.PENDING_MESSAGES_UPDATER.get(replicator) == 0);

        PersistentTopic shadowTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(shadowTopicName).get().get();
        Assert.assertNotNull(shadowTopic);

        Message<byte[]> shadowMessage = shadowConsumer.receive(5, TimeUnit.SECONDS);

        Assert.assertEquals(shadowMessage.getData(), sourceMessage.getData());
        Assert.assertEquals(shadowMessage.getSequenceId(), sourceMessage.getSequenceId());
        Assert.assertEquals(shadowMessage.getEventTime(), sourceMessage.getEventTime());
        Assert.assertEquals(shadowMessage.getProperties(), sourceMessage.getProperties());
        Assert.assertEquals(shadowMessage.getKey(), sourceMessage.getKey());
        Assert.assertEquals(shadowMessage.getOrderingKey(), sourceMessage.getOrderingKey());
        Assert.assertEquals(shadowMessage.getSchemaVersion(), sourceMessage.getSchemaVersion());
        Assert.assertEquals(shadowMessage.getPublishTime(), sourceMessage.getPublishTime());
        Assert.assertEquals(shadowMessage.getBrokerPublishTime(), sourceMessage.getBrokerPublishTime());
        Assert.assertEquals(shadowMessage.getIndex(), sourceMessage.getIndex());

        //`replicatedFrom` is set as localClusterName in shadow topic.
        Assert.assertNotEquals(shadowMessage.getReplicatedFrom(), sourceMessage.getReplicatedFrom());
        Assert.assertEquals(shadowMessage.getMessageId(), sourceMessage.getMessageId());
    }
}