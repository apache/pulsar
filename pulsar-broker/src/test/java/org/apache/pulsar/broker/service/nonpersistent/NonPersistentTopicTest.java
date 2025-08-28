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
package org.apache.pulsar.broker.service.nonpersistent;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.lang.reflect.Field;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.broker.service.AbstractTopic;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.broker.service.SubscriptionOption;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.awaitility.Awaitility;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class NonPersistentTopicTest extends BrokerTestBase {

    @BeforeMethod(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testAccumulativeStats() throws Exception {
        final String topicName = "non-persistent://prop/ns-abc/aTopic";
        final String sharedSubName = "shared";
        final String failoverSubName = "failOver";

        Consumer<String> consumer1 = pulsarClient.newConsumer(Schema.STRING).topic(topicName)
                .subscriptionType(SubscriptionType.Shared).subscriptionName(sharedSubName).subscribe();
        Consumer<String> consumer2 = pulsarClient.newConsumer(Schema.STRING).topic(topicName)
                .subscriptionType(SubscriptionType.Failover).subscriptionName(failoverSubName).subscribe();
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicName).create();

        NonPersistentTopic topic = (NonPersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();

        // stats are at zero before any activity
        TopicStats stats = topic.getStats(false, false, false);
        assertEquals(stats.getBytesInCounter(), 0);
        assertEquals(stats.getMsgInCounter(), 0);
        assertEquals(stats.getBytesOutCounter(), 0);
        assertEquals(stats.getMsgOutCounter(), 0);

        producer.newMessage().value("test").eventTime(5).send();

        Message<String> msg = consumer1.receive();
        assertNotNull(msg);
        msg = consumer2.receive();
        assertNotNull(msg);

        // send/receive result in non-zero stats
        TopicStats statsBeforeUnsubscribe = topic.getStats(false, false, false);
        assertTrue(statsBeforeUnsubscribe.getBytesInCounter() > 0);
        assertTrue(statsBeforeUnsubscribe.getMsgInCounter() > 0);
        assertTrue(statsBeforeUnsubscribe.getBytesOutCounter() > 0);
        assertTrue(statsBeforeUnsubscribe.getMsgOutCounter() > 0);

        consumer1.unsubscribe();
        consumer2.unsubscribe();
        producer.close();
        topic.getProducers().values().forEach(topic::removeProducer);
        assertEquals(topic.getProducers().size(), 0);

        // consumer unsubscribe/producer removal does not result in stats loss
        TopicStats statsAfterUnsubscribe = topic.getStats(false, false, false);
        assertEquals(statsAfterUnsubscribe.getBytesInCounter(), statsBeforeUnsubscribe.getBytesInCounter());
        assertEquals(statsAfterUnsubscribe.getMsgInCounter(), statsBeforeUnsubscribe.getMsgInCounter());
        assertEquals(statsAfterUnsubscribe.getBytesOutCounter(), statsBeforeUnsubscribe.getBytesOutCounter());
        assertEquals(statsAfterUnsubscribe.getMsgOutCounter(), statsBeforeUnsubscribe.getMsgOutCounter());
    }

    @Test
    public void testCreateNonExistentPartitions() throws PulsarAdminException, PulsarClientException {
        final String topicName = "non-persistent://prop/ns-abc/testCreateNonExistentPartitions";
        admin.topics().createPartitionedTopic(topicName, 4);
        TopicName partition = TopicName.get(topicName).getPartition(4);
        try {
            @Cleanup
            Producer<byte[]> producer = pulsarClient.newProducer()
                    .topic(partition.toString())
                    .create();
            fail("unexpected behaviour");
        } catch (PulsarClientException.NotFoundException ignored) {

        }
        assertEquals(admin.topics().getPartitionedTopicMetadata(topicName).partitions, 4);
    }


    @Test
    public void testSubscriptionsOnNonPersistentTopic() throws Exception {
        final String topicName = "non-persistent://prop/ns-abc/topic_" + UUID.randomUUID();
        final String exclusiveSubName = "exclusive";
        final String failoverSubName = "failover";
        final String sharedSubName = "shared";
        final String keySharedSubName = "key_shared";

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName)
                .create();

        producer.send("This is a message");
        NonPersistentTopic topic = (NonPersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();

        NonPersistentTopic mockTopic = Mockito.spy(topic);
        pulsar.getBrokerService().getTopics().put(topicName, CompletableFuture.completedFuture(Optional.of(mockTopic)));
        Mockito
                .doAnswer(inv -> {
                    SubscriptionOption option = inv.getArgument(0);
                    if (option.isDurable()) {
                        return CompletableFuture.failedFuture(
                                new IllegalArgumentException("isDurable cannot be true when subscribe "
                                        + "on non-persistent topic"));
                    }
                    return inv.callRealMethod();
                }).when(mockTopic).subscribe(Mockito.any());

        @Cleanup
        Consumer<String> exclusiveConsumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName(exclusiveSubName)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionMode(SubscriptionMode.Durable)
                .subscribe();

        @Cleanup
        Consumer<String> failoverConsumer1 = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName(failoverSubName)
                .subscriptionType(SubscriptionType.Failover)
                .subscriptionMode(SubscriptionMode.Durable)
                .subscribe();
        @Cleanup
        Consumer<String> failoverConsumer2 = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName(failoverSubName)
                .subscriptionType(SubscriptionType.Failover)
                .subscriptionMode(SubscriptionMode.Durable)
                .subscribe();
        @Cleanup
        Consumer<String> sharedConsumer1 = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName(sharedSubName)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionMode(SubscriptionMode.Durable)
                .subscribe();
        @Cleanup
        Consumer<String> sharedConsumer2 = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName(sharedSubName)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionMode(SubscriptionMode.Durable)
                .subscribe();

        @Cleanup
        Consumer<String> keySharedConsumer1 = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName(keySharedSubName)
                .subscriptionType(SubscriptionType.Key_Shared)
                .subscriptionMode(SubscriptionMode.Durable)
                .subscribe();
        @Cleanup
        Consumer<String> keySharedConsumer2 = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName(keySharedSubName)
                .subscriptionType(SubscriptionType.Key_Shared)
                .subscriptionMode(SubscriptionMode.Durable)
                .subscribe();

        final var subscriptionMap = mockTopic.getSubscriptions();
        assertEquals(subscriptionMap.size(), 4);

        // Check exclusive subscription
        NonPersistentSubscription exclusiveSub = subscriptionMap.get(exclusiveSubName);
        assertNotNull(exclusiveSub);
        exclusiveConsumer.close();
        Awaitility.waitAtMost(10, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS)
                .until(() -> subscriptionMap.get(exclusiveSubName) == null);

        // Check failover subscription
        NonPersistentSubscription failoverSub = subscriptionMap.get(failoverSubName);
        assertNotNull(failoverSub);
        failoverConsumer1.close();
        failoverSub = subscriptionMap.get(failoverSubName);
        assertNotNull(failoverSub);
        failoverConsumer2.close();
        Awaitility.waitAtMost(10, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS)
                .until(() -> subscriptionMap.get(failoverSubName) == null);

        // Check shared subscription
        NonPersistentSubscription sharedSub = subscriptionMap.get(sharedSubName);
        assertNotNull(sharedSub);
        sharedConsumer1.close();
        sharedSub = subscriptionMap.get(sharedSubName);
        assertNotNull(sharedSub);
        sharedConsumer2.close();
        Awaitility.waitAtMost(10, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS)
                .until(() -> subscriptionMap.get(sharedSubName) == null);

        // Check KeyShared subscription
        NonPersistentSubscription keySharedSub = subscriptionMap.get(keySharedSubName);
        assertNotNull(keySharedSub);
        keySharedConsumer1.close();
        keySharedSub = subscriptionMap.get(keySharedSubName);
        assertNotNull(keySharedSub);
        keySharedConsumer2.close();
        Awaitility.waitAtMost(10, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS)
                .until(() -> subscriptionMap.get(keySharedSubName) == null);
    }


    @Test
    public void testRemoveProducerOnNonPersistentTopic() throws Exception {
        final String topicName = "non-persistent://prop/ns-abc/topic_" + UUID.randomUUID();

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topicName)
                .create();

        NonPersistentTopic topic = (NonPersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();
        Field field = AbstractTopic.class.getDeclaredField("userCreatedProducerCount");
        field.setAccessible(true);
        int userCreatedProducerCount = (int) field.get(topic);
        assertEquals(userCreatedProducerCount, 1);

        producer.close();
        userCreatedProducerCount = (int) field.get(topic);
        assertEquals(userCreatedProducerCount, 0);
    }
}
