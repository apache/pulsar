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
package org.apache.pulsar.broker.admin;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import com.google.common.collect.Sets;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;


@Slf4j
@Test(groups = "broker")
public class AdminApiMaxUnackedMessages extends MockedPulsarServiceBaseTest {

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        super.internalSetup();

        admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Sets.newHashSet("role1", "role2"), Sets.newHashSet("test"));
        admin.tenants().createTenant("max-unacked-messages", tenantInfo);
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
        resetConfig();
    }

    @Test(timeOut = 30000)
    public void testNamespacePolicy() throws Exception {
        pulsar.getConfiguration().setMaxUnackedMessagesPerConsumer(3);
        admin.namespaces().createNamespace("max-unacked-messages/policy-on-consumers");
        final String namespace = "max-unacked-messages/policy-on-consumers";
        final String topic = "persistent://" + namespace + "/testNamespacePolicy";

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).create();

        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer().receiverQueueSize(1)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionName("sub").topic(topic).subscribe();
        //set namespace-level policy
        admin.namespaces().setMaxUnackedMessagesPerConsumer(namespace, 1);
        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(topic).get().get();
        Awaitility.await().untilAsserted(() ->
                assertEquals(persistentTopic.getSubscription("sub")
                        .getConsumers().get(0).getMaxUnackedMessages(), 1));
        //consumer-throttling should take effect
        for (int i = 0; i < 20; i++) {
            producer.send("msg".getBytes());
        }
        Message<byte[]> message = consumer.receive(500, TimeUnit.MILLISECONDS);
        assertNotNull(message);
        Message<byte[]> nullMsg = consumer.receive(500, TimeUnit.MILLISECONDS);
        assertNull(nullMsg);

        //disable limit check
        admin.namespaces().setMaxUnackedMessagesPerConsumer(namespace, 0);
        Awaitility.await().untilAsserted(() ->
                assertEquals(persistentTopic.getSubscription("sub")
                        .getConsumers().get(0).getMaxUnackedMessages(), 0));
        consumer.acknowledge(message);
        message = consumer.receive(500, TimeUnit.MILLISECONDS);
        assertNotNull(message);
    }

    @Test
    public void testMaxUnackedMessagesOnConsumers() throws Exception {
        admin.namespaces().createNamespace("max-unacked-messages/default-on-consumers");
        String namespace = "max-unacked-messages/default-on-consumers";
        assertNull(admin.namespaces().getMaxUnackedMessagesPerConsumer(namespace));
        admin.namespaces().setMaxUnackedMessagesPerConsumer(namespace, 2*50000);
        assertEquals(2*50000, admin.namespaces().getMaxUnackedMessagesPerConsumer(namespace).intValue());
    }

    @Test
    public void testMaxUnackedMessagesOnSubscription() throws Exception {
        admin.namespaces().createNamespace("max-unacked-messages/default-on-subscription");
        String namespace = "max-unacked-messages/default-on-subscription";
        assertNull(admin.namespaces().getMaxUnackedMessagesPerSubscription(namespace));
        admin.namespaces().setMaxUnackedMessagesPerSubscription(namespace, 2*200000);
        Awaitility.await().untilAsserted(()
                -> assertEquals(2*200000, admin.namespaces().getMaxUnackedMessagesPerSubscription(namespace).intValue()));

        admin.namespaces().removeMaxUnackedMessagesPerSubscription(namespace);
        Awaitility.await().untilAsserted(()
                -> assertNull(admin.namespaces().getMaxUnackedMessagesPerSubscription(namespace)));
    }

    @Test
    public void testMaxUnackedMessagesPerConsumerPriority() throws Exception {
        int brokerLevelPolicy = 3;
        int namespaceLevelPolicy = 2;
        int topicLevelPolicy = 1;
        cleanup();
        conf.setSystemTopicEnabled(true);
        conf.setTopicLevelPoliciesEnabled(true);
        conf.setMaxUnackedMessagesPerConsumer(brokerLevelPolicy);
        setup();
        final String namespace = "max-unacked-messages/priority-on-consumers";
        final String topic = "persistent://" + namespace + "/testMaxUnackedMessagesPerConsumerPriority";
        admin.namespaces().createNamespace(namespace);
        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).create();
        for (int i = 0; i < 50; i++) {
            producer.send("msg".getBytes());
        }
        assertNull(admin.namespaces().getMaxUnackedMessagesPerConsumer(namespace));
        assertNull(admin.topics().getMaxUnackedMessagesOnConsumer(topic));
        admin.namespaces().setMaxUnackedMessagesPerConsumer(namespace, namespaceLevelPolicy);
        admin.topics().setMaxUnackedMessagesOnConsumer(topic, topicLevelPolicy);

        Awaitility.await().untilAsserted(()
                -> assertNotNull(admin.namespaces().getMaxUnackedMessagesPerConsumer(namespace)));
        Awaitility.await().untilAsserted(()
                -> assertNotNull(admin.topics().getMaxUnackedMessagesOnConsumer(topic)));
        assertEquals(admin.namespaces().getMaxUnackedMessagesPerConsumer(namespace).intValue(), namespaceLevelPolicy);
        assertEquals(admin.topics().getMaxUnackedMessagesOnConsumer(topic).intValue(), topicLevelPolicy);
        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName("sub").topic(topic).receiverQueueSize(1).subscribe();
        PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(topic).get().get();
        org.apache.pulsar.broker.service.Consumer serverConsumer =
                persistentTopic.getSubscription("sub").getConsumers().get(0);
        assertEquals(serverConsumer.getMaxUnackedMessages(), topicLevelPolicy);
        List<Message> msgs = consumeMsg(consumer, 3);
        assertEquals(msgs.size(), 1);
        //disable topic-level limiter
        admin.topics().setMaxUnackedMessagesOnConsumer(topic, 0);
        Awaitility.await().untilAsserted(()
                -> assertEquals(admin.topics().getMaxUnackedMessagesOnConsumer(topic).intValue(), 0));
        ackMsgs(consumer, msgs);
        //remove topic-level policy, namespace-level should take effect
        admin.topics().removeMaxUnackedMessagesOnConsumer(topic);
        Awaitility.await().untilAsserted(() ->
                assertNull(admin.topics().getMaxUnackedMessagesOnConsumer(topic)));
        assertEquals(admin.namespaces().getMaxUnackedMessagesPerConsumer(namespace).intValue(), namespaceLevelPolicy);
        Awaitility.await().untilAsserted(() ->
                assertEquals(serverConsumer.getMaxUnackedMessages(), namespaceLevelPolicy));
        msgs = consumeMsg(consumer, 5);
        assertEquals(msgs.size(), namespaceLevelPolicy);
        ackMsgs(consumer, msgs);
        //disable namespace-level limiter
        admin.namespaces().setMaxUnackedMessagesPerConsumer(namespace, 0);
        Awaitility.await().untilAsserted(()
                -> assertEquals(admin.namespaces().getMaxUnackedMessagesPerConsumer(namespace).intValue(), 0));
        msgs = consumeMsg(consumer, 5);
        assertEquals(msgs.size(), 5);
        ackMsgs(consumer, msgs);
        //remove namespace-level policy, broker-level should take effect
        admin.namespaces().removeMaxUnackedMessagesPerConsumer(namespace);
        Awaitility.await().untilAsserted(()
                -> assertNull(admin.namespaces().getMaxUnackedMessagesPerConsumer(namespace)));
        msgs = consumeMsg(consumer, 5);
        assertEquals(msgs.size(), brokerLevelPolicy);
        ackMsgs(consumer, msgs);

    }

    private List<Message> consumeMsg(Consumer<?> consumer, int msgNum) throws Exception {
        List<Message> list = new ArrayList<>();
        for (int i = 0; i <msgNum; i++) {
            Message message = consumer.receive(500, TimeUnit.MILLISECONDS);
            if (message == null) {
                break;
            }
            list.add(message);
        }
        return list;
    }

    private void ackMsgs(Consumer<?> consumer, List<Message> msgs) throws Exception {
        for (Message msg : msgs) {
            consumer.acknowledge(msg);
        }
    }
}
