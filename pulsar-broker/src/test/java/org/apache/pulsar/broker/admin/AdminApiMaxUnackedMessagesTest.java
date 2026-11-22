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
package org.apache.pulsar.broker.admin;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.CustomLog;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@CustomLog
@Test(groups = "broker-admin")
public class AdminApiMaxUnackedMessagesTest extends SharedPulsarBaseTest {

    private int brokerMaxUnackedMessagesPerConsumer;

    @Override
    protected boolean checkClientHandlesClosed() {
        return true;
    }

    @BeforeMethod
    public void saveConfiguration() {
        brokerMaxUnackedMessagesPerConsumer = getConfig().getMaxUnackedMessagesPerConsumer();
    }

    @AfterMethod(alwaysRun = true)
    public void restoreConfiguration() {
        // the shared broker reads it when a topic loads, so it applies only to this class's topics
        getConfig().setMaxUnackedMessagesPerConsumer(brokerMaxUnackedMessagesPerConsumer);
    }

    @Test(timeOut = 30000)
    public void testNamespacePolicy() throws Exception {
        getConfig().setMaxUnackedMessagesPerConsumer(3);
        final String namespace = getNamespace();
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
                (PersistentTopic) getTopicIfExists(topic).get().get();
        org.apache.pulsar.broker.service.Consumer serverConsumer =
                persistentTopic.getSubscription("sub").getConsumers().get(0);
        Awaitility.await().untilAsserted(() ->
                assertEquals(serverConsumer.getMaxUnackedMessages(), 1));
        //consumer-throttling should take effect
        for (int i = 0; i < 20; i++) {
            producer.send("msg".getBytes());
        }
        Message<byte[]> message = consumer.receive(500, TimeUnit.MILLISECONDS);
        assertNotNull(message);
        Message<byte[]> nullMsg = consumer.receive(500, TimeUnit.MILLISECONDS);
        assertNull(nullMsg);
        Awaitility.await().untilAsserted(() -> assertTrue(serverConsumer.isBlocked()));

        //disable limit check
        admin.namespaces().setMaxUnackedMessagesPerConsumer(namespace, 0);
        Awaitility.await().untilAsserted(() -> {
            assertEquals(serverConsumer.getMaxUnackedMessages(), 0);
            assertFalse(serverConsumer.isBlocked());
        });
        Message<byte[]> nextMessage = consumer.receive(5, TimeUnit.SECONDS);
        assertNotNull(nextMessage);
        consumer.acknowledge(message);
        consumer.acknowledge(nextMessage);
    }

    @Test
    public void testMaxUnackedMessagesOnConsumers() throws Exception {
        String namespace = getNamespace();
        assertNull(admin.namespaces().getMaxUnackedMessagesPerConsumer(namespace));
        admin.namespaces().setMaxUnackedMessagesPerConsumer(namespace, 2 * 50000);
        assertEquals(2 * 50000, admin.namespaces().getMaxUnackedMessagesPerConsumer(namespace).intValue());
    }

    @Test
    public void testMaxUnackedMessagesOnSubscription() throws Exception {
        String namespace = getNamespace();
        assertNull(admin.namespaces().getMaxUnackedMessagesPerSubscription(namespace));
        admin.namespaces().setMaxUnackedMessagesPerSubscription(namespace, 2 * 200000);
        Awaitility.await().untilAsserted(()
                -> assertEquals(2 * 200000, admin.namespaces()
                .getMaxUnackedMessagesPerSubscription(namespace).intValue()));

        admin.namespaces().removeMaxUnackedMessagesPerSubscription(namespace);
        Awaitility.await().untilAsserted(()
                -> assertNull(admin.namespaces().getMaxUnackedMessagesPerSubscription(namespace)));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testMaxUnackedMessagesPerConsumerPriority() throws Exception {
        int brokerLevelPolicy = 3;
        int namespaceLevelPolicy = 2;
        int topicLevelPolicy = 1;
        // the topic created below takes this broker-level value when it loads
        getConfig().setMaxUnackedMessagesPerConsumer(brokerLevelPolicy);
        final String namespace = getNamespace();
        final String topic = "persistent://" + namespace + "/testMaxUnackedMessagesPerConsumerPriority";
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
        PersistentTopic persistentTopic = (PersistentTopic) getTopicIfExists(topic).get().get();
        org.apache.pulsar.broker.service.Consumer serverConsumer =
                persistentTopic.getSubscription("sub").getConsumers().get(0);
        assertEquals(serverConsumer.getMaxUnackedMessages(), topicLevelPolicy);
        List<Message<?>> msgs = consumeMsg(consumer, 3);
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

    private List<Message<?>> consumeMsg(Consumer<?> consumer, int msgNum) throws Exception {
        List<Message<?>> list = new ArrayList<>();
        for (int i = 0; i < msgNum; i++) {
            Message<?> message = consumer.receive(1500, TimeUnit.MILLISECONDS);
            if (message == null) {
                break;
            }
            list.add(message);
        }
        return list;
    }

    private void ackMsgs(Consumer<?> consumer, List<Message<?>> msgs) throws Exception {
        for (Message<?> msg : msgs) {
            consumer.acknowledge(msg);
        }
    }
}
