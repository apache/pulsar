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
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.common.collect.Lists;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.ClientErrorException;
import javax.ws.rs.core.Response.Status;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.client.admin.PulsarAdminException.ConflictException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class CreateSubscriptionTest extends ProducerConsumerBase {

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        super.internalSetup();
        producerBaseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void createSubscriptionSingleTopic() throws Exception {
        String topic = "persistent://my-property/my-ns/my-topic";
        admin.topics().createSubscription(topic, "sub-1", MessageId.latest);

        // Create should fail if the subscription already exists
        try {
            admin.topics().createSubscription(topic, "sub-1", MessageId.latest);
            fail("Should have failed");
        } catch (ConflictException e) {
            assertEquals(((ClientErrorException) e.getCause()).getResponse().getStatus(),
                    Status.CONFLICT.getStatusCode());
        }

        assertEquals(admin.topics().getSubscriptions(topic), Lists.newArrayList("sub-1"));

        Producer<byte[]> p1 = pulsarClient.newProducer().topic(topic).create();
        p1.send("test-1".getBytes());
        p1.send("test-2".getBytes());
        MessageId m3 = p1.send("test-3".getBytes());

        assertEquals(admin.topics().getStats(topic).getSubscriptions().get("sub-1").getMsgBacklog(), 3);

        admin.topics().createSubscription(topic, "sub-2", MessageId.latest);
        assertEquals(admin.topics().getStats(topic).getSubscriptions().get("sub-2").getMsgBacklog(), 0);

        admin.topics().createSubscription(topic, "sub-3", MessageId.earliest);
        assertEquals(admin.topics().getStats(topic).getSubscriptions().get("sub-3").getMsgBacklog(), 3);

        admin.topics().createSubscription(topic, "sub-5", m3);
        assertEquals(admin.topics().getStats(topic).getSubscriptions().get("sub-5").getMsgBacklog(), 1);
    }

    @Test
    public void createSubscriptionOnPartitionedTopic() throws Exception {
        String topic = "persistent://my-property/my-ns/my-partitioned-topic";
        admin.topics().createPartitionedTopic(topic, 10);

        admin.topics().createSubscription(topic, "sub-1", MessageId.latest);

        // Create should fail if the subscription already exists
        try {
            admin.topics().createSubscription(topic, "sub-1", MessageId.latest);
            fail("Should have failed");
        } catch (Exception e) {
            // Expected
        }

        for (int i = 0; i < 10; i++) {
            assertEquals(admin.topics().getSubscriptions(TopicName.get(topic).getPartition(i).toString()),
                    Lists.newArrayList("sub-1"));
        }
    }

    @Test
    public void createSubscriptionOnPartitionedTopicWithPartialFailure() throws Exception {
        String topic = "persistent://my-property/my-ns/my-partitioned-topic";
        admin.topics().createPartitionedTopic(topic, 10);

        // create subscription for one partition
        final String partitionedTopic0 = topic+"-partition-0";
        admin.topics().createSubscription(partitionedTopic0, "sub-1", MessageId.latest);

        admin.topics().createSubscription(topic, "sub-1", MessageId.latest);

        // Create should fail if the subscription already exists
        try {
            admin.topics().createSubscription(topic, "sub-1", MessageId.latest);
            fail("Should have failed");
        } catch (Exception e) {
            // Expected
        }

        for (int i = 0; i < 10; i++) {
            assertEquals(
                    admin.topics().getSubscriptions(TopicName.get(topic).getPartition(i).toString()),
                    Lists.newArrayList("sub-1"));
        }
    }

    @Test
    public void addSubscriptionPropertiesTest() throws Exception {
        String topic = "persistent://my-property/my-ns/topic" + UUID.randomUUID();
        admin.topics().createNonPartitionedTopic(topic);
        Map<String, Long> map = new HashMap<>();
        map.put("1", 1L);
        map.put("2", 2L);
        String subName = "my-sub";
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).receiverQueueSize(1)
                .subscriptionProperties(map).subscriptionName(subName).subscribe();
        PersistentSubscription subscription = (PersistentSubscription) pulsar.getBrokerService()
                .getTopicReference(topic).get().getSubscription(subName);
        Map<String, Long> properties = subscription.getCursor().getProperties();
        assertTrue(properties.containsKey("1"));
        assertTrue(properties.containsKey("2"));
        assertEquals(properties.get("1").longValue(), 1L);
        assertEquals(properties.get("2").longValue(), 2L);

        // after updating mark delete position, the properties should still exist
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).create();
        for (int i = 0; i < 10; i++) {
            producer.send("msg".getBytes(StandardCharsets.UTF_8));
        }
        Message message = consumer.receive(1, TimeUnit.SECONDS);
        assertNotNull(message);
        consumer.acknowledge(message);
        MessageIdImpl messageId = (MessageIdImpl) message.getMessageId();
        Awaitility.await().untilAsserted(() -> {
            assertEquals(subscription.getCursor().getMarkDeletedPosition().getLedgerId(), messageId.getLedgerId());
            assertEquals(subscription.getCursor().getMarkDeletedPosition().getEntryId(), messageId.getEntryId());
        });
        properties = subscription.getCursor().getProperties();
        assertTrue(properties.containsKey("1"));
        assertTrue(properties.containsKey("2"));
        assertEquals(properties.get("1").longValue(), 1L);
        assertEquals(properties.get("2").longValue(), 2L);

        consumer.close();
        producer.close();
        // restart broker, the properties should still exist
        restartBroker();

        Producer<byte[]> producer2 = pulsarClient.newProducer().topic(topic).create();
        PersistentSubscription subscription2 = (PersistentSubscription) pulsar.getBrokerService()
                .getTopicReference(topic).get().getSubscription(subName);
        Awaitility.await().untilAsserted(() -> {
            Map<String, Long> properties2 = subscription2.getCursor().getProperties();
            assertTrue(properties2.containsKey("1"));
            assertTrue(properties2.containsKey("2"));
            assertEquals(properties2.get("1").longValue(), 1L);
            assertEquals(properties2.get("2").longValue(), 2L);
        });
        producer2.close();

        // create a new consumer with new properties, the new properties should be ignored
        Map<String, Long> map2 = new HashMap<>();
        map.put("3", 3L);
        map.put("4", 4L);
        Consumer<byte[]> consumer2 = pulsarClient.newConsumer().topic(topic).receiverQueueSize(1)
                .subscriptionProperties(map2).subscriptionName(subName).subscribe();
        subscription2.getCursor().getProperties();
        Map<String, Long> properties3 = subscription2.getCursor().getProperties();
        assertTrue(properties3.containsKey("1"));
        assertTrue(properties3.containsKey("2"));
        assertEquals(properties3.get("1").longValue(), 1L);
        assertEquals(properties3.get("2").longValue(), 2L);
        consumer2.close();
    }
}
