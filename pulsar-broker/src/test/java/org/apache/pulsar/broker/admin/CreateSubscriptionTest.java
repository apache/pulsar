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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.ClientErrorException;
import javax.ws.rs.core.Response.Status;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.PulsarAdminException.ConflictException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.PartitionedTopicStats;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.awaitility.Awaitility;
import org.eclipse.jetty.http.HttpStatus;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-admin")
@Slf4j
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

        assertEquals(admin.topics().getSubscriptions(topic), List.of("sub-1"));

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
                    List.of("sub-1"));
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
                    List.of("sub-1"));
        }
    }

    @DataProvider(name = "subscriptionMode")
    public Object[][] subscriptionModeProvider() {
        return new Object[][] { { SubscriptionMode.Durable }, { SubscriptionMode.NonDurable } };
    }

    @Test(dataProvider = "subscriptionMode")
    public void testSubscriptionPropertiesStats(SubscriptionMode subscriptionMode) throws Exception {
        // test non-partitioned topic
        final String topic = "persistent://my-property/my-ns/topic" + UUID.randomUUID();
        admin.topics().createNonPartitionedTopic(topic);
        Map<String, String> map = new HashMap<>();
        map.put("test-topic", "tag1");
        String subName = "my-sub";
        pulsarClient.newConsumer().subscriptionMode(subscriptionMode).topic(topic).receiverQueueSize(1)
                .subscriptionProperties(map).subscriptionName(subName).subscribe();
        TopicStats stats = admin.topics().getStats(topic);
        Map<String, String> subProperties = stats.getSubscriptions().get(subName).getSubscriptionProperties();
        assertEquals(subProperties, map);

        // test partitioned-topic
        final String partitionedTopic  = "persistent://my-property/my-ns/topic" + UUID.randomUUID();
        admin.topics().createPartitionedTopic(partitionedTopic, 10);
        Map<String, String> pMap = new HashMap<>();
        pMap.put("topic1", "tag1");
        pMap.put("topic2", "tag2");
        pMap.put("topic3", "tag3");
        String pSubName = "my-sub-1";
        pulsarClient.newConsumer().subscriptionMode(subscriptionMode).topic(partitionedTopic).receiverQueueSize(1)
                .subscriptionProperties(pMap).subscriptionName(pSubName).subscribe();

        PartitionedTopicStats pStats = admin.topics().getPartitionedStats(partitionedTopic, false);
        Map<String, String> pSubProperties = pStats.getSubscriptions().get(pSubName)
                .getSubscriptionProperties();
        assertEquals(pSubProperties, pMap);

        PartitionedTopicStats pStatsForPerPartition = admin.topics().getPartitionedStats(partitionedTopic, true);
        Map<String, String> pSubPropForPerPartition = pStatsForPerPartition.getSubscriptions().get(pSubName)
                .getSubscriptionProperties();
        assertEquals(pSubPropForPerPartition, pMap);
    }

    @Test(dataProvider = "subscriptionMode")
    public void addSubscriptionPropertiesTest(SubscriptionMode subscriptionMode) throws Exception {
        String topic = "persistent://my-property/my-ns/topic" + UUID.randomUUID();
        admin.topics().createNonPartitionedTopic(topic);
        Map<String, String> map = new HashMap<>();
        map.put("1", "1");
        map.put("2", "2");
        String subName = "my-sub";
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .subscriptionMode(subscriptionMode)
                .topic(topic).receiverQueueSize(1).subscriptionProperties(map).subscriptionName(subName).subscribe();
        PersistentSubscription subscription = (PersistentSubscription) pulsar.getBrokerService()
                .getTopicReference(topic).get().getSubscription(subName);
        Map<String, String> properties = subscription.getSubscriptionProperties();
        assertEquals(properties, map);

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
        properties = subscription.getSubscriptionProperties();
        assertEquals(properties, map);

        consumer.close();
        producer.close();
        // restart broker, consumer use old properties
        restartBroker();

        Consumer<byte[]> consumer2 = pulsarClient.newConsumer().topic(topic)
                .subscriptionMode(subscriptionMode).receiverQueueSize(1)
                .subscriptionProperties(map).subscriptionName(subName).subscribe();
        PersistentSubscription subscription2 = (PersistentSubscription) pulsar.getBrokerService()
                .getTopicReference(topic).get().getSubscription(subName);
        Awaitility.await().untilAsserted(() -> {
            Map<String, String> properties2 = subscription2.getSubscriptionProperties();
            assertEquals(properties2, map);
        });
        consumer2.close();

        // create a new consumer with new properties, the new properties should be ignored
        Map<String, String> map3 = new HashMap<>();
        map3.put("3", "3");
        map3.put("4", "4");
        Consumer<byte[]> consumer3 = pulsarClient.newConsumer().topic(topic).subscriptionMode(subscriptionMode)
                .receiverQueueSize(1)
                .subscriptionProperties(map3).subscriptionName(subName).subscribe();
        Map<String, String> properties3 = subscription.getSubscriptionProperties();
        assertEquals(properties3, map);
        consumer3.close();

        //restart and create a new consumer with new properties, the new properties must not be updated
        // for a Durable subscription, but for a NonDurable subscription we pick up the new values
        restartBroker();
        Consumer<byte[]> consumer4 = pulsarClient.newConsumer().subscriptionMode(subscriptionMode)
                .topic(topic).receiverQueueSize(1)
                .subscriptionProperties(map3).subscriptionName(subName).subscribe();
        PersistentSubscription subscription4 = (PersistentSubscription) pulsar.getBrokerService()
                .getTopicReference(topic).get().getSubscription(subName);
        Map<String, String> properties4 = subscription4.getSubscriptionProperties();
        if (subscriptionMode == SubscriptionMode.Durable) {
            assertEquals(properties4, map);
        } else {
            assertEquals(properties4, map3);

        }
        consumer4.close();


        //consumer subscribe without subscriptionProperties set, it will get the old one for a Durable Subscription
        // it will use the new (empty) set for a NonDurable Subscription
        // so for Non-Durable subscriptions you have always to re-connect with the same properties
        consumer4 = pulsarClient.newConsumer().topic(topic).subscriptionMode(subscriptionMode).receiverQueueSize(1)
                .subscriptionName(subName).subscribe();
        subscription4 = (PersistentSubscription) pulsar.getBrokerService()
                .getTopicReference(topic).get().getSubscription(subName);
        properties4 = subscription4.getSubscriptionProperties();
        if (subscriptionMode == SubscriptionMode.Durable) {
            assertEquals(properties4, map);
        } else {
            assertTrue(properties4.isEmpty());
        }
        consumer4.close();

        //restart broker, properties for Durable subscription are reloaded from Metadata
        restartBroker();
        consumer4 = pulsarClient.newConsumer().topic(topic).subscriptionMode(subscriptionMode)
                .receiverQueueSize(1)
                .subscriptionName(subName).subscribe();
        subscription4 = (PersistentSubscription) pulsar.getBrokerService()
                .getTopicReference(topic).get().getSubscription(subName);
        properties4 = subscription4.getSubscriptionProperties();
        if (subscriptionMode == SubscriptionMode.Durable) {
            assertEquals(properties4, map);
        } else {
            assertTrue(properties4.isEmpty());
        }
        consumer4.close();

        //restart broker and create a new consumer with new properties, the properties will not be updated
        restartBroker();
        consumer4 = pulsarClient.newConsumer().topic(topic).receiverQueueSize(1)
                .subscriptionMode(subscriptionMode)
                .subscriptionProperties(map)
                .subscriptionName(subName).subscribe();
        PersistentSubscription subscription5 = (PersistentSubscription) pulsar.getBrokerService()
                .getTopicReference(topic).get().getSubscription(subName);
        properties4 = subscription5.getSubscriptionProperties();

        // for the NonDurable subscription here we have the same properties because they
        // are sent by the Consumer
        assertEquals(properties4, map);

        consumer4.close();

        String subNameShared = "my-sub-shared";
        Map<String, String> mapShared = new HashMap<>();
        mapShared.put("6", "7");
        // open two consumers with a Shared Subscription
        Consumer consumerShared1 = pulsarClient.newConsumer().topic(topic).receiverQueueSize(1)
                .subscriptionMode(subscriptionMode)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionProperties(mapShared)
                .subscriptionName(subNameShared).subscribe();
        PersistentSubscription subscriptionShared = (PersistentSubscription) pulsar.getBrokerService()
                .getTopicReference(topic).get().getSubscription(subNameShared);
        properties = subscriptionShared.getSubscriptionProperties();
        assertEquals(properties, mapShared);

        // add a new consumer, the properties are not updated
        Map<String, String> mapShared2 = new HashMap<>();
        mapShared2.put("8", "9");
        Consumer consumerShared2 = pulsarClient.newConsumer().topic(topic).receiverQueueSize(1)
                .subscriptionMode(subscriptionMode)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionProperties(mapShared2)
                .subscriptionName(subNameShared).subscribe();

        properties = subscriptionShared.getSubscriptionProperties();
        assertEquals(properties, mapShared);

        // add a third consumer, the properties are NOT updated
        Map<String, String> mapShared3 = new HashMap<>();
        mapShared3.put("10", "11");
        Consumer consumerShared3 = pulsarClient.newConsumer().topic(topic).receiverQueueSize(1)
                .subscriptionMode(subscriptionMode)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionProperties(mapShared3)
                .subscriptionName(subNameShared).subscribe();

        properties = subscriptionShared.getSubscriptionProperties();
        // verify that the properties are not updated
        assertEquals(properties, mapShared);

        consumerShared1.close();
        consumerShared2.close();
        consumerShared3.close();
    }

    @Test
    public void subscriptionModePersistedTest() throws Exception {
        String topic = "persistent://my-property/my-ns/topic" + UUID.randomUUID();
        admin.topics().createNonPartitionedTopic(topic);
        Map<String, String> map = new HashMap<>();
        map.put("1", "1");
        map.put("2", "2");
        String subName = "my-sub";
        pulsarClient.newConsumer()
                .subscriptionMode(SubscriptionMode.Durable)
                .topic(topic)
                .subscriptionProperties(map)
                .subscriptionName(subName)
                .subscribe()
                .close();
        PersistentSubscription subscription = (PersistentSubscription) pulsar.getBrokerService()
                .getTopicReference(topic).get().getSubscription(subName);
        Map<String, String> properties = subscription.getSubscriptionProperties();
        assertTrue(properties.containsKey("1"));
        assertTrue(properties.containsKey("2"));
        assertEquals(properties.get("1"), "1");
        assertEquals(properties.get("2"), "2");

        Map<String, String> subscriptionPropertiesFromAdmin =
                admin.topics().getStats(topic).getSubscriptions().get(subName).getSubscriptionProperties();
        assertEquals(map, subscriptionPropertiesFromAdmin);

        // unload the topic
        admin.topics().unload(topic);

        // verify that the properties are still there
        subscriptionPropertiesFromAdmin =
                admin.topics().getStats(topic).getSubscriptions().get(subName).getSubscriptionProperties();
        assertEquals(map, subscriptionPropertiesFromAdmin);


        // create a new subscription, initially properties are empty
        String subName2 = "my-sub2";
        admin.topics().createSubscription(topic, subName2, MessageId.latest);

        subscriptionPropertiesFromAdmin =
                admin.topics().getStats(topic).getSubscriptions().get(subName2).getSubscriptionProperties();
        assertTrue(subscriptionPropertiesFromAdmin.isEmpty());

        // create a consumer, this is not allowed to update the properties
        pulsarClient.newConsumer()
                .subscriptionMode(SubscriptionMode.Durable)
                .topic(topic)
                .subscriptionProperties(map)
                .subscriptionName(subName2)
                .subscribe()
                .close();

        // verify that the properties are not changed
        subscriptionPropertiesFromAdmin =
                admin.topics().getStats(topic).getSubscriptions().get(subName2).getSubscriptionProperties();
        assertTrue(subscriptionPropertiesFromAdmin.isEmpty());
    }

    @Test
    public void createSubscriptionBySpecifyingStringPosition() throws IOException, PulsarAdminException {
        final int numberOfMessages = 5;
        String topic = "persistent://my-property/my-ns/my-topic";
        RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(30 * 1000).build();
        CloseableHttpClient httpClient = HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).build();

        // Produce some messages to pulsar
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).create();
        for (int i = 0; i < numberOfMessages; i++) {
            producer.send(new byte[10]);
        }

        // Create a subscription from the latest position
        String latestSubName = "sub-latest";
        HttpPut request = new HttpPut(String.format("%s/admin/v2/persistent/my-property/my-ns/my-topic/subscription/%s",
                admin.getServiceUrl(), latestSubName));
        request.setHeader("Content-Type", "application/json");
        request.setEntity(new StringEntity("\"latest\""));

        HttpResponse httpResponse = httpClient.execute(request);
        assertEquals(httpResponse.getStatusLine().getStatusCode(), HttpStatus.NO_CONTENT_204);

        long msgBacklog = admin.topics().getStats(topic).getSubscriptions().get(latestSubName).getMsgBacklog();
        assertEquals(msgBacklog, 0);

        // Create a subscription from the earliest position
        String earliestSubName = "sub-earliest";
        request = new HttpPut(String.format("%s/admin/v2/persistent/my-property/my-ns/my-topic/subscription/%s",
                admin.getServiceUrl(), earliestSubName));
        request.setHeader("Content-Type", "application/json");
        request.setEntity(new StringEntity("\"earliest\""));

        httpResponse = httpClient.execute(request);
        assertEquals(httpResponse.getStatusLine().getStatusCode(), HttpStatus.NO_CONTENT_204);

        msgBacklog = admin.topics().getStats(topic).getSubscriptions().get(earliestSubName).getMsgBacklog();
        assertEquals(msgBacklog, numberOfMessages);

        producer.close();
    }

    @Test
    public void testWaitingCurosrCausedMemoryLeak() throws Exception {
        String topic = "persistent://my-property/my-ns/my-topic";
        for (int i = 0; i < 10; i ++) {
            Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING).topic(topic)
                    .subscriptionType(SubscriptionType.Failover).subscriptionName("test" + i).subscribe();
            Awaitility.await().untilAsserted(() -> assertTrue(consumer.isConnected()));
            consumer.close();
        }
        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topic).get();
        ManagedLedgerImpl ml = (ManagedLedgerImpl)(topicRef.getManagedLedger());
        assertEquals(ml.getWaitingCursorsCount(), 0);
    }

}
