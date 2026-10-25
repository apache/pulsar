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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.CustomLog;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.policies.data.stats.NonPersistentTopicStatsImpl;
import org.apache.pulsar.common.policies.data.stats.TopicStatsImpl;
import org.apache.pulsar.common.util.FutureUtil;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@CustomLog
@Test(groups = "broker-admin")
public class AdminTopicApiTest extends SharedPulsarBaseTest {

    @Test
    public void testDeleteNonExistTopic() throws Exception {
        // Case 1: call delete for a partitioned topic.
        final String topic1 = newTopicName();
        admin.topics().createPartitionedTopic(topic1, 2);
        admin.schemas().createSchemaAsync(topic1, Schema.STRING.getSchemaInfo());
        Awaitility.await().untilAsserted(() -> {
            assertEquals(admin.schemas().getAllSchemas(topic1).size(), 1);
        });
        try {
            admin.topics().delete(topic1);
            fail("expected a 409 error");
        } catch (Exception ex) {
            assertTrue(ex.getMessage().contains("please call delete-partitioned-topic"));
        }
        Awaitility.await().pollDelay(Duration.ofSeconds(2)).untilAsserted(() -> {
            assertEquals(admin.schemas().getAllSchemas(topic1).size(), 1);
        });
        // cleanup.
        admin.topics().deletePartitionedTopic(topic1, false);

        // Case 2: call delete-partitioned-topic for a non-partitioned topic.
        final String topic2 = newTopicName();
        admin.topics().createNonPartitionedTopic(topic2);
        admin.schemas().createSchemaAsync(topic2, Schema.STRING.getSchemaInfo());
        Awaitility.await().untilAsserted(() -> {
            assertEquals(admin.schemas().getAllSchemas(topic2).size(), 1);
        });
        try {
            admin.topics().deletePartitionedTopic(topic2);
            fail("expected a 409 error");
        } catch (Exception ex) {
            assertTrue(ex.getMessage().contains("Instead of calling delete-partitioned-topic please call delete"));
        }
        Awaitility.await().pollDelay(Duration.ofSeconds(2)).untilAsserted(() -> {
            assertEquals(admin.schemas().getAllSchemas(topic2).size(), 1);
        });
        // cleanup.
        admin.topics().delete(topic2, false);

        // Case 3: delete topic does not exist.
        final String topic3 = newTopicName();
        try {
            admin.topics().delete(topic3);
            fail("expected a 404 error");
        } catch (Exception ex) {
            assertTrue(ex.getMessage().contains("not found"));
        }
        try {
            admin.topics().deletePartitionedTopic(topic3);
            fail("expected a 404 error");
        } catch (Exception ex) {
            assertTrue(ex.getMessage().contains("not found"));
        }
    }

    @Test
    public void testPeekMessages() throws Exception {
        @Cleanup
        PulsarClient newPulsarClient = PulsarClient.builder()
            .serviceUrl(getBrokerServiceUrl())
            .build();

        final String topic = newTopicName();

        @Cleanup
        Consumer<byte[]> consumer = newPulsarClient.newConsumer()
            .topic(topic)
            .subscriptionName("my-sub")
            .subscribe();

        final int numMessages = 5;

        @Cleanup
        Producer<byte[]> producer = newPulsarClient.newProducer()
                .topic(topic)
                .enableBatching(true)
                .batchingMaxPublishDelay(3, TimeUnit.SECONDS)
                .batchingMaxMessages(5)
                .create();

        for (int i = 0; i < numMessages; i++) {
            producer.newMessage()
                .value(("value-" + i).getBytes(UTF_8))
                .sendAsync();
        }
        producer.flush();

        for (int i = 0; i < numMessages; i++) {
            Message<byte[]> msg = consumer.receive();
            log.info().attr("message", new String(msg.getValue(), UTF_8)).log("Received message");
        }
        List<Message<byte[]>> messages = admin.topics().peekMessages(topic, "my-sub", 5);
        Assert.assertEquals(new String(messages.get(0).getValue(), UTF_8), "value-0");
        Assert.assertEquals(new String(messages.get(1).getValue(), UTF_8), "value-1");
        Assert.assertEquals(new String(messages.get(2).getValue(), UTF_8), "value-2");
        Assert.assertEquals(new String(messages.get(3).getValue(), UTF_8), "value-3");
        Assert.assertEquals(new String(messages.get(4).getValue(), UTF_8), "value-4");
    }

    @DataProvider
    public Object[] getStatsDataProvider() {
        return new Object[]{
                TopicDomain.persistent.value(),
                TopicDomain.non_persistent.value(),
        };
    }

    @Test(dataProvider = "getStatsDataProvider")
    public void testGetStats(String domain) throws Exception {
        String topic = domain + "://" + getNamespace() + "/" + UUID.randomUUID();
        admin.topics().createNonPartitionedTopic(topic);

        @Cleanup
        PulsarClient newPulsarClient = PulsarClient.builder()
                .serviceUrl(getBrokerServiceUrl())
                .build();

        final String subscriptionName = "my-sub";
        @Cleanup
        Consumer<byte[]> consumer = newPulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName(subscriptionName)
                .subscribe();

        TopicStats stats = admin.topics().getStats(topic);
        assertNotNull(stats);
        if (topic.startsWith(TopicDomain.non_persistent.value())) {
            assertTrue(stats instanceof NonPersistentTopicStatsImpl);
        } else {
            assertTrue(stats instanceof TopicStatsImpl);
        }
        assertTrue(stats.getSubscriptions().containsKey(subscriptionName));
    }

    @Test
    public void testGetMessagesId() throws PulsarClientException, ExecutionException, InterruptedException {
        String topic = newTopicName();

        int numMessages = 10;
        int batchingMaxMessages = numMessages / 2;

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .enableBatching(true)
                .batchingMaxMessages(batchingMaxMessages)
                .batchingMaxPublishDelay(60, TimeUnit.SECONDS)
                .create();

        List<CompletableFuture<MessageId>> futures = new ArrayList<>();
        for (int i = 0; i < numMessages; i++) {
            futures.add(producer.sendAsync(("msg-" + i).getBytes(UTF_8)));
        }
        FutureUtil.waitForAll(futures).get();

        Map<MessageIdImpl, Integer> messageIdMap = new HashMap<>();
        futures.forEach(n -> {
            try {
                MessageId messageId = n.get();
                if (messageId instanceof MessageIdImpl impl) {
                    MessageIdImpl key = new MessageIdImpl(impl.getLedgerId(), impl.getEntryId(), -1);
                    Integer i = messageIdMap.computeIfAbsent(key, __ -> 0);
                    messageIdMap.put(key, i + 1);
                }
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        });

        messageIdMap.forEach((key, value) -> {
            assertEquals(value, batchingMaxMessages);
            try {
                List<Message<byte[]>> messages = admin.topics().getMessagesById(topic,
                        key.getLedgerId(), key.getEntryId());
                assertNotNull(messages);
                assertEquals(messages.size(), batchingMaxMessages);
            } catch (PulsarAdminException e) {
                throw new RuntimeException(e);
            }
        });

        // The message id doesn't exist.
        assertThrows(PulsarAdminException.NotFoundException.class, () -> admin.topics()
                .getMessagesById(topic, 1024, 2048));
    }
}
