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
package org.apache.pulsar.client.api;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.common.util.FutureUtil;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "flaky")
public class ClientDeduplicationTest extends ProducerConsumerBase {

    @DataProvider
    public static Object[][] batchingTypes() {
        return new Object[][] {
                { BatcherBuilder.DEFAULT },
                { BatcherBuilder.KEY_BASED }
        };
    }

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(priority = -1)
    public void testNamespaceDeduplicationApi() throws Exception {
        final String namespace = "my-property/my-ns";
        assertNull(admin.namespaces().getDeduplicationStatus(namespace));
        admin.namespaces().setDeduplicationStatus(namespace, true);
        Awaitility.await().untilAsserted(() -> assertTrue(admin.namespaces().getDeduplicationStatus(namespace)));
        admin.namespaces().setDeduplicationStatus(namespace, false);
        Awaitility.await().untilAsserted(() -> assertFalse(admin.namespaces().getDeduplicationStatus(namespace)));
        admin.namespaces().removeDeduplicationStatus(namespace);
        Awaitility.await().untilAsserted(() -> assertNull(admin.namespaces().getDeduplicationStatus(namespace)));
    }

    @Test
    public void testProducerSequenceAfterReconnect() throws Exception {
        final String topic = "persistent://my-property/my-ns/testProducerSequenceAfterReconnect";
        admin.namespaces().setDeduplicationStatus("my-property/my-ns", true);

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer().topic(topic)
                .producerName("my-producer-name");
        Producer<byte[]> producer = producerBuilder.create();

        assertEquals(producer.getLastSequenceId(), -1L);

        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
            assertEquals(producer.getLastSequenceId(), i);
        }

        producer.close();

        producer = producerBuilder.create();
        assertEquals(producer.getLastSequenceId(), 9L);

        for (int i = 10; i < 20; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
            assertEquals(producer.getLastSequenceId(), i);
        }

        producer.close();
    }

    @Test
    public void testProducerSequenceAfterRestart() throws Exception {
        String topic = "persistent://my-property/my-ns/testProducerSequenceAfterRestart";
        admin.namespaces().setDeduplicationStatus("my-property/my-ns", true);

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer().topic(topic)
                .producerName("my-producer-name");
        Producer<byte[]> producer = producerBuilder.create();

        assertEquals(producer.getLastSequenceId(), -1L);

        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
            assertEquals(producer.getLastSequenceId(), i);
        }

        producer.close();

        // Kill and restart broker
        restartBroker();

        producer = producerBuilder.create();
        assertEquals(producer.getLastSequenceId(), 9L);

        for (int i = 10; i < 20; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
            assertEquals(producer.getLastSequenceId(), i);
        }

        producer.close();
    }

    @Test(timeOut = 30000)
    public void testProducerDeduplication() throws Exception {
        String topic = "persistent://my-property/my-ns/testProducerDeduplication";
        admin.namespaces().setDeduplicationStatus("my-property/my-ns", true);

        // Set infinite timeout
        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer().topic(topic)
                .producerName("my-producer-name").sendTimeout(0, TimeUnit.SECONDS);
        Producer<byte[]> producer = producerBuilder.create();

        assertEquals(producer.getLastSequenceId(), -1L);

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("my-subscription")
                .subscribe();

        producer.newMessage().value("my-message-0".getBytes()).sequenceId(0).send();
        producer.newMessage().value("my-message-1".getBytes()).sequenceId(1).send();
        producer.newMessage().value("my-message-2".getBytes()).sequenceId(2).send();

        // Repeat the messages and verify they're not received by consumer
        producer.newMessage().value("my-message-1".getBytes()).sequenceId(1).send();
        producer.newMessage().value("my-message-2".getBytes()).sequenceId(2).send();

        producer.close();

        for (int i = 0; i < 3; i++) {
            Message<byte[]> msg = consumer.receive();
            assertEquals(new String(msg.getData()), "my-message-" + i);
            consumer.acknowledge(msg);
        }

        // No other messages should be received
        Message<byte[]> msg = consumer.receive(1, TimeUnit.SECONDS);
        assertNull(msg);

        // Kill and restart broker
        restartBroker();

        producer = producerBuilder.create();
        assertEquals(producer.getLastSequenceId(), 2L);

        // Repeat the messages and verify they're not received by consumer
        producer.newMessage().value("my-message-1".getBytes()).sequenceId(1).send();
        producer.newMessage().value("my-message-2".getBytes()).sequenceId(2).send();

        msg = consumer.receive(1, TimeUnit.SECONDS);
        assertNull(msg);

        producer.close();
    }

    @Test(timeOut = 30000, dataProvider = "batchingTypes")
    public void testProducerDeduplicationWithDiscontinuousSequenceId(BatcherBuilder batcherBuilder) throws Exception {
        String topic = "persistent://my-property/my-ns/testProducerDeduplicationWithDiscontinuousSequenceId-"
                + System.currentTimeMillis();
        admin.namespaces().setDeduplicationStatus("my-property/my-ns", true);

        // Set infinite timeout
        ProducerBuilder<byte[]> producerBuilder =
                pulsarClient.newProducer()
                        .topic(topic)
                        .producerName("my-producer-name")
                        .enableBatching(true)
                        .batcherBuilder(batcherBuilder)
                        .batchingMaxMessages(10)
                        .batchingMaxPublishDelay(1L, TimeUnit.HOURS)
                        .sendTimeout(0, TimeUnit.SECONDS);

        Producer<byte[]> producer = producerBuilder.create();

        assertEquals(producer.getLastSequenceId(), -1L);

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("my-subscription")
                .subscribe();

        producer.newMessage().value("my-message-0".getBytes()).sequenceId(2).sendAsync();
        producer.newMessage().value("my-message-1".getBytes()).sequenceId(3).sendAsync();
        producer.newMessage().value("my-message-2".getBytes()).sequenceId(5).sendAsync();

        producer.flush();

        // Repeat the messages and verify they're not received by consumer
        producer.newMessage().value("my-message-0".getBytes()).sequenceId(2).sendAsync();
        producer.newMessage().value("my-message-1".getBytes()).sequenceId(4).sendAsync();
        producer.newMessage().value("my-message-3".getBytes()).sequenceId(6).sendAsync();
        producer.flush();

        for (int i = 0; i < 4; i++) {
            Message<byte[]> msg = consumer.receive(3, TimeUnit.SECONDS);
            assertNotNull(msg);
            assertEquals(new String(msg.getData()), "my-message-" + i);
            consumer.acknowledge(msg);
        }

        // No other messages should be received
        Message<byte[]> msg = consumer.receive(1, TimeUnit.SECONDS);
        assertNull(msg);

        producer.close();
        // Kill and restart broker
        restartBroker();

        producer = producerBuilder.create();
        assertEquals(producer.getLastSequenceId(), 6L);

        // Repeat the messages and verify they're not received by consumer
        producer.newMessage().value("my-message-1".getBytes()).sequenceId(2).sendAsync();
        producer.newMessage().value("my-message-2".getBytes()).sequenceId(4).sendAsync();
        producer.flush();

        msg = consumer.receive(1, TimeUnit.SECONDS);
        assertNull(msg);

        producer.close();
    }

    @Test(timeOut = 30000)
    public void testProducerDeduplicationNonBatchAsync() throws Exception {
        String topic = "persistent://my-property/my-ns/testProducerDeduplicationNonBatchAsync";
        admin.namespaces().setDeduplicationStatus("my-property/my-ns", true);

        // Set infinite timeout
        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer().topic(topic)
                .producerName("my-producer-name").enableBatching(false).sendTimeout(0, TimeUnit.SECONDS);
        Producer<byte[]> producer = producerBuilder.create();

        assertEquals(producer.getLastSequenceId(), -1L);

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("my-subscription")
                .subscribe();

        producer.newMessage().value("my-message-0".getBytes()).sequenceId(2).sendAsync();
        producer.newMessage().value("my-message-1".getBytes()).sequenceId(3).sendAsync();
        producer.newMessage().value("my-message-2".getBytes()).sequenceId(5).sendAsync();

        // Repeat the messages and verify they're not received by consumer
        producer.newMessage().value("my-message-1".getBytes()).sequenceId(2).sendAsync();
        producer.newMessage().value("my-message-2".getBytes()).sequenceId(4).sendAsync();
        producer.close();

        for (int i = 0; i < 3; i++) {
            Message<byte[]> msg = consumer.receive();
            assertEquals(new String(msg.getData()), "my-message-" + i);
            consumer.acknowledge(msg);
        }

        // No other messages should be received
        Message<byte[]> msg = consumer.receive(1, TimeUnit.SECONDS);
        assertNull(msg);

        // Kill and restart broker
        restartBroker();

        producer = producerBuilder.create();
        assertEquals(producer.getLastSequenceId(), 5L);

        // Repeat the messages and verify they're not received by consumer
        producer.newMessage().value("my-message-1".getBytes()).sequenceId(2).sendAsync();
        producer.newMessage().value("my-message-2".getBytes()).sequenceId(4).sendAsync();

        msg = consumer.receive(1, TimeUnit.SECONDS);
        assertNull(msg);

        producer.close();
    }

    @Test(timeOut = 30000)
    public void testKeyBasedBatchingOrder() throws Exception {
        final String topic = "persistent://my-property/my-ns/test-key-based-batching-order";
        admin.namespaces().setDeduplicationStatus("my-property/my-ns", true);

        final Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("sub")
                .subscribe();
        final Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .batcherBuilder(BatcherBuilder.KEY_BASED)
                .batchingMaxMessages(100)
                .batchingMaxBytes(1024 * 1024 * 5)
                .batchingMaxPublishDelay(1, TimeUnit.HOURS)
                .create();
        // | key | sequence id list |
        // | :-- | :--------------- |
        // | A | 0, 3, 4 |
        // | B | 1, 2 |
        final List<CompletableFuture<MessageId>> sendFutures = new ArrayList<>();
        sendFutures.add(producer.newMessage().key("A").value("msg-0").sequenceId(0L).sendAsync());
        sendFutures.add(producer.newMessage().key("B").value("msg-1").sequenceId(1L).sendAsync());
        sendFutures.add(producer.newMessage().key("B").value("msg-2").sequenceId(2L).sendAsync());
        sendFutures.add(producer.newMessage().key("A").value("msg-3").sequenceId(3L).sendAsync());
        sendFutures.add(producer.newMessage().key("A").value("msg-4").sequenceId(4L).sendAsync());
        // The message order is expected to be [1, 2, 0, 3, 4]. The sequence ids are not ordered strictly, but:
        // 1. The sequence ids for a given key are ordered.
        // 2. The highest sequence ids of batches are ordered.
        producer.flush();

        FutureUtil.waitForAll(sendFutures);
        final List<MessageId> sendMessageIds = sendFutures.stream().map(CompletableFuture::join)
                .collect(Collectors.toList());
        for (int i = 0; i < sendMessageIds.size(); i++) {
            log.info("Send msg-{} to {}", i, sendMessageIds.get(i));
        }

        final List<Long> sequenceIdList = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            final Message<String> msg = consumer.receive(3, TimeUnit.SECONDS);
            if (msg == null) {
                break;
            }
            log.info("Received {}, key: {}, seq id: {}, msg id: {}",
                    msg.getValue(), msg.getKey(), msg.getSequenceId(), msg.getMessageId());
            assertNotNull(msg);
            sequenceIdList.add(msg.getSequenceId());
        }
        assertEquals(sequenceIdList, Arrays.asList(1L, 2L, 0L, 3L, 4L));

        for (int i = 0; i < 5; i++) {
            // Currently sending a duplicated message won't throw an exception. Instead, an invalid result is returned.
            final MessageId messageId = producer.newMessage().value("msg").sequenceId(i).send();
            assertTrue(messageId instanceof BatchMessageIdImpl);
            final BatchMessageIdImpl messageIdImpl = (BatchMessageIdImpl) messageId;
            assertEquals(messageIdImpl.getLedgerId(), -1L);
            assertEquals(messageIdImpl.getEntryId(), -1L);
        }

        consumer.close();
        producer.close();
    }
}
