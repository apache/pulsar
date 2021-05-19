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
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "flaky")
public class ClientDeduplicationTest extends ProducerConsumerBase {

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

    @Test
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

    @Test(timeOut = 30000)
    public void testProducerDeduplicationWithDiscontinuousSequenceId() throws Exception {
        String topic = "persistent://my-property/my-ns/testProducerDeduplicationWithDiscontinuousSequenceId";
        admin.namespaces().setDeduplicationStatus("my-property/my-ns", true);

        // Set infinite timeout
        ProducerBuilder<byte[]> producerBuilder =
                pulsarClient.newProducer()
                        .topic(topic)
                        .producerName("my-producer-name")
                        .enableBatching(true)
                        .batchingMaxMessages(10)
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
            Message<byte[]> msg = consumer.receive();
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
}
