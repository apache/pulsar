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
package org.apache.pulsar.client.kafka.compat.tests;

import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.PulsarKafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class KafkaConsumerTest extends BrokerTestBase {

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        isTcpLookup = true;
        super.baseSetup();
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testSimpleConsumer() throws Exception {
        String topic = "persistent://prop/ns-abc/testSimpleConsumer";

        Properties props = new Properties();
        props.put("bootstrap.servers", lookupUrl.toString());
        props.put("group.id", "my-subscription-name");
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());

        Consumer<String, String> consumer = new PulsarKafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));

        Producer<byte[]> pulsarProducer = pulsarClient.newProducer().topic(topic).create();

        for (int i = 0; i < 10; i++) {
            pulsarProducer.newMessage().key(Integer.toString(i)).value(("hello-" + i).getBytes()).send();
        }

        AtomicInteger received = new AtomicInteger();
        while (received.get() < 10) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            records.forEach(record -> {
                assertEquals(record.key(), Integer.toString(received.get()));
                assertEquals(record.value(), "hello-" + received.get());

                received.incrementAndGet();
            });

            consumer.commitSync();
        }

        consumer.close();
    }

    @Test
    public void testConsumerAutoCommit() throws Exception {
        String topic = "persistent://prop/ns-abc/testConsumerAutoCommit";

        Properties props = new Properties();
        props.put("bootstrap.servers", lookupUrl.toString());
        props.put("group.id", "my-subscription-name");
        props.put("enable.auto.commit", "true");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());

        Consumer<String, String> consumer = new PulsarKafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));

        Producer<byte[]> pulsarProducer = pulsarClient.newProducer().topic(topic).create();

        for (int i = 0; i < 10; i++) {
            pulsarProducer.newMessage().key(Integer.toString(i)).value(("hello-" + i).getBytes()).send();
        }

        AtomicInteger received = new AtomicInteger();
        while (received.get() < 10) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            records.forEach(record -> {
                assertEquals(record.key(), Integer.toString(received.get()));
                assertEquals(record.value(), "hello-" + received.get());
                received.incrementAndGet();
            });
        }

        consumer.close();

        // Re-open consumer and verify every message was acknowledged
        Consumer<String, String> consumer2 = new PulsarKafkaConsumer<>(props);
        consumer2.subscribe(Arrays.asList(topic));

        ConsumerRecords<String, String> records = consumer2.poll(100);
        assertEquals(records.count(), 0);
        consumer2.close();
    }

    @Test
    public void testConsumerManualOffsetCommit() throws Exception {
        String topic = "persistent://sample/standalone/ns/testConsumerManualOffsetCommit";

        Properties props = new Properties();
        props.put("bootstrap.servers", lookupUrl.toString());
        props.put("group.id", "my-subscription-name");
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());

        Consumer<String, String> consumer = new PulsarKafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));

        Producer<byte[]> pulsarProducer = pulsarClient.newProducer().topic(topic).create();

        for (int i = 0; i < 10; i++) {
            pulsarProducer.newMessage().key(Integer.toString(i)).value(("hello-" + i).getBytes()).send();
        }

        AtomicInteger received = new AtomicInteger();
        while (received.get() < 10) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            records.forEach(record -> {
                assertEquals(record.key(), Integer.toString(received.get()));
                assertEquals(record.value(), "hello-" + received.get());

                Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                offsets.put(new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset()));
                consumer.commitSync(offsets);

                received.incrementAndGet();
            });
        }

        consumer.close();

        // Re-open consumer and verify every message was acknowledged
        Consumer<String, String> consumer2 = new PulsarKafkaConsumer<>(props);
        consumer2.subscribe(Arrays.asList(topic));

        ConsumerRecords<String, String> records = consumer2.poll(100);
        assertEquals(records.count(), 0);
        consumer2.close();
    }

    @Test
    public void testPartitions() throws Exception {
        String topic = "persistent://sample/standalone/ns/testPartitions";

        // Create 8 partitions in topic
        admin.tenants().createTenant("sample", new TenantInfo());
        admin.topics().createPartitionedTopic(topic, 8);

        Properties props = new Properties();
        props.put("bootstrap.servers", lookupUrl.toString());
        props.put("group.id", "my-subscription-name");
        props.put("enable.auto.commit", "true");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());

        Producer<byte[]> pulsarProducer = pulsarClient.newProducer().topic(topic)
                .messageRoutingMode(org.apache.pulsar.client.api.MessageRoutingMode.RoundRobinPartition).create();

        // Create 2 Kakfa consumer and verify each gets half of the messages
        List<Consumer<String, String>> consumers = new ArrayList<>();
        for (int c = 0; c < 2; c++) {
            Consumer<String, String> consumer = new PulsarKafkaConsumer<>(props);
            consumer.subscribe(Arrays.asList(topic));
            consumers.add(consumer);
        }

        int N = 8 * 3;

        for (int i = 0; i < N; i++) {
            pulsarProducer.newMessage().key(Integer.toString(i)).value(("hello-" + i).getBytes()).send();
        }

        consumers.forEach(consumer -> {
            int expectedMessaged = N / consumers.size();

            for (int i = 0; i < expectedMessaged;) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                i += records.count();
            }

            // No more messages for this consumer
            ConsumerRecords<String, String> records = consumer.poll(100);
            assertEquals(records.count(), 0);
        });

        consumers.forEach(Consumer::close);
    }

    @Test
    public void testConsumerSeek() throws Exception {
        String topic = "persistent://sample/standalone/ns/testSimpleConsumer";

        Properties props = new Properties();
        props.put("bootstrap.servers", lookupUrl.toString());
        props.put("group.id", "my-subscription-name");
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("pulsar.consumer.acknowledgments.group.time.millis", "0");

        Consumer<String, String> consumer = new PulsarKafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));

        Producer<byte[]> pulsarProducer = pulsarClient.newProducer().topic(topic).create();

        for (int i = 0; i < 10; i++) {
            pulsarProducer.newMessage().key(Integer.toString(i)).value(("hello-" + i).getBytes()).send();
        }

        AtomicInteger received = new AtomicInteger();
        while (received.get() < 10) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            records.forEach(record -> {
                assertEquals(record.key(), Integer.toString(received.get()));
                assertEquals(record.value(), "hello-" + received.get());

                received.incrementAndGet();
            });

            consumer.commitSync();
        }

        consumer.seekToBeginning(Collections.emptyList());

        Thread.sleep(500);

        // Messages should be available again
        received.set(0);
        while (received.get() < 10) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            records.forEach(record -> {
                assertEquals(record.key(), Integer.toString(received.get()));
                assertEquals(record.value(), "hello-" + received.get());

                received.incrementAndGet();
            });

            consumer.commitSync();
        }

        consumer.close();
    }

    @Test
    public void testConsumerSeekToEnd() throws Exception {
        String topic = "persistent://sample/standalone/ns/testSimpleConsumer";

        Properties props = new Properties();
        props.put("bootstrap.servers", lookupUrl.toString());
        props.put("group.id", "my-subscription-name");
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("pulsar.consumer.acknowledgments.group.time.millis", "0");

        Consumer<String, String> consumer = new PulsarKafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));

        Producer<byte[]> pulsarProducer = pulsarClient.newProducer().topic(topic).create();

        for (int i = 0; i < 10; i++) {
            pulsarProducer.newMessage().key(Integer.toString(i)).value(("hello-" + i).getBytes()).send();
        }

        AtomicInteger received = new AtomicInteger();
        while (received.get() < 10) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            records.forEach(record -> {
                assertEquals(record.key(), Integer.toString(received.get()));
                assertEquals(record.value(), "hello-" + received.get());

                received.incrementAndGet();
            });

            consumer.commitSync();
        }

        consumer.seekToEnd(Collections.emptyList());
        Thread.sleep(500);

        consumer.close();

        // Recreate the consumer
        consumer = new PulsarKafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));

        ConsumerRecords<String, String> records = consumer.poll(100);
        // Since we are at the end of the topic, there should be no messages
        assertEquals(records.count(), 0);

        consumer.close();
    }

}
