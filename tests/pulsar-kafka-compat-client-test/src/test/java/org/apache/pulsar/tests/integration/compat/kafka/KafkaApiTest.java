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
package org.apache.pulsar.tests.integration.compat.kafka;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.Cleanup;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import org.apache.avro.reflect.Nullable;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.client.impl.schema.StringSchema;
import org.apache.pulsar.client.kafka.compat.PulsarKafkaSchema;
import org.apache.pulsar.tests.integration.suites.PulsarStandaloneTestSuite;
import org.testng.Assert;
import org.testng.annotations.Test;

@Slf4j
public class KafkaApiTest extends PulsarStandaloneTestSuite {

    @Data
    public static class Foo {
        @Nullable
        private String field1;
        @Nullable
        private String field2;
        private int field3;
    }

    @Data
    public static class Bar {
        private boolean field1;
    }

    private static String getPlainTextServiceUrl() {
        return container.getPlainTextServiceUrl();
    }

    private static String getHttpServiceUrl() {
        return container.getHttpServiceUrl();
    }

    @Test(timeOut = 30000)
    public void testSimpleProducerConsumer() throws Exception {
        String topic = "persistent://public/default/testSimpleProducerConsumer";

        Properties producerProperties = new Properties();
        producerProperties.put("bootstrap.servers", getPlainTextServiceUrl());
        producerProperties.put("key.serializer", IntegerSerializer.class.getName());
        producerProperties.put("value.serializer", StringSerializer.class.getName());
        Producer<Integer, String> producer = new KafkaProducer<>(producerProperties);

        Properties consumerProperties = new Properties();
        consumerProperties.put("bootstrap.servers", getPlainTextServiceUrl());
        consumerProperties.put("group.id", "my-subscription-name");
        consumerProperties.put("key.deserializer", IntegerDeserializer.class.getName());
        consumerProperties.put("value.deserializer", StringDeserializer.class.getName());
        consumerProperties.put("enable.auto.commit", "true");
        Consumer<Integer, String> consumer = new KafkaConsumer<>(consumerProperties);
        consumer.subscribe(Arrays.asList(topic));

        List<Long> offsets = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            RecordMetadata md = producer.send(new ProducerRecord<Integer, String>(topic, i, "hello-" + i)).get();
            offsets.add(md.offset());
            log.info("Published message at {}", Long.toHexString(md.offset()));
        }

        producer.flush();
        producer.close();

        AtomicInteger received = new AtomicInteger();
        while (received.get() < 10) {
            ConsumerRecords<Integer, String> records = consumer.poll(100);
            records.forEach(record -> {
                assertEquals(record.key().intValue(), received.get());
                assertEquals(record.value(), "hello-" + received.get());
                assertEquals(record.offset(), offsets.get(received.get()).longValue());

                received.incrementAndGet();
            });

            consumer.commitSync();
        }

        consumer.close();
    }

    @Test
    public void testSimpleConsumer() throws Exception {
        String topic = "testSimpleConsumer";

        Properties props = new Properties();
        props.put("bootstrap.servers", getPlainTextServiceUrl());
        props.put("group.id", "my-subscription-name");
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());

        @Cleanup
        Consumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));

        @Cleanup
        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(getPlainTextServiceUrl()).build();

        @Cleanup
        org.apache.pulsar.client.api.Producer<byte[]> pulsarProducer = pulsarClient.newProducer().topic(topic).create();

        for (int i = 0; i < 10; i++) {
            pulsarProducer.newMessage().key(Integer.toString(i)).value(("hello-" + i).getBytes()).send();
        }

        AtomicInteger received = new AtomicInteger();
        while (received.get() < 10) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            if (!records.isEmpty()) {
                records.forEach(record -> {
                    String key = Integer.toString(received.get());
                    String value = "hello-" + received.get();
                    log.info("Receive record : key = {}, value = {}, topic = {}, ptn = {}",
                        key, value, record.topic(), record.partition());
                    assertEquals(record.key(), key);
                    assertEquals(record.value(), value);

                    received.incrementAndGet();
                });

                consumer.commitSync();
            }
        }
    }

    @Test
    public void testConsumerAutoCommit() throws Exception {
        String topic = "testConsumerAutoCommit";

        Properties props = new Properties();
        props.put("bootstrap.servers", getPlainTextServiceUrl());
        props.put("group.id", "my-subscription-name");
        props.put("enable.auto.commit", "true");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());

        Consumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));

        @Cleanup
        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(getPlainTextServiceUrl()).build();
        org.apache.pulsar.client.api.Producer<byte[]> pulsarProducer = pulsarClient.newProducer().topic(topic).create();

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
        Consumer<String, String> consumer2 = new KafkaConsumer<>(props);
        consumer2.subscribe(Arrays.asList(topic));

        ConsumerRecords<String, String> records = consumer2.poll(100);
        assertEquals(records.count(), 0);
        consumer2.close();
    }

    @Test
    public void testConsumerManualOffsetCommit() throws Exception {
        String topic = "testConsumerManualOffsetCommit";

        Properties props = new Properties();
        props.put("bootstrap.servers", getPlainTextServiceUrl());
        props.put("group.id", "my-subscription-name");
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());

        Consumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));

        @Cleanup
        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(getPlainTextServiceUrl()).build();
        org.apache.pulsar.client.api.Producer<byte[]> pulsarProducer = pulsarClient.newProducer().topic(topic).create();

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
        Consumer<String, String> consumer2 = new KafkaConsumer<>(props);
        consumer2.subscribe(Arrays.asList(topic));

        ConsumerRecords<String, String> records = consumer2.poll(100);
        assertEquals(records.count(), 0);
        consumer2.close();
    }

    @Test
    public void testPartitions() throws Exception {
        String topic = "testPartitions";

        // Create 8 partitions in topic
        @Cleanup
        PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(getHttpServiceUrl()).build();
        admin.topics().createPartitionedTopic(topic, 8);

        Properties props = new Properties();
        props.put("bootstrap.servers", getPlainTextServiceUrl());
        props.put("group.id", "my-subscription-name");
        props.put("enable.auto.commit", "true");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());

        @Cleanup
        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(getPlainTextServiceUrl()).build();
        org.apache.pulsar.client.api.Producer<byte[]> pulsarProducer = pulsarClient.newProducer().topic(topic)
                .messageRoutingMode(org.apache.pulsar.client.api.MessageRoutingMode.RoundRobinPartition).create();

        // Create 2 Kakfa consumer and verify each gets half of the messages
        List<Consumer<String, String>> consumers = new ArrayList<>();
        for (int c = 0; c < 2; c++) {
            Consumer<String, String> consumer = new KafkaConsumer<>(props);
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
    public void testExplicitPartitions() throws Exception {
        String topic = "testExplicitPartitions";

        // Create 8 partitions in topic
        @Cleanup
        PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(getHttpServiceUrl()).build();
        admin.topics().createPartitionedTopic(topic, 8);

        Properties producerProperties = new Properties();
        producerProperties.put("bootstrap.servers", getPlainTextServiceUrl());
        producerProperties.put("key.serializer", IntegerSerializer.class.getName());
        producerProperties.put("value.serializer", StringSerializer.class.getName());

        @Cleanup
        Producer<Integer, String> producer = new KafkaProducer<>(producerProperties);

        Properties props = new Properties();
        props.put("bootstrap.servers", getPlainTextServiceUrl());
        props.put("group.id", "my-subscription-name");
        props.put("enable.auto.commit", "true");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());

        // Create Kakfa consumer and verify all messages came from intended partition
        @Cleanup
        Consumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));

        int N = 8 * 3;

        final int choosenPartition = 5;

        for (int i = 0; i < N; i++) {
            producer.send(new ProducerRecord<>(topic, choosenPartition, i, "hello-" + i));
        }

        producer.flush();

        for (int i = 0; i < N;) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            i += records.count();

            records.forEach(record -> {
                assertEquals(record.partition(), choosenPartition);
            });
        }

        // No more messages for this consumer
        ConsumerRecords<String, String> records = consumer.poll(100);
        assertEquals(records.count(), 0);
    }

    public static class MyCustomPartitioner implements Partitioner {

        static int USED_PARTITION = 3;

        @Override
        public void configure(Map<String, ?> conf) {
            // Do nothing
        }

        @Override
        public void close() {
            // Do nothing
        }

        @Override
        public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
            // Dummy implementation that always return same partition
            return USED_PARTITION;
        }
    }

    @Test
    public void testCustomRouter() throws Exception {
        String topic = "testCustomRouter";

        // Create 8 partitions in topic
        @Cleanup
        PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(getHttpServiceUrl()).build();
        admin.topics().createPartitionedTopic(topic, 8);

        Properties producerProperties = new Properties();
        producerProperties.put("bootstrap.servers", getPlainTextServiceUrl());
        producerProperties.put("key.serializer", IntegerSerializer.class.getName());
        producerProperties.put("value.serializer", StringSerializer.class.getName());
        producerProperties.put("partitioner.class", MyCustomPartitioner.class.getName());

        @Cleanup
        Producer<Integer, String> producer = new KafkaProducer<>(producerProperties);

        Properties props = new Properties();
        props.put("bootstrap.servers", getPlainTextServiceUrl());
        props.put("group.id", "my-subscription-name");
        props.put("enable.auto.commit", "true");
        props.put("key.deserializer", IntegerDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());

        // Create Kakfa consumer and verify all messages came from intended partition
        @Cleanup
        Consumer<Integer, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));

        int N = 8 * 3;

        for (int i = 0; i < N; i++) {
            producer.send(new ProducerRecord<>(topic, i, "hello-" + i));
        }

        producer.flush();

        for (int i = 0; i < N;) {
            ConsumerRecords<Integer, String> records = consumer.poll(100);
            i += records.count();

            records.forEach(record -> {
                assertEquals(record.partition(), MyCustomPartitioner.USED_PARTITION);
            });
        }

        // No more messages for this consumer
        ConsumerRecords<Integer, String> records = consumer.poll(100);
        assertEquals(records.count(), 0);
    }

    @Test
    public void testConsumerSeek() throws Exception {
        String topic = "testConsumerSeek";

        Properties props = new Properties();
        props.put("bootstrap.servers", getPlainTextServiceUrl());
        props.put("group.id", "my-subscription-name");
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("pulsar.consumer.acknowledgments.group.time.millis", "0");

        Consumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));

        @Cleanup
        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(getPlainTextServiceUrl()).build();
        org.apache.pulsar.client.api.Producer<byte[]> pulsarProducer = pulsarClient.newProducer().topic(topic).create();

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
        String topic = "testConsumerSeekToEnd";

        Properties props = new Properties();
        props.put("bootstrap.servers", getPlainTextServiceUrl());
        props.put("group.id", "my-subscription-name");
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("pulsar.consumer.acknowledgments.group.time.millis", "0");

        Consumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));

        @Cleanup
        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(getPlainTextServiceUrl()).build();
        org.apache.pulsar.client.api.Producer<byte[]> pulsarProducer = pulsarClient.newProducer().topic(topic).create();

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
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));

        ConsumerRecords<String, String> records = consumer.poll(100);
        // Since we are at the end of the topic, there should be no messages
        assertEquals(records.count(), 0);

        consumer.close();
    }

    @Test
    public void testSimpleProducer() throws Exception {
        String topic = "testSimpleProducer";

        @Cleanup
        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(getPlainTextServiceUrl()).build();
        org.apache.pulsar.client.api.Consumer<byte[]> pulsarConsumer = pulsarClient.newConsumer().topic(topic)
                .subscriptionName("my-subscription")
                .subscribe();

        Properties props = new Properties();
        props.put("bootstrap.servers", getPlainTextServiceUrl());

        props.put("key.serializer", IntegerSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        Producer<Integer, String> producer = new KafkaProducer<>(props);

        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<Integer, String>(topic, i, "hello-" + i));
        }

        producer.flush();
        producer.close();

        for (int i = 0; i < 10; i++) {
            Message<byte[]> msg = pulsarConsumer.receive(1, TimeUnit.SECONDS);
            assertEquals(new String(msg.getData()), "hello-" + i);
            pulsarConsumer.acknowledge(msg);
        }
    }

    @Test(timeOut = 10000)
    public void testProducerCallback() throws Exception {
        String topic = "testProducerCallback";

        @Cleanup
        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(getPlainTextServiceUrl()).build();
        org.apache.pulsar.client.api.Consumer<byte[]> pulsarConsumer = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName("my-subscription")
                .subscribe();

        Properties props = new Properties();
        props.put("bootstrap.servers", getPlainTextServiceUrl());

        props.put("key.serializer", IntegerSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        Producer<Integer, String> producer = new KafkaProducer<>(props);

        CountDownLatch counter = new CountDownLatch(10);

        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<Integer, String>(topic, i, "hello-" + i), (metadata, exception) -> {
                assertEquals(metadata.topic(), topic);
                assertNull(exception);

                counter.countDown();
            });
        }

        counter.await();

        for (int i = 0; i < 10; i++) {
            Message<byte[]> msg = pulsarConsumer.receive(1, TimeUnit.SECONDS);
            assertEquals(new String(msg.getData()), "hello-" + i);
            pulsarConsumer.acknowledge(msg);
        }

        producer.close();
    }

    @Test
    public void testProducerAvroSchemaWithPulsarKafkaClient() throws Exception {
        String topic = "testProducerAvroSchemaWithPulsarKafkaClient";
        AvroSchema<Bar> barSchema = AvroSchema.of(SchemaDefinition.<Bar>builder().withPojo(Bar.class).build());
        AvroSchema<Foo> fooSchema = AvroSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).build());
        @Cleanup
        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(getPlainTextServiceUrl()).build();
        org.apache.pulsar.client.api.Consumer<byte[]> pulsarConsumer =
                pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName("my-subscription")
                .subscribe();
        Properties props = new Properties();
        props.put("bootstrap.servers", getPlainTextServiceUrl());
        props.put("key.serializer", IntegerSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        Producer<Bar, Foo> producer = new KafkaProducer<>(props, barSchema, fooSchema);
        for (int i = 0; i < 10; i++) {
            Bar bar = new Bar();
            bar.setField1(true);

            Foo foo = new Foo();
            foo.setField1("field1");
            foo.setField2("field2");
            foo.setField3(i);
            producer.send(new ProducerRecord<Bar, Foo>(topic, bar, foo));
        }
        producer.flush();
        producer.close();

        for (int i = 0; i < 10; i++) {
            Message<byte[]> msg = pulsarConsumer.receive(1, TimeUnit.SECONDS);
            Foo value = fooSchema.decode(msg.getValue());
            Assert.assertEquals(value.getField1(), "field1");
            Assert.assertEquals(value.getField2(), "field2");
            Assert.assertEquals(value.getField3(), i);
            pulsarConsumer.acknowledge(msg);
        }
    }

    @Test
    public void testConsumerAvroSchemaWithPulsarKafkaClient() throws Exception {
        String topic = "testConsumerAvroSchemaWithPulsarKafkaClient";

        StringSchema stringSchema = new StringSchema();
        AvroSchema<Foo> fooSchema = AvroSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).build());

        Properties props = new Properties();
        props.put("bootstrap.servers", getPlainTextServiceUrl());
        props.put("group.id", "my-subscription-name");
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());

        @Cleanup
        Consumer<String, Foo> consumer = new KafkaConsumer<String, Foo>(props, new StringSchema(), fooSchema);
        consumer.subscribe(Arrays.asList(topic));

        @Cleanup
        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(getPlainTextServiceUrl()).build();
        org.apache.pulsar.client.api.Producer<Foo> pulsarProducer = pulsarClient.newProducer(fooSchema).topic(topic).create();

        for (int i = 0; i < 10; i++) {
            Foo foo = new Foo();
            foo.setField1("field1");
            foo.setField2("field2");
            foo.setField3(i);
            pulsarProducer.newMessage().keyBytes(stringSchema.encode(Integer.toString(i))).value(foo).send();
        }

        AtomicInteger received = new AtomicInteger();
        while (received.get() < 10) {
            ConsumerRecords<String, Foo> records = consumer.poll(100);
            if (!records.isEmpty()) {
                records.forEach(record -> {
                    Assert.assertEquals(record.key(), Integer.toString(received.get()));
                    Foo value = record.value();
                    Assert.assertEquals(value.getField1(), "field1");
                    Assert.assertEquals(value.getField2(), "field2");
                    Assert.assertEquals(value.getField3(), received.get());
                    received.incrementAndGet();
                });

                consumer.commitSync();
            }
        }
    }

    @Test
    public void testProducerConsumerAvroSchemaWithPulsarKafkaClient() throws Exception {
        String topic = "testProducerConsumerAvroSchemaWithPulsarKafkaClient";

        AvroSchema<Bar> barSchema = AvroSchema.of(SchemaDefinition.<Bar>builder().withPojo(Bar.class).build());
        AvroSchema<Foo> fooSchema = AvroSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).build());

        Properties props = new Properties();
        props.put("bootstrap.servers", getPlainTextServiceUrl());
        props.put("group.id", "my-subscription-name");
        props.put("enable.auto.commit", "false");
        props.put("key.serializer", IntegerSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());

        @Cleanup
        Consumer<Bar, Foo> consumer = new KafkaConsumer<>(props, barSchema, fooSchema);
        consumer.subscribe(Arrays.asList(topic));

        Producer<Bar, Foo> producer = new KafkaProducer<>(props, barSchema, fooSchema);

        for (int i = 0; i < 10; i++) {
            Bar bar = new Bar();
            bar.setField1(true);

            Foo foo = new Foo();
            foo.setField1("field1");
            foo.setField2("field2");
            foo.setField3(i);
            producer.send(new ProducerRecord<>(topic, bar, foo));
        }
        producer.flush();
        producer.close();

        AtomicInteger received = new AtomicInteger();
        while (received.get() < 10) {
            ConsumerRecords<Bar, Foo> records = consumer.poll(100);
            if (!records.isEmpty()) {
                records.forEach(record -> {
                    Bar key = record.key();
                    Assert.assertTrue(key.isField1());
                    Foo value = record.value();
                    Assert.assertEquals(value.getField1(), "field1");
                    Assert.assertEquals(value.getField2(), "field2");
                    Assert.assertEquals(value.getField3(), received.get());
                    received.incrementAndGet();
                });

                consumer.commitSync();
            }
        }
    }

    @Test
    public void testProducerConsumerJsonSchemaWithPulsarKafkaClient() throws Exception {
        String topic = "testProducerConsumerJsonSchemaWithPulsarKafkaClient";

        JSONSchema<Bar> barSchema = JSONSchema.of(SchemaDefinition.<Bar>builder().withPojo(Bar.class).build());
        JSONSchema<Foo> fooSchema = JSONSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).build());

        Properties props = new Properties();
        props.put("bootstrap.servers", getPlainTextServiceUrl());
        props.put("group.id", "my-subscription-name");
        props.put("enable.auto.commit", "false");
        props.put("key.serializer", IntegerSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());

        @Cleanup
        Consumer<Bar, Foo> consumer = new KafkaConsumer<>(props, barSchema, fooSchema);
        consumer.subscribe(Arrays.asList(topic));

        Producer<Bar, Foo> producer = new KafkaProducer<>(props, barSchema, fooSchema);

        for (int i = 0; i < 10; i++) {
            Bar bar = new Bar();
            bar.setField1(true);

            Foo foo = new Foo();
            foo.setField1("field1");
            foo.setField2("field2");
            foo.setField3(i);
            producer.send(new ProducerRecord<>(topic, bar, foo));
        }
        producer.flush();
        producer.close();

        AtomicInteger received = new AtomicInteger();
        while (received.get() < 10) {
            ConsumerRecords<Bar, Foo> records = consumer.poll(100);
            if (!records.isEmpty()) {
                records.forEach(record -> {
                    Bar key = record.key();
                    Assert.assertTrue(key.isField1());
                    Foo value = record.value();
                    Assert.assertEquals(value.getField1(), "field1");
                    Assert.assertEquals(value.getField2(), "field2");
                    Assert.assertEquals(value.getField3(), received.get());
                    received.incrementAndGet();
                });

                consumer.commitSync();
            }
        }
    }

    @Test
    public void testProducerConsumerMixedSchemaWithPulsarKafkaClient() throws Exception {
        String topic = "testProducerConsumerMixedSchemaWithPulsarKafkaClient";

        Schema<String> keySchema = new PulsarKafkaSchema<>(new StringSerializer(), new StringDeserializer());
        JSONSchema<Foo> valueSchema = JSONSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).build());

        Properties props = new Properties();
        props.put("bootstrap.servers", getPlainTextServiceUrl());
        props.put("group.id", "my-subscription-name");
        props.put("enable.auto.commit", "false");
        props.put("key.serializer", IntegerSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());

        @Cleanup
        Consumer<String, Foo> consumer = new KafkaConsumer<>(props, keySchema, valueSchema);
        consumer.subscribe(Arrays.asList(topic));

        Producer<String, Foo> producer = new KafkaProducer<>(props, keySchema, valueSchema);

        for (int i = 0; i < 10; i++) {
            Foo foo = new Foo();
            foo.setField1("field1");
            foo.setField2("field2");
            foo.setField3(i);
            producer.send(new ProducerRecord<>(topic, "hello" + i, foo));
        }
        producer.flush();
        producer.close();

        AtomicInteger received = new AtomicInteger();
        while (received.get() < 10) {
            ConsumerRecords<String, Foo> records = consumer.poll(100);
            if (!records.isEmpty()) {
                records.forEach(record -> {
                    String key = record.key();
                    Assert.assertEquals(key, "hello" + received.get());
                    Foo value = record.value();
                    Assert.assertEquals(value.getField1(), "field1");
                    Assert.assertEquals(value.getField2(), "field2");
                    Assert.assertEquals(value.getField3(), received.get());
                    received.incrementAndGet();
                });

                consumer.commitSync();
            }
        }
    }
}
