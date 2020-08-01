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
package org.apache.kafka.clients.producer;

import static org.testng.Assert.assertEquals;

import java.lang.reflect.Field;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.impl.ProducerBuilderImpl;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.testng.annotations.Test;

import kafka.producer.Partitioner;
import kafka.producer.ProducerConfig;
import kafka.serializer.Encoder;

public class PulsarKafkaProducerTest {

    private static final String BROKER_URL = "metadata.broker.list";
    private static final String PRODUCER_TYPE = "producer.type";
    private static final String SERIALIZER_CLASS = "serializer.class";
    private static final String KEY_SERIALIZER_CLASS = "key.serializer.class";
    private static final String PARTITIONER_CLASS = "partitioner.class";
    private static final String COMPRESSION_CODEC = "compression.codec";
    private static final String QUEUE_BUFFERING_MAX_MS = "queue.buffering.max.ms";
    private static final String QUEUE_BUFFERING_MAX_MESSAGES = "queue.buffering.max.messages";
    private static final String QUEUE_ENQUEUE_TIMEOUT_MS = "queue.enqueue.timeout.ms";
    private static final String BATCH_NUM_MESSAGES = "batch.num.messages";
    private static final String CLIENT_ID = "client.id";

    @Test
    public void testPulsarKafkaProducerWithDefaultConfig() throws Exception {
        // https://kafka.apache.org/08/documentation.html#producerconfigs
        Properties properties = new Properties();
        properties.put(BROKER_URL, "http://localhost:8080/");

        ProducerConfig config = new ProducerConfig(properties);
        PulsarKafkaProducer<byte[], byte[]> producer = new PulsarKafkaProducer<>(config);
        ProducerBuilderImpl<byte[]> producerBuilder = (ProducerBuilderImpl<byte[]>) producer.getPulsarProducerBuilder();
        Field field = ProducerBuilderImpl.class.getDeclaredField("conf");
        field.setAccessible(true);
        ProducerConfigurationData conf = (ProducerConfigurationData) field.get(producerBuilder);
        System.out.println("getMaxPendingMessages= " + conf.getMaxPendingMessages());
        assertEquals(conf.getCompressionType(), CompressionType.NONE);
        assertEquals(conf.isBlockIfQueueFull(), true);
        assertEquals(conf.getMaxPendingMessages(), 1000);
        assertEquals(conf.getBatchingMaxPublishDelayMicros(), TimeUnit.MILLISECONDS.toMicros(1));
        assertEquals(conf.getBatchingMaxMessages(), 1000);
    }

    @Test
    public void testPulsarKafkaProducer() throws Exception {
        // https://kafka.apache.org/08/documentation.html#producerconfigs
        Properties properties = new Properties();
        properties.put(BROKER_URL, "http://localhost:8080/");
        properties.put(COMPRESSION_CODEC, "gzip"); // compression: ZLIB
        properties.put(QUEUE_ENQUEUE_TIMEOUT_MS, "-1"); // block queue if full => -1 = true
        properties.put(QUEUE_BUFFERING_MAX_MESSAGES, "6000"); // queue max message
        properties.put(QUEUE_BUFFERING_MAX_MS, "100"); // batch delay
        properties.put(BATCH_NUM_MESSAGES, "500"); // batch msg
        properties.put(CLIENT_ID, "test");
        ProducerConfig config = new ProducerConfig(properties);
        PulsarKafkaProducer<byte[], byte[]> producer = new PulsarKafkaProducer<>(config);
        ProducerBuilderImpl<byte[]> producerBuilder = (ProducerBuilderImpl<byte[]>) producer.getPulsarProducerBuilder();
        Field field = ProducerBuilderImpl.class.getDeclaredField("conf");
        field.setAccessible(true);
        ProducerConfigurationData conf = (ProducerConfigurationData) field.get(producerBuilder);
        assertEquals(conf.getCompressionType(), CompressionType.ZLIB);
        assertEquals(conf.isBlockIfQueueFull(), true);
        assertEquals(conf.getMaxPendingMessages(), 6000);
        assertEquals(conf.getBatchingMaxPublishDelayMicros(), TimeUnit.MILLISECONDS.toMicros(100));
        assertEquals(conf.getBatchingMaxMessages(), 500);
    }

    @Test
    public void testPulsarKafkaProducerWithSerializer() throws Exception {
        Properties properties = new Properties();
        properties.put(BROKER_URL, "http://localhost:8080/");
        properties.put(PRODUCER_TYPE, "sync");
        properties.put(SERIALIZER_CLASS, TestEncoder.class.getName());
        properties.put(KEY_SERIALIZER_CLASS, TestEncoder.class.getName());
        properties.put(PARTITIONER_CLASS, TestPartitioner.class.getName());
        ProducerConfig config = new ProducerConfig(properties);
        PulsarKafkaProducer<byte[], byte[]> producer = new PulsarKafkaProducer<>(config);
        assertEquals(producer.getKeySerializer().getClass(), TestEncoder.class);
        assertEquals(producer.getValueSerializer().getClass(), TestEncoder.class);
        assertEquals(producer.getPartitioner().getClass(), TestPartitioner.class);

    }

    public static class TestEncoder implements Encoder<String> {
        @Override
        public byte[] toBytes(String value) {
            return value.getBytes();
        }
    }

    public static class TestPartitioner implements Partitioner {
        @Override
        public int partition(Object obj, int totalPartition) {
            return obj.hashCode() % totalPartition;
        }
    }

}
