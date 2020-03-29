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
package org.apache.pulsar.client.kafka.test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerConnector;
import org.apache.kafka.clients.consumer.PulsarKafkaStream;
import org.apache.kafka.clients.consumer.PulsarMessageAndMetadata;
import org.apache.kafka.clients.producer.PulsarKafkaProducer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Objects;
import com.google.common.collect.Sets;

import kafka.consumer.ConsumerConfig;
import kafka.producer.KeyedMessage;
import kafka.producer.Partitioner;
import kafka.producer.ProducerConfig;
import kafka.serializer.Decoder;
import kafka.serializer.Encoder;
import kafka.serializer.StringDecoder;
import kafka.serializer.StringEncoder;

public class KafkaProducerConsumerTest extends ProducerConsumerBase {

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

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testPulsarKafkaProducerWithSerializer() throws Exception {
        final String serviceUrl = lookupUrl.toString();
        final String topicName = "persistent://my-property/my-ns/my-topic1";

        // (1) Create consumer
        Properties properties = new Properties();
        properties.put("zookeeper.connect", serviceUrl);
        properties.put("group.id", "group1");
        properties.put("consumer.id", "cons1");
        properties.put("auto.commit.enable", "true");
        properties.put("auto.commit.interval.ms", "100");
        properties.put("queued.max.message.chunks", "100");

        ConsumerConfig conSConfig = new ConsumerConfig(properties);
        ConsumerConnector connector = new ConsumerConnector(conSConfig);
        Map<String, Integer> topicCountMap = Collections.singletonMap(topicName, 2);
        Map<String, List<PulsarKafkaStream<String, Tweet>>> streams = connector.createMessageStreams(topicCountMap,
                new StringDecoder(null), new TestDecoder());

        // (2) Create producer
        Properties properties2 = new Properties();
        properties2.put(BROKER_URL, serviceUrl);
        properties2.put(PRODUCER_TYPE, "sync");
        properties2.put(SERIALIZER_CLASS, TestEncoder.class.getName());
        properties2.put(KEY_SERIALIZER_CLASS, StringEncoder.class.getName());
        properties2.put(PARTITIONER_CLASS, TestPartitioner.class.getName());
        properties2.put(COMPRESSION_CODEC, "gzip"); // compression: ZLIB
        properties2.put(QUEUE_ENQUEUE_TIMEOUT_MS, "-1"); // block queue if full => -1 = true
        properties2.put(QUEUE_BUFFERING_MAX_MESSAGES, "6000"); // queue max message
        properties2.put(QUEUE_BUFFERING_MAX_MS, "100"); // batch delay
        properties2.put(BATCH_NUM_MESSAGES, "500"); // batch msg
        properties2.put(CLIENT_ID, "test");
        ProducerConfig config = new ProducerConfig(properties2);
        PulsarKafkaProducer<String, Tweet> producer = new PulsarKafkaProducer<>(config);

        String name = "user";
        String msg = "Hello World!";
        Set<Tweet> published = Sets.newHashSet();
        Set<Tweet> received = Sets.newHashSet();
        int total = 10;
        for (int i = 0; i < total; i++) {
            String sendMessage = msg + i;
            Tweet tweet = new Tweet(name, sendMessage);
            KeyedMessage<String, Tweet> message = new KeyedMessage<>(topicName, name, tweet);
            published.add(tweet);
            producer.send(message);
        }
        while (received.size() < total) {
            for (int i = 0; i < streams.size(); i++) {
                List<PulsarKafkaStream<String, Tweet>> kafkaStreams = streams.get(topicName);
                assertEquals(kafkaStreams.size(), 2);
                for (PulsarKafkaStream<String, Tweet> kafkaStream : kafkaStreams) {
                    for (PulsarMessageAndMetadata<String, KafkaProducerConsumerTest.Tweet> record : kafkaStream) {
                        received.add(record.message());
                        assertEquals(record.key(), name);
                    }
                }
            }
        }
        assertEquals(published.size(), received.size());
        published.removeAll(received);
        assertTrue(published.isEmpty());
    }

    public static class Tweet implements Serializable {
        private static final long serialVersionUID = 1L;
        public String userName;
        public String message;

        public Tweet(String userName, String message) {
            super();
            this.userName = userName;
            this.message = message;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(userName, message);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Tweet) {
                Tweet tweet = (Tweet) obj;
                return Objects.equal(this.userName, tweet.userName) && Objects.equal(this.message, tweet.message);
            }
            return false;
        }
    }

    public static class TestEncoder implements Encoder<Tweet> {
        @Override
        public byte[] toBytes(Tweet tweet) {
            return (tweet.userName + "," + tweet.message).getBytes();
        }
    }

    public static class TestDecoder implements Decoder<Tweet> {
        @Override
        public Tweet fromBytes(byte[] input) {
            String[] tokens = (new String(input)).split(",");
            return new Tweet(tokens[0], tokens[1]);
        }
    }

    public static class TestPartitioner implements Partitioner {
        @Override
        public int partition(Object obj, int totalPartition) {
            return obj.hashCode() % totalPartition;
        }
    }

    
    @Test
    public void testProducerConsumerWithoutSerializer() throws Exception {
        final String serviceUrl = lookupUrl.toString();
        final String topicName = "persistent://my-property/my-ns/my-topic1";
        // (1) Create consumer
        Properties properties = new Properties();
        properties.put("zookeeper.connect", serviceUrl);
        properties.put("group.id", "group1");
        properties.put("consumer.id", "cons1");
        properties.put("auto.commit.enable", "true");
        properties.put("auto.commit.interval.ms", "100");
        properties.put("queued.max.message.chunks", "100");

        ConsumerConfig conSConfig = new ConsumerConfig(properties);
        ConsumerConnector connector = new ConsumerConnector(conSConfig);
        Map<String, Integer> topicCountMap = Collections.singletonMap(topicName, 2);
        Map<String, List<PulsarKafkaStream<byte[], byte[]>>> streams = connector.createMessageStreams(topicCountMap);

        // (2) Create producer
        Properties properties2 = new Properties();
        properties2.put(BROKER_URL, serviceUrl);
        properties2.put(PRODUCER_TYPE, "sync");
        properties2.put(PARTITIONER_CLASS, TestPartitioner.class.getName());
        properties2.put(COMPRESSION_CODEC, "gzip"); // compression: ZLIB
        properties2.put(QUEUE_ENQUEUE_TIMEOUT_MS, "-1"); // block queue if full => -1 = true
        properties2.put(QUEUE_BUFFERING_MAX_MESSAGES, "6000"); // queue max message
        properties2.put(QUEUE_BUFFERING_MAX_MS, "100"); // batch delay
        properties2.put(BATCH_NUM_MESSAGES, "500"); // batch msg
        properties2.put(CLIENT_ID, "test");
        ProducerConfig config = new ProducerConfig(properties2);
        PulsarKafkaProducer<byte[], byte[]> producer = new PulsarKafkaProducer<>(config);

        String name = "user";
        String msg = "Hello World!";
        int total = 10;
        for (int i = 0; i < total; i++) {
            String sendMessage = msg + i;
            KeyedMessage<byte[], byte[]> message = new KeyedMessage<>(topicName, name.getBytes(), sendMessage.getBytes());
            producer.send(message);
        }
        int count = 0;
        while (count < total) {
            for (int i = 0; i < streams.size(); i++) {
                List<PulsarKafkaStream<byte[], byte[]>> kafkaStreams = streams.get(topicName);
                assertEquals(kafkaStreams.size(), 2);
                for (PulsarKafkaStream<byte[], byte[]> kafkaStream : kafkaStreams) {
                    for (PulsarMessageAndMetadata<byte[], byte[]> record : kafkaStream) {
                        count++;
                        System.out.println(new String(record.message()));
                    }
                }
            }
        }
    }
}
