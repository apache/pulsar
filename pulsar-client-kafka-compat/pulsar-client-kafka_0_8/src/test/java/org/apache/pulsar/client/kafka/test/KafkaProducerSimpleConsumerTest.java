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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.producer.PulsarKafkaProducer;
import org.apache.kafka.clients.simple.consumer.PulsarKafkaSimpleConsumer;
import org.apache.kafka.clients.simple.consumer.PulsarMsgAndOffset;
import org.apache.kafka.clients.simple.consumer.PulsarOffsetCommitRequest;
import org.apache.kafka.clients.simple.consumer.PulsarOffsetFetchRequest;
import org.apache.kafka.clients.simple.consumer.PulsarOffsetMetadataAndError;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.util.MessageIdUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.base.Objects;
import com.google.common.collect.Sets;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.common.OffsetMetadataAndError;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.message.MessageAndOffset;
import kafka.producer.KeyedMessage;
import kafka.producer.Partitioner;
import kafka.producer.ProducerConfig;
import kafka.serializer.Decoder;
import kafka.serializer.Encoder;
import kafka.serializer.StringEncoder;

public class KafkaProducerSimpleConsumerTest extends ProducerConsumerBase {

    private static final String BROKER_URL = "metadata.broker.list";
    private static final String PRODUCER_TYPE = "producer.type";
    private static final String KEY_SERIALIZER_CLASS = "key.serializer.class";
    private static final String PARTITIONER_CLASS = "partitioner.class";
    private static final String COMPRESSION_CODEC = "compression.codec";
    private static final String QUEUE_BUFFERING_MAX_MS = "queue.buffering.max.ms";
    private static final String QUEUE_BUFFERING_MAX_MESSAGES = "queue.buffering.max.messages";
    private static final String QUEUE_ENQUEUE_TIMEOUT_MS = "queue.enqueue.timeout.ms";
    private static final String BATCH_NUM_MESSAGES = "batch.num.messages";
    private static final String CLIENT_ID = "client.id";
    
    private final static int publishPartition = 1;
    
    @DataProvider(name = "partitions")
    public Object[][] totalPartitions() {
        return new Object[][] { { 0 }, { 10 } };
    }

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

    @Test(dataProvider="partitions")
    public void testPulsarKafkaProducerWithSerializer(int partitions) throws Exception {
        final String serviceUrl = lookupUrl.toString();
        final String topicName = "persistent://my-property/my-ns/my-topic";
        final String groupId = "group1";
        
        int partition = -1;
        if (partitions > 0) {
            admin.topics().createPartitionedTopic(topicName, 10);
            partition = publishPartition;
        }
        
        // create subscription
        Consumer<byte[]> cons = pulsarClient.newConsumer().topic(topicName).subscriptionName(groupId).subscribe();
        cons.close();

        // (2) Create producer
        Properties properties2 = new Properties();
        properties2.put(BROKER_URL, serviceUrl);
        properties2.put(PRODUCER_TYPE, "sync");
        properties2.put(KEY_SERIALIZER_CLASS, StringEncoder.class.getName());
        properties2.put(PARTITIONER_CLASS, TestPartitioner.class.getName());
        properties2.put(COMPRESSION_CODEC, "gzip"); // compression: ZLIB
        properties2.put(QUEUE_ENQUEUE_TIMEOUT_MS, "-1"); // block queue if full => -1 = true
        properties2.put(QUEUE_BUFFERING_MAX_MESSAGES, "6000"); // queue max message
        properties2.put(QUEUE_BUFFERING_MAX_MS, "100"); // batch delay
        properties2.put(BATCH_NUM_MESSAGES, "500"); // batch msg
        properties2.put(CLIENT_ID, "test");
        ProducerConfig config = new ProducerConfig(properties2);
        PulsarKafkaProducer<String, byte[]> producer = new PulsarKafkaProducer<>(config);

        String name = "user";
        String msg = "Hello World!";
        Set<String> published = Sets.newHashSet();
        Set<String> received = Sets.newHashSet();
        int total = 10;
        for (int i = 0; i < total; i++) {
            String sendMessage = msg + i;
            KeyedMessage<String, byte[]> message = new KeyedMessage<>(topicName, name, sendMessage.getBytes());
            published.add(sendMessage);
            producer.send(message);
        }
        
        
        // (2) Consume using simple consumer
        PulsarKafkaSimpleConsumer consumer = new PulsarKafkaSimpleConsumer(serviceUrl, 0, 0, 0, "clientId");
        List<String> topics = Collections.singletonList(topicName);
        TopicMetadataRequest req = new TopicMetadataRequest(topics);
        kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);
        
        List<TopicMetadata> metaData = resp.topicsMetadata();
        PartitionMetadata part = metaData.get(0).partitionsMetadata().get(0);
        
        long readOffset = kafka.api.OffsetRequest.EarliestTime();
        FetchRequest fReq = new FetchRequestBuilder()
                .clientId("c1")
                .addFetch(topicName, partition, readOffset, 100000)
                .build();
        FetchResponse fetchResponse = consumer.fetch(fReq);
        
        long lastOffset = 0;
        MessageId offset = null;
        for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(topicName, partition)) {
            long currentOffset = messageAndOffset.offset();
            if (currentOffset < readOffset) {
                continue;
            }
            offset = ((PulsarMsgAndOffset)messageAndOffset).getFullOffset();
            lastOffset = messageAndOffset.offset();
            ByteBuffer payload = messageAndOffset.message().payload();

            byte[] bytes = new byte[payload.limit()];
            payload.get(bytes);
            received.add(new String(bytes, StandardCharsets.UTF_8));
        }
        lastOffset -= 1;
        
        assertEquals(published.size(), received.size());
        published.removeAll(received);
        assertTrue(published.isEmpty());
        
        TopicAndPartition topicPartition = new TopicAndPartition(topicName, partition);
        PulsarOffsetMetadataAndError offsetError = new PulsarOffsetMetadataAndError(offset, null, (short) 0);
        Map<TopicAndPartition, PulsarOffsetMetadataAndError> requestInfo = Collections.singletonMap(topicPartition,
                offsetError);
        PulsarOffsetCommitRequest offsetReq = new PulsarOffsetCommitRequest(groupId, requestInfo, (short) -1, 0, "c1");
        consumer.commitOffsets(offsetReq);
        
        final long expectedReadOffsetPosition = lastOffset;
        
        retryStrategically((test) -> fetchOffset(consumer, topicPartition, groupId) == expectedReadOffsetPosition, 10, 150);
        
        long offset1 = fetchOffset(consumer, topicPartition, groupId);
        MessageIdImpl actualMsgId = ((MessageIdImpl)MessageIdUtils.getMessageId(offset1));
        
        MessageIdImpl expectedMsgId = (MessageIdImpl) offset;
        assertEquals(actualMsgId.getLedgerId(), expectedMsgId.getLedgerId());
        assertEquals(actualMsgId.getEntryId(), expectedMsgId.getEntryId() + 1);
    }

    private long fetchOffset(PulsarKafkaSimpleConsumer consumer, TopicAndPartition topicPartition, String groupId) {
        List<TopicAndPartition> fetchReqInfo = Collections.singletonList(topicPartition);
        PulsarOffsetFetchRequest fetchOffsetRequest = new PulsarOffsetFetchRequest(groupId, fetchReqInfo, (short)-1, 0, "test");
        OffsetMetadataAndError offsetResponse = consumer.fetchOffsets(fetchOffsetRequest).offsets().get(topicPartition);
        return offsetResponse.offset();
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
            //return obj.hashCode() % totalPartition;
            return publishPartition;
        }
    }

}
