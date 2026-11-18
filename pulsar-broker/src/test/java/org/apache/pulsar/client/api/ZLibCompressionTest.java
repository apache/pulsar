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
package org.apache.pulsar.client.api;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.client.impl.ChunkMessageIdImpl;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.compression.CompressionCodecProvider;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-api")
public class ZLibCompressionTest extends SharedPulsarBaseTest {

    @DataProvider
    public Object[][] batchingVariants() {
        return new Object[][] {
                {false, BatcherBuilder.DEFAULT},
                {true, BatcherBuilder.DEFAULT},
                {true, BatcherBuilder.KEY_BASED}
        };
    }

    @Test(dataProvider = "batchingVariants", timeOut = 60000)
    public void testZLibSendAndReceive(boolean batching, BatcherBuilder batcherBuilder) throws Exception {
        String topic = newTopicName();
        int messageCount = 12;
        try (Consumer<byte[]> consumer = pulsarClient.newConsumer()
                     .topic(topic)
                     .subscriptionName("sub")
                     .poolMessages(true)
                     .subscribe();
             Producer<byte[]> producer = pulsarClient.newProducer()
                     .topic(topic)
                     .compressionType(CompressionType.ZLIB)
                     .compressionMinMsgBodySize(0)
                     .enableBatching(batching)
                     .batcherBuilder(batcherBuilder)
                     .batchingMaxMessages(messageCount * 2)
                     .batchingMaxBytes(1024 * 1024)
                     .batchingMaxPublishDelay(1, TimeUnit.HOURS)
                     .create()) {
            for (int round = 0; round < 2; round++) {
                List<byte[]> payloads = new ArrayList<>();
                List<CompletableFuture<MessageId>> sends = new ArrayList<>();
                for (int i = 0; i < messageCount; i++) {
                    byte[] payload = new byte[1 + i * 1024];
                    if (i % 2 == 0) {
                        Arrays.fill(payload, (byte) (round + i));
                    } else {
                        new Random(round * messageCount + i).nextBytes(payload);
                    }
                    payloads.add(payload);
                    sends.add(producer.newMessage()
                            .key("key-" + i % 2)
                            .property("index", Integer.toString(i))
                            .property("round", Integer.toString(round))
                            .value(payload)
                            .sendAsync());
                }
                producer.flush();
                for (CompletableFuture<MessageId> send : sends) {
                    send.get(10, TimeUnit.SECONDS);
                }

                Set<Integer> received = new HashSet<>();
                for (int i = 0; i < messageCount; i++) {
                    Message<byte[]> message = consumer.receive(10, TimeUnit.SECONDS);
                    assertThat(message).as("message %s in round %s", i, round).isNotNull();
                    try {
                        int index = Integer.parseInt(message.getProperty("index"));
                        assertThat(received.add(index)).as("each message is delivered once").isTrue();
                        assertThat(message.getProperty("round")).isEqualTo(Integer.toString(round));
                        assertThat(message.getKey()).isEqualTo("key-" + index % 2);
                        assertThat(message.getData()).isEqualTo(payloads.get(index));
                        assertZLibCompression(message);

                        MessageIdAdv messageId = (MessageIdAdv) message.getMessageId();
                        if (batching) {
                            int batchSize = batcherBuilder == BatcherBuilder.KEY_BASED
                                    ? messageCount / 2 : messageCount;
                            assertThat(messageId.getBatchSize()).isEqualTo(batchSize);
                            assertThat(messageId.getBatchIndex()).isBetween(0, batchSize - 1);
                        } else {
                            assertThat(messageId.getBatchIndex()).isEqualTo(-1);
                        }
                        consumer.acknowledge(message);
                    } finally {
                        message.release();
                    }
                }
                assertThat(received).hasSize(messageCount);
            }
        }
    }

    @Test(timeOut = 60000)
    public void testZLibChunkedSendAndReceive() throws Exception {
        String topic = newTopicName();
        try (Consumer<byte[]> consumer = pulsarClient.newConsumer()
                     .topic(topic)
                     .subscriptionName("sub")
                     .poolMessages(true)
                     .subscribe();
             Producer<byte[]> producer = pulsarClient.newProducer()
                     .topic(topic)
                     .compressionType(CompressionType.ZLIB)
                     .compressionMinMsgBodySize(0)
                     .enableBatching(false)
                     .enableChunking(true)
                     .chunkMaxMessageSize(4096)
                     .create()) {
            for (int i = 0; i < 2; i++) {
                byte[] payload = new byte[16 * 1024 + i];
                new Random(i).nextBytes(payload);
                MessageId sentId = producer.send(payload);
                assertThat(sentId).as("incompressible payload is split into chunks")
                        .isInstanceOf(ChunkMessageIdImpl.class);

                Message<byte[]> message = consumer.receive(10, TimeUnit.SECONDS);
                assertThat(message).isNotNull();
                try {
                    assertThat(message.getMessageId()).isEqualTo(sentId);
                    assertThat(message.getData()).isEqualTo(payload);
                    assertZLibCompression(message);
                    consumer.acknowledge(message);
                } finally {
                    message.release();
                }
            }
        }
    }

    private static void assertZLibCompression(Message<byte[]> message) {
        assertThat(((MessageImpl<?>) message).getMessageBuilder().getCompression())
                .as("the payload was compressed before being sent")
                .isEqualTo(CompressionCodecProvider.convertToWireProtocol(CompressionType.ZLIB));
    }
}
