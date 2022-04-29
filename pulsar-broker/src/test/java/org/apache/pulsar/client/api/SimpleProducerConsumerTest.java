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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.pulsar.common.naming.TopicName.PARTITIONED_TOPIC_SUFFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.Timeout;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.avro.Schema.Parser;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.mledger.impl.EntryCacheImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.ConsumerBase;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.MultiTopicsConsumerImpl;
import org.apache.pulsar.client.impl.PartitionedProducerImpl;
import org.apache.pulsar.client.impl.TopicMessageImpl;
import org.apache.pulsar.client.impl.TypedMessageBuilderImpl;
import org.apache.pulsar.client.impl.crypto.MessageCryptoBc;
import org.apache.pulsar.client.impl.schema.writer.AvroWriter;
import org.apache.pulsar.common.api.EncryptionContext;
import org.apache.pulsar.common.api.EncryptionContext.EncryptionKey;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.api.proto.SingleMessageMetadata;
import org.apache.pulsar.common.compression.CompressionCodec;
import org.apache.pulsar.common.compression.CompressionCodecProvider;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.common.util.FutureUtil;
import org.awaitility.Awaitility;
import org.powermock.reflect.Whitebox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "flaky")
public class SimpleProducerConsumerTest extends ProducerConsumerBase {
    private static final Logger log = LoggerFactory.getLogger(SimpleProducerConsumerTest.class);
    private static final int TIMEOUT_MULTIPLIER = Integer.getInteger("SimpleProducerConsumerTest.receive.timeout.multiplier", 1);
    private static final int RECEIVE_TIMEOUT_SECONDS = 5 * TIMEOUT_MULTIPLIER;
    private static final int RECEIVE_TIMEOUT_SHORT_MILLIS = 200 * TIMEOUT_MULTIPLIER;
    private static final int RECEIVE_TIMEOUT_MEDIUM_MILLIS = 1000 * TIMEOUT_MULTIPLIER;

    @BeforeMethod(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testRedeliveryFailOverConsumer() throws Exception {
        boolean ackReceiptEnabled = false;

        log.info("-- Starting {} test --", methodName);

        final int receiverQueueSize = 10;

        String topic = "persistent://my-property/my-ns/unacked-topic";

        // Only subscribe consumer
        ConsumerImpl<byte[]> consumer = (ConsumerImpl<byte[]>) pulsarClient.newConsumer()
                .topic(topic).subscriptionName("subscriber-1")
                .receiverQueueSize(receiverQueueSize).subscriptionType(SubscriptionType.Failover)
                .isAckReceiptEnabled(ackReceiptEnabled)
                .acknowledgmentGroupTime(0, TimeUnit.SECONDS).subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic)
                .create();

        // (1) First round to produce-consume messages
        int consumeMsgInParts = 4;
        for (int i = 0; i < receiverQueueSize; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }
        producer.flush();
        // (1.a) consume first consumeMsgInParts msgs and trigger redeliver
        Message<byte[]> msg;
        List<Message<byte[]>> messages1 = Lists.newArrayList();
        for (int i = 0; i < consumeMsgInParts; i++) {
            msg = consumer.receive(RECEIVE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (msg != null) {
                messages1.add(msg);
                consumer.acknowledge(msg);
                log.info("Received message: " + new String(msg.getData()));
            } else {
                break;
            }
        }
        assertEquals(messages1.size(), consumeMsgInParts);
        consumer.redeliverUnacknowledgedMessages();
        Thread.sleep(1000L);

        // (1.b) consume second consumeMsgInParts msgs and trigger redeliver
        messages1.clear();
        for (int i = 0; i < consumeMsgInParts; i++) {
            msg = consumer.receive(RECEIVE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (msg != null) {
                messages1.add(msg);
                consumer.acknowledge(msg);
                log.info("Received message: " + new String(msg.getData()));
            } else {
                break;
            }
        }
        assertEquals(messages1.size(), consumeMsgInParts);
        consumer.redeliverUnacknowledgedMessages();
        Thread.sleep(1000L);

        // (2) Second round to produce-consume messages
        for (int i = 0; i < receiverQueueSize; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }
        producer.flush();

        int remainingMsgs = (2 * receiverQueueSize) - (2 * consumeMsgInParts);
        messages1.clear();
        for (int i = 0; i < remainingMsgs; i++) {
            msg = consumer.receive(RECEIVE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (msg != null) {
                messages1.add(msg);
                consumer.acknowledge(msg);
                log.info("Received message: " + new String(msg.getData()));
            } else {
                break;
            }
        }
        assertEquals(messages1.size(), remainingMsgs);

        producer.close();
        consumer.close();
        log.info("-- Exiting {} test --", methodName);
    }

    @Test
    public void testRedeliveryFailOverConsumer1() throws Exception {
        boolean ackReceiptEnabled = true;

        log.info("-- Starting {} test --", methodName);

        final int receiverQueueSize = 10;

        String topic = "persistent://my-property/my-ns/unacked-topic";

        // Only subscribe consumer
        ConsumerImpl<byte[]> consumer = (ConsumerImpl<byte[]>) pulsarClient.newConsumer()
                .topic(topic).subscriptionName("subscriber-1")
                .receiverQueueSize(receiverQueueSize).subscriptionType(SubscriptionType.Failover)
                .isAckReceiptEnabled(ackReceiptEnabled)
                .acknowledgmentGroupTime(0, TimeUnit.SECONDS).subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic)
                .create();

        // (1) First round to produce-consume messages
        int consumeMsgInParts = 4;
        for (int i = 0; i < receiverQueueSize; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }
        producer.flush();
        // (1.a) consume first consumeMsgInParts msgs and trigger redeliver
        Message<byte[]> msg;
        List<Message<byte[]>> messages1 = Lists.newArrayList();
        for (int i = 0; i < consumeMsgInParts; i++) {
            msg = consumer.receive(RECEIVE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (msg != null) {
                messages1.add(msg);
                consumer.acknowledge(msg);
                log.info("Received message: " + new String(msg.getData()));
            } else {
                break;
            }
        }
        assertEquals(messages1.size(), consumeMsgInParts);
        consumer.redeliverUnacknowledgedMessages();
        Thread.sleep(1000L);

        // (1.b) consume second consumeMsgInParts msgs and trigger redeliver
        messages1.clear();
        for (int i = 0; i < consumeMsgInParts; i++) {
            msg = consumer.receive(RECEIVE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (msg != null) {
                messages1.add(msg);
                consumer.acknowledge(msg);
                log.info("Received message: " + new String(msg.getData()));
            } else {
                break;
            }
        }
        assertEquals(messages1.size(), consumeMsgInParts);
        consumer.redeliverUnacknowledgedMessages();
        Thread.sleep(1000L);

        // (2) Second round to produce-consume messages
        for (int i = 0; i < receiverQueueSize; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }
        producer.flush();

        int remainingMsgs = (2 * receiverQueueSize) - (2 * consumeMsgInParts);
        messages1.clear();
        for (int i = 0; i < remainingMsgs; i++) {
            msg = consumer.receive(RECEIVE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (msg != null) {
                messages1.add(msg);
                consumer.acknowledge(msg);
                log.info("Received message: " + new String(msg.getData()));
            } else {
                break;
            }
        }
        assertEquals(messages1.size(), remainingMsgs);

        producer.close();
        consumer.close();
        log.info("-- Exiting {} test --", methodName);
    }




}
