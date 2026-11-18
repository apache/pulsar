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
package org.apache.pulsar.client.impl;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.pulsar.broker.BrokerTestUtil.spyWithClassAndConstructorArgsRecordingInvocations;
import static org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest.retryStrategically;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.CustomLog;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.AbstractDispatcherSingleActiveConsumer;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.broker.service.SharedPulsarCluster;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.api.schema.SchemaReader;
import org.apache.pulsar.client.api.schema.SchemaWriter;
import org.apache.pulsar.client.impl.HandlerState.State;
import org.apache.pulsar.client.impl.schema.SchemaDefinitionBuilderImpl;
import org.apache.pulsar.client.impl.schema.reader.JacksonJsonReader;
import org.apache.pulsar.client.impl.schema.writer.JacksonJsonWriter;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.protocol.PulsarHandler;
import org.awaitility.Awaitility;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-impl")
@CustomLog
public class BrokerClientIntegrationTest extends SharedPulsarBaseTest {

    private String methodName;
    private int activeConsumerFailoverDelayTimeMillis;

    @Override
    protected boolean checkClientHandlesClosed() {
        return true;
    }

    @BeforeMethod
    public void saveConfiguration(Method method) {
        methodName = method.getName();
        activeConsumerFailoverDelayTimeMillis = getConfig().getActiveConsumerFailoverDelayTimeMillis();
    }

    @AfterMethod(alwaysRun = true)
    public void restoreConfiguration() {
        // testResetCursor changes it; the broker reads it when a failover subscription changes its active consumer
        getConfig().setActiveConsumerFailoverDelayTimeMillis(activeConsumerFailoverDelayTimeMillis);
    }

    @DataProvider
    public Object[][] subType() {
        return new Object[][] { { SubscriptionType.Shared }, { SubscriptionType.Failover } };
    }

    @DataProvider(name = "booleanFlagProvider")
    public Object[][] booleanFlagProvider() {
        return new Object[][] { { true }, { false } };
    }

    /**
     * It verifies that consumer which doesn't support batch-message.
     * <p>
     * 1. broker disconnects that consumer
     * <p>
     * 2. redeliver all those messages to other supported consumer under the same subscription
     *
     * @param subType
     * @throws Exception
     */
    @Test(dataProvider = "subType")
    public void testUnsupportedBatchMessageConsumer(SubscriptionType subType) throws Exception {
        log.info().attr("method", methodName).log("Starting test");

        final String topicName = "persistent://" + getNamespace() + "/my-topic1";
        final String subscriptionName = "my-subscriber-name" + subType;
        // a client of its own: the test changes the protocol version of its connection on the broker
        @Cleanup
        PulsarClient client = newPulsarClient();

        ConsumerImpl<byte[]> consumer1 = (ConsumerImpl<byte[]>) client.newConsumer()
                .topic(topicName)
                .subscriptionName(subscriptionName)
                .subscriptionType(subType)
                .subscribe();

        final int numMessagesPerBatch = 10;

        Producer<byte[]> producer = client.newProducer()
                    .topic(topicName)
                    .enableBatching(false)
                    .create();
        Producer<byte[]> batchProducer = client.newProducer()
                .topic(topicName).enableBatching(true)
                .batchingMaxPublishDelay(Long.MAX_VALUE, TimeUnit.SECONDS)
                .batchingMaxMessages(numMessagesPerBatch)
                .create();

        // update consumer's version to incompatible batch-message version = Version.V3
        Topic topic = getPulsar().getBrokerService().getOrCreateTopic(topicName).get();
        org.apache.pulsar.broker.service.Consumer brokerConsumer = topic.getSubscriptions().get(subscriptionName)
                .getConsumers().get(0);
        Field cnxField = org.apache.pulsar.broker.service.Consumer.class.getDeclaredField("cnx");
        cnxField.setAccessible(true);
        PulsarHandler cnx = (PulsarHandler) cnxField.get(brokerConsumer);
        Field versionField = PulsarHandler.class.getDeclaredField("remoteEndpointProtocolVersion");
        versionField.setAccessible(true);
        versionField.set(cnx, 3);

        // (1) send non-batch message: consumer should be able to consume
        MessageId lastNonBatchedMessageId = null;
        for (int i = 0; i < numMessagesPerBatch; i++) {
            String message = "my-message-" + i;
            lastNonBatchedMessageId = producer.send(message.getBytes());
        }
        Set<String> messageSet = new HashSet<>();
        Message<byte[]> msg = null;
        for (int i = 0; i < numMessagesPerBatch; i++) {
            msg = consumer1.receive(1, TimeUnit.SECONDS);
            String receivedMessage = new String(msg.getData());
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
            consumer1.acknowledge(msg);
        }

        // Also set clientCnx of the consumer to null so, it avoid reconnection so, other consumer can consume for
        // verification
        consumer1.setClientCnx(null);
        // (2) send batch-message which should not be able to consume: as broker will disconnect the consumer
        for (int i = 0; i < numMessagesPerBatch; i++) {
            String message = "my-batch-message-" + i;
            batchProducer.sendAsync(message.getBytes());
        }
        batchProducer.flush();

        // consumer should have not received any message as it should have been disconnected
        msg = consumer1.receive(100, TimeUnit.MILLISECONDS);
        assertNull(msg);

        // subscribe consumer2 with supporting batch version
        @Cleanup
        PulsarClient newPulsarClient = PulsarClient.builder().serviceUrl(getWebServiceUrl())
                .build(); // Creates new client connection
        Consumer<byte[]> consumer2 = newPulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionName(subscriptionName)
                .subscriptionType(subType)
                .subscribe();
        consumer2.seek(lastNonBatchedMessageId);

        messageSet.clear();
        for (int i = 0; i < numMessagesPerBatch; i++) {
            msg = consumer2.receive();
            String receivedMessage = new String(msg.getData());
            log.debug().attr("message", receivedMessage).log("Received message");
            String expectedMessage = "my-batch-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
            consumer2.acknowledge(msg);
        }

        consumer2.close();
        producer.close();
        batchProducer.close();
        log.info().attr("method", methodName).log("Exiting test");
    }

    @Test(dataProvider = "subType")
    public void testResetCursor(SubscriptionType subType) throws Exception {
        final RetentionPolicies policy = new RetentionPolicies(60, 52 * 1024);
        final TopicName topicName = TopicName.get("persistent://" + getNamespace() + "/unacked-topic");
        final int warmup = 20;
        final int testSize = 150;
        final List<Message<byte[]>> received = new ArrayList<>();
        final String subsId = "sub";

        final NavigableMap<Long, TimestampEntryCount> publishTimeIdMap = new ConcurrentSkipListMap<>();

        // set delay time to start dispatching messages to active consumer in order to avoid message duplication
        getConfig().setActiveConsumerFailoverDelayTimeMillis(500);

        admin.namespaces().setRetention(topicName.getNamespace(), policy);

        ConsumerBuilder<byte[]> consumerBuilder = pulsarClient.newConsumer().topic(topicName.toString())
                .startMessageIdInclusive()
                .subscriptionName(subsId).subscriptionType(subType).messageListener((consumer, msg) -> {
                    try {
                        synchronized (received) {
                            received.add(msg);
                        }
                        consumer.acknowledge(msg);
                        long publishTime = msg.getPublishTime();
                        log.info(" publish time is " + publishTime + "," + msg.getMessageId());
                        TimestampEntryCount timestampEntryCount = publishTimeIdMap.computeIfAbsent(publishTime,
                                (k) -> new TimestampEntryCount(publishTime));
                        timestampEntryCount.incrementAndGet();
                    } catch (final PulsarClientException e) {
                        log.warn("Failed to ack!");
                    }
                });
        Consumer<byte[]> consumer1 = consumerBuilder.subscribe();
        Consumer<byte[]> consumer2 = consumerBuilder.subscribe();
        final Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName.toString()).create();

        log.info("warm up started for " + topicName.toString());
        // send warmup msgs
        byte[] msgBytes = new byte[1000];
        for (int i = 0; i < warmup; i++) {
            producer.send(msgBytes);
        }
        log.info("warm up finished.");

        // sleep to ensure receiving of msgs
        for (int n = 0; n < 10 && received.size() < warmup; n++) {
            Thread.sleep(200);
        }

        // validate received msgs
        Assert.assertEquals(received.size(), warmup);
        received.clear();

        // publish testSize num of msgs
        log.info("Sending more messages.");
        for (int n = 0; n < testSize; n++) {
            producer.send(msgBytes);
            Thread.sleep(1);
        }
        log.info("Sending more messages done.");

        Thread.sleep(3000);

        long begints = publishTimeIdMap.firstEntry().getKey();
        long endts = publishTimeIdMap.lastEntry().getKey();
        // find reset timestamp
        long timestamp = (endts - begints) / 2 + begints;
        timestamp = publishTimeIdMap.floorKey(timestamp);

        NavigableMap<Long, TimestampEntryCount> expectedMessages = new ConcurrentSkipListMap<>();
        expectedMessages.putAll(publishTimeIdMap.tailMap(timestamp, true));

        received.clear();

        log.info("reset cursor to " + timestamp + " for topic " + topicName.toString() + " for subs " + subsId);
        log.info("issuing admin operation on " + admin.getServiceUrl());
        List<String> subList = admin.topics().getSubscriptions(topicName.toString());
        for (String subs : subList) {
            log.info("got sub " + subs);
        }
        publishTimeIdMap.clear();
        // reset the cursor to this timestamp
        Assert.assertTrue(subList.contains(subsId));
        admin.topics().resetCursor(topicName.toString(), subsId, timestamp);

        Thread.sleep(3000);
        int totalExpected = 0;
        for (TimestampEntryCount tec : expectedMessages.values()) {
            totalExpected += tec.numMessages;
        }
        // validate that replay happens after the timestamp
        Assert.assertTrue(publishTimeIdMap.firstEntry().getKey() >= timestamp);
        consumer1.close();
        consumer2.close();
        producer.close();
        // validate that expected and received counts match
        int totalReceived = 0;
        for (TimestampEntryCount tec : publishTimeIdMap.values()) {
            totalReceived += tec.numMessages;
        }
        Assert.assertEquals(totalReceived, totalExpected, "did not receive all messages on replay after reset");
    }

    static class TimestampEntryCount {
        private final long timestamp;
        private int numMessages;

        public TimestampEntryCount(long ts) {
            this.numMessages = 0;
            this.timestamp = ts;
        }

        public int incrementAndGet() {
            return ++numMessages;
        }

        public long getTimestamp() {
            return timestamp;
        }
    }
    @SuppressWarnings("deprecation")

    @Test
    public void testCleanProducer() throws Exception {
        log.info().attr("method", methodName).log("Starting test");

        final int operationTimeOut = 500;
        @Cleanup
        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(getWebServiceUrl())
                .statsInterval(0, TimeUnit.SECONDS).operationTimeout(operationTimeOut, TimeUnit.MILLISECONDS).build();
        CountDownLatch latch = new CountDownLatch(1);
        pulsarClient.newProducer().topic("persistent://" + SharedPulsarCluster.TENANT_NAME + "/lookup/my-topic1")
                .createAsync()
                .handle((producer, e) -> {
                    latch.countDown();
                    return null;
                });

        latch.await(operationTimeOut + 1000, TimeUnit.MILLISECONDS);
        Field prodField = PulsarClientImpl.class.getDeclaredField("producers");
        prodField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Set<ProducerBase<byte[]>> producers = (Set<ProducerBase<byte[]>>) prodField
                .get(pulsarClient);
        assertTrue(producers.isEmpty());
        log.info().attr("method", methodName).log("Exiting test");
    }


    @Test
    @SuppressWarnings("unchecked")
    public void testAvroSchemaProducerConsumerWithSpecifiedReaderAndWriter() throws PulsarClientException {
        final String topicName = "persistent://" + getNamespace() + "/my-topic1";
        TestMessageObject object = new TestMessageObject();
        SchemaReader<TestMessageObject> reader = Mockito.mock(SchemaReader.class);
        SchemaWriter<TestMessageObject> writer = Mockito.mock(SchemaWriter.class);
        Mockito.when(reader.read(Mockito.any(InputStream.class),
                Mockito.any(byte[].class))).thenReturn(object);
        Mockito.when(writer.write(Mockito.any(TestMessageObject.class)))
                .thenReturn("fake data".getBytes(StandardCharsets.UTF_8));
        SchemaDefinition<TestMessageObject> schemaDefinition = new SchemaDefinitionBuilderImpl<TestMessageObject>()
                .withPojo(TestMessageObject.class)
                .withSchemaReader(reader)
                .withSchemaWriter(writer)
                .build();
        Schema<TestMessageObject> schema = Schema.AVRO(schemaDefinition);

        try (PulsarClient client = PulsarClient.builder()
                .serviceUrl(getWebServiceUrl())
                .build(); Producer<TestMessageObject> producer = client.newProducer(schema).topic(topicName).create();
             Consumer<TestMessageObject> consumer =
                     client.newConsumer(schema).topic(topicName).subscriptionName("my-subscriber-name").subscribe()) {

            assertNotNull(producer);
            assertNotNull(consumer);
            producer.newMessage().value(object).send();
            TestMessageObject testObject = consumer.receive().getValue();
            Assert.assertEquals(object.getValue(), testObject.getValue());
            Mockito.verify(writer, Mockito.times(1)).write(Mockito.any());
            Mockito.verify(reader, Mockito.times(1)).read(Mockito.any(InputStream.class), Mockito.any(byte[].class));
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testJsonSchemaProducerConsumerWithSpecifiedReaderAndWriter() throws PulsarClientException {
        final String topicName = "persistent://" + getNamespace() + "/my-topic1";
        ObjectMapper mapper = new ObjectMapper();
        SchemaReader<TestMessageObject> reader =
                spyWithClassAndConstructorArgsRecordingInvocations(JacksonJsonReader.class, mapper,
                        TestMessageObject.class);
        SchemaWriter<TestMessageObject> writer =
                spyWithClassAndConstructorArgsRecordingInvocations(JacksonJsonWriter.class, mapper);

        SchemaDefinition<TestMessageObject> schemaDefinition = new SchemaDefinitionBuilderImpl<TestMessageObject>()
                .withPojo(TestMessageObject.class)
                .withSchemaReader(reader)
                .withSchemaWriter(writer)
                .build();
        Schema<TestMessageObject> schema = Schema.JSON(schemaDefinition);

        try (PulsarClient client = PulsarClient.builder()
                .serviceUrl(getWebServiceUrl())
                .build(); Producer<TestMessageObject> producer = client.newProducer(schema).topic(topicName).create();
             Consumer<TestMessageObject> consumer = client.newConsumer(schema).topic(topicName)
                     .subscriptionName("my-subscriber-name").subscribe()) {

            assertNotNull(producer);
            assertNotNull(consumer);

            TestMessageObject object = new TestMessageObject();
            object.setValue("fooooo");
            producer.newMessage().value(object).send();

            TestMessageObject testObject = consumer.receive().getValue();
            Assert.assertEquals(object.getValue(), testObject.getValue());

            Mockito.verify(writer, Mockito.times(1)).write(Mockito.any());
            Mockito.verify(reader, Mockito.times(1)).read(Mockito.any(InputStream.class));
        }
    }

    @Getter
    @Setter
    @EqualsAndHashCode
    private static final class TestMessageObject{
        private String value;
    }

    /**
     * It validates pooled message consumption for batch and non-batch messages.
     *
     * @throws Exception
     */
    @Test(dataProvider = "booleanFlagProvider")
    public void testConsumerWithPooledMessages(boolean isBatchingEnabled) throws Exception {
        log.info().attr("method", methodName).log("Starting test");

        @Cleanup
        PulsarClient newPulsarClient = PulsarClient.builder().serviceUrl(getWebServiceUrl()).build();

        final String topic = "persistent://" + getNamespace() + "/testConsumerWithPooledMessages" + isBatchingEnabled;

        @Cleanup
        Consumer<ByteBuffer> consumer = newPulsarClient.newConsumer(Schema.BYTEBUFFER).topic(topic)
                .subscriptionName("my-sub").poolMessages(true).subscribe();

        @Cleanup
        Producer<byte[]> producer = newPulsarClient.newProducer().topic(topic)
                .enableBatching(isBatchingEnabled).create();

        final int numMessages = 100;
        for (int i = 0; i < numMessages; i++) {
            producer.newMessage().value(("value-" + i).getBytes(UTF_8))
            .eventTime((i + 1) * 100L).sendAsync();
        }
        producer.flush();

        // Reuse pre-allocated pooled buffer to process every message
        byte[] val = null;
        int size = 0;
        for (int i = 0; i < numMessages; i++) {
            Message<ByteBuffer> msg = consumer.receive();
            ByteBuffer value;
            try {
                value = msg.getValue();
                int capacity = value.remaining();
                // expand the size of buffer if needed
                if (capacity > size) {
                    val = new byte[capacity];
                    size = capacity;
                }
                // read message into pooled buffer
                value.get(val, 0, capacity);
                // process the message
                assertEquals(("value-" + i), new String(val, 0, capacity));
            } finally {
                msg.release();
            }
        }
        consumer.close();
        producer.close();
    }

    /**
     * It verifies that expiry/redelivery of messages relesaes the messages without leak.
     *
     * @param isBatchingEnabled
     * @throws Exception
     */
    @Test(dataProvider = "booleanFlagProvider")
    @SuppressWarnings("unchecked")
    public void testPooledMessageWithAckTimeout(boolean isBatchingEnabled) throws Exception {
        log.info().attr("method", methodName).log("Starting test");

        @Cleanup
        PulsarClient newPulsarClient = PulsarClient.builder().serviceUrl(getWebServiceUrl()).build();

        final String topic = "persistent://" + getNamespace() + "/testPooledMessageWithAckTimeout" + isBatchingEnabled;

        @Cleanup
        ConsumerImpl<ByteBuffer> consumer = (ConsumerImpl<ByteBuffer>) newPulsarClient.newConsumer(Schema.BYTEBUFFER)
                .topic(topic).subscriptionName("my-sub").poolMessages(true).subscribe();

        @Cleanup
        Producer<byte[]> producer = newPulsarClient.newProducer().topic(topic).enableBatching(isBatchingEnabled)
                .create();

        final int numMessages = 100;
        for (int i = 0; i < numMessages; i++) {
            producer.newMessage().value(("value-" + i).getBytes(UTF_8)).eventTime((i + 1) * 100L).sendAsync();
        }
        producer.flush();

        retryStrategically((test) -> consumer.incomingMessages.peek() != null, 5, 500);
        MessageImpl<ByteBuffer> msg = (MessageImpl) consumer.incomingMessages.peek();
        assertNotNull(msg);
        ByteBuf payload = ((MessageImpl) msg).getPayload();
        assertNotEquals(payload.refCnt(), 0);
        consumer.redeliverUnacknowledgedMessages();
        Awaitility.await().untilAsserted(() -> {
            assertTrue(consumer.incomingMessages.size() >= 100);
        });
        consumer.close();
        producer.close();
        admin.topics().delete(topic, false);
        assertEquals(consumer.incomingMessages.size(), 0);
        assertEquals(payload.refCnt(), 0);
    }

    /**
     * It validates pooled message consumption for batch and non-batch messages.
     *
     * @throws Exception
     */
    @Test(dataProvider = "booleanFlagProvider")
    public void testConsumerWithPooledMessagesWithReader(boolean isBatchingEnabled) throws Exception {
        log.info().attr("method", methodName).log("Starting test");

        @Cleanup
        PulsarClient newPulsarClient = PulsarClient.builder().serviceUrl(getWebServiceUrl()).build();

        final String topic = "persistent://" + getNamespace() + "/testConsumerWithPooledMessages" + isBatchingEnabled;

        @Cleanup
        Reader<ByteBuffer> reader = newPulsarClient.newReader(Schema.BYTEBUFFER).topic(topic).poolMessages(true)
                .startMessageId(MessageId.latest).create();

        @Cleanup
        Producer<byte[]> producer = newPulsarClient.newProducer().topic(topic)
                .enableBatching(isBatchingEnabled).create();

        final int numMessages = 100;
        for (int i = 0; i < numMessages; i++) {
            producer.newMessage().value(("value-" + i).getBytes(UTF_8))
            .eventTime((i + 1) * 100L).sendAsync();
        }
        producer.flush();

        // Reuse pre-allocated pooled buffer to process every message
        byte[] val = null;
        int size = 0;
        for (int i = 0; i < numMessages; i++) {
            Message<ByteBuffer> msg = reader.readNext();
            ByteBuffer value;
            try {
                value = msg.getValue();
                int capacity = value.remaining();
                // expand the size of buffer if needed
                if (capacity > size) {
                    val = new byte[capacity];
                    size = capacity;
                }
                // read message into pooled buffer
                value.get(val, 0, capacity);
                // process the message
                assertEquals(("value-" + i), new String(val, 0, capacity));
                assertTrue(value.isDirect());
            } finally {
                msg.release();
            }
        }
        reader.close();
        producer.close();
    }

    @Test
    public void testActiveConsumerCleanup() throws Exception {
        log.info().attr("method", methodName).log("Starting test");

        int numMessages = 100;
        final CountDownLatch latch = new CountDownLatch(numMessages);
        String topic = BrokerTestUtil.newUniqueName("persistent://" + getNamespace() + "/closed-cnx-topic");
        admin.topics().createNonPartitionedTopic(topic);
        String sub = "my-subscriber-name";
        @Cleanup
        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(getWebServiceUrl()).build();
        ConsumerImpl c =
            (ConsumerImpl) pulsarClient.newConsumer().topic(topic).subscriptionName(sub).messageListener((c1, msg) -> {
                Assert.assertNotNull(msg, "Message cannot be null");
                String receivedMessage = new String(msg.getData());
                log.debug().attr("message", receivedMessage).log("Received message in the listener");
                c1.acknowledgeAsync(msg);
                latch.countDown();
            }).subscribe();
        PersistentTopic topicRef = (PersistentTopic) getTopicReference(topic).get();
        AbstractDispatcherSingleActiveConsumer dispatcher = (AbstractDispatcherSingleActiveConsumer) topicRef
                .getSubscription(sub).getDispatcher();
        assertNotNull(dispatcher.getActiveConsumer());

        // Inject an blocker to make the "ping & pong" does not work.
        CountDownLatch countDownLatch = new CountDownLatch(1);
        ConnectionHandler connectionHandler = c.getConnectionHandler();
        ClientCnx clientCnx = connectionHandler.cnx();
        clientCnx.ctx().executor().submit(() -> {
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        @Cleanup
        PulsarClient pulsarClient2 = PulsarClient.builder().serviceUrl(getWebServiceUrl()).build();
        Consumer<byte[]> consumer = null;
        for (int i = 0; i < 2; i++) {
            try {
                consumer = pulsarClient2.newConsumer().topic(topic).subscriptionName(sub).messageListener((c1, msg) -> {
                    Assert.assertNotNull(msg, "Message cannot be null");
                    String receivedMessage = new String(msg.getData());
                    log.debug().attr("message", receivedMessage).log("Received message in the listener");
                    c1.acknowledgeAsync(msg);
                    latch.countDown();
                }).subscribe();
            } catch (PulsarClientException.ConsumerBusyException ignore) {
               // It's ok.
            }
        }
        assertNotNull(consumer);
        log.info().attr("method", methodName).log("Exiting test");

        // cleanup.
        countDownLatch.countDown();
        consumer.close();
        pulsarClient.close();
        pulsarClient2.close();
        admin.topics().delete(topic, false);
    }

    @Test
    public void testManagedLedgerLazyCursorLedgerCreation() throws Exception {
        String topic = "persistent://" + getNamespace() + "/testManagedLedgerLazyCursorLedgerCreationEnabled";
        String sub = "my-subscriber-name";

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).create();
        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName(sub).subscribe();
        PersistentTopicInternalStats stats = admin.topics().getInternalStats(topic);
        assertEquals(stats.cursors.get(sub).state, "NoLedger");
        producer.send("test".getBytes(UTF_8));
        consumer.acknowledgeCumulative(consumer.receive());
        Awaitility.await().untilAsserted(() -> {
            PersistentTopicInternalStats stats1 = admin.topics().getInternalStats(topic);
            assertEquals(stats1.cursors.get(sub).state, "Open");
            assertEquals(stats1.lastConfirmedEntry, stats1.cursors.get(sub).markDeletePosition);
        });
    }

    @Test
    public void testSharedConsumerUnsubscribe() throws Exception {
        String topic = "persistent://" + getNamespace() + "/sharedUnsubscribe";
        String sub = "my-subscriber-name";
        @Cleanup
        Consumer<byte[]> consumer1 = pulsarClient.newConsumer().topic(topic).subscriptionType(SubscriptionType.Shared)
                .subscriptionName(sub).subscribe();
        @Cleanup
        Consumer<byte[]> consumer2 = pulsarClient.newConsumer().topic(topic).subscriptionType(SubscriptionType.Shared)
                .subscriptionName(sub).subscribe();
        try {
            consumer1.unsubscribe();
            fail("should have failed as consumer-2 is already connected");
        } catch (Exception e) {
            // Ok
        }

        consumer1.unsubscribe(true);
        try {
            consumer2.unsubscribe(true);
        } catch (PulsarClientException.NotConnectedException e) {
            // Ok. consumer-2 is already disconnected with force unsubscription
        }
        assertFalse(consumer1.isConnected());
        assertFalse(consumer2.isConnected());
    }

    @Test(dataProvider = "subType")
    public void testUnsubscribeForce(SubscriptionType type) throws Exception {
        String topic = "persistent://" + getNamespace() + "/sharedUnsubscribe";
        String sub = "my-subscriber-name";
        @Cleanup
        Consumer<byte[]> consumer1 = pulsarClient.newConsumer().topic(topic).subscriptionType(type)
                .subscriptionName(sub).subscribe();
        consumer1.unsubscribe(true);
        assertFalse(consumer1.isConnected());
    }

    /**
     * Regression test for https://github.com/apache/pulsar/issues/25997: a broker-assigned redirect
     * ({@code CloseProducer}/{@code CloseConsumer} carrying an {@code assignedBrokerServiceUrl})
     * pointing at a wrong or unreachable broker must be honored for the immediate reconnect attempt
     * only. If that attempt fails, subsequent retries must fall back to topic lookup instead of
     * staying pinned to the stale address.
     */
    @Test
    public void testStaleBrokerRedirectFallsBackToLookup() throws Exception {
        String topic = "persistent://" + getNamespace() + "/staleRedirectFallback";
        @Cleanup
        Producer<byte[]> producerInstance = pulsarClient.newProducer().topic(topic).create();
        @Cleanup
        Consumer<byte[]> consumerInstance = pulsarClient.newConsumer().topic(topic)
                .subscriptionName("my-subscriber-name").subscribe();
        ProducerImpl<byte[]> producer = (ProducerImpl<byte[]>) producerInstance;
        ConsumerImpl<byte[]> consumer = (ConsumerImpl<byte[]>) consumerInstance;

        // An address with no listener behind it: dialing the redirect fails immediately.
        URI unreachable;
        try (ServerSocket socket = new ServerSocket(0)) {
            unreachable = URI.create("pulsar://127.0.0.1:" + socket.getLocalPort());
        }

        // Deliver the disconnect+redirect exactly as ClientCnx#handleCloseProducer and
        // #handleCloseConsumer do for a broker unload notification.
        producer.connectionClosed(producer.getClientCnx(), Optional.of(0L), Optional.of(unreachable));
        consumer.connectionClosed(consumer.getClientCnx(), Optional.of(0L), Optional.of(unreachable));

        // The redirect dial fails; the clients must recover by looking up the topic again.
        Awaitility.await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals(producer.getState(), State.Ready);
            assertEquals(consumer.getState(), State.Ready);
        });

        producer.send("msg".getBytes(UTF_8));
        assertNotNull(consumer.receive(10, TimeUnit.SECONDS));
    }

    private <T> void testMessageOrderAndDuplicates(Set<T> messagesReceived, T receivedMessage,
            T expectedMessage) {
        // Make sure that messages are received in order
        Assert.assertEquals(receivedMessage, expectedMessage,
                "Received message " + receivedMessage + " did not match the expected message " + expectedMessage);

        // Make sure that there are no duplicates
        Assert.assertTrue(messagesReceived.add(receivedMessage), "Received duplicate message " + receivedMessage);
    }
}
