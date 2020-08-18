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
package org.apache.pulsar.client.impl;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.impl.ProducerImpl.OpSendMsg;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.protocol.ByteBufPair;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Commands.ChecksumType;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MessageIdTest extends BrokerTestBase {
    private static final Logger log = LoggerFactory.getLogger(MessageIdTest.class);

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        baseSetup();
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        internalCleanup();
    }

    @Test(timeOut = 10000)
    public void producerSendAsync() throws PulsarClientException {
        // 1. Basic Config
        String key = "producerSendAsync";
        final String topicName = "persistent://prop/cluster/namespace/topic-" + key;
        final String subscriptionName = "my-subscription-" + key;
        final String messagePredicate = "my-message-" + key + "-";
        final int numberOfMessages = 30;

        // 2. Create Producer
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        // 3. Create Consumer
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subscriptionName)
                .subscribe();

        // 4. Publish message and get message id
        Set<MessageId> messageIds = new HashSet<>();
        List<Future<MessageId>> futures = new ArrayList<>();
        for (int i = 0; i < numberOfMessages; i++) {
            String message = messagePredicate + i;
            futures.add(producer.sendAsync(message.getBytes()));
        }

        MessageIdImpl previousMessageId = null;
        for (Future<MessageId> f : futures) {
            try {
                MessageIdImpl currentMessageId = (MessageIdImpl) f.get();
                if (previousMessageId != null) {
                    Assert.assertTrue(currentMessageId.compareTo(previousMessageId) > 0,
                            "Message Ids should be in ascending order");
                }
                messageIds.add(currentMessageId);
                previousMessageId = currentMessageId;
            } catch (Exception e) {
                Assert.fail("Failed to publish message, Exception: " + e.getMessage());
            }
        }

        // 4. Check if message Ids are correct
        log.info("Message IDs = " + messageIds);
        Assert.assertEquals(messageIds.size(), numberOfMessages, "Not all messages published successfully");

        for (int i = 0; i < numberOfMessages; i++) {
            Message<byte[]> message = consumer.receive();
            Assert.assertEquals(new String(message.getData()), messagePredicate + i);
            MessageId messageId = message.getMessageId();
            Assert.assertTrue(messageIds.remove(messageId), "Failed to receive message");
        }
        log.info("Message IDs = " + messageIds);
        Assert.assertEquals(messageIds.size(), 0, "Not all messages received successfully");
        consumer.unsubscribe();
    }

    @Test(timeOut = 10000)
    public void producerSend() throws PulsarClientException {
        // 1. Basic Config
        String key = "producerSend";
        final String topicName = "persistent://prop/cluster/namespace/topic-" + key;
        final String subscriptionName = "my-subscription-" + key;
        final String messagePredicate = "my-message-" + key + "-";
        final int numberOfMessages = 30;

        // 2. Create Producer
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();

        // 3. Create Consumer
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subscriptionName)
                .subscribe();

        // 4. Publish message and get message id
        Set<MessageId> messageIds = new HashSet<>();
        for (int i = 0; i < numberOfMessages; i++) {
            String message = messagePredicate + i;
            messageIds.add(producer.send(message.getBytes()));
        }

        // 4. Check if message Ids are correct
        log.info("Message IDs = " + messageIds);
        Assert.assertEquals(messageIds.size(), numberOfMessages, "Not all messages published successfully");

        for (int i = 0; i < numberOfMessages; i++) {
            Assert.assertTrue(messageIds.remove(consumer.receive().getMessageId()), "Failed to receive Message");
        }
        log.info("Message IDs = " + messageIds);
        Assert.assertEquals(messageIds.size(), 0, "Not all messages received successfully");
        consumer.unsubscribe();
        ;
    }

    @Test(timeOut = 10000)
    public void partitionedProducerSendAsync() throws PulsarClientException, PulsarAdminException {
        // 1. Basic Config
        String key = "partitionedProducerSendAsync";
        final String topicName = "persistent://prop/cluster/namespace/topic-" + key;
        final String subscriptionName = "my-subscription-" + key;
        final String messagePredicate = "my-message-" + key + "-";
        final int numberOfMessages = 30;
        int numberOfPartitions = 3;
        admin.topics().createPartitionedTopic(topicName, numberOfPartitions);

        // 2. Create Producer
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        // 3. Create Consumer
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subscriptionName)
                .subscribe();

        // 4. Publish message and get message id
        Set<MessageId> messageIds = new HashSet<>();
        Set<Future<MessageId>> futures = new HashSet<>();
        for (int i = 0; i < numberOfMessages; i++) {
            String message = messagePredicate + i;
            futures.add(producer.sendAsync(message.getBytes()));
        }

        futures.forEach(f -> {
            try {
                messageIds.add(f.get());
            } catch (Exception e) {
                Assert.fail("Failed to publish message, Exception: " + e.getMessage());
            }
        });

        // 4. Check if message Ids are correct
        log.info("Message IDs = " + messageIds);
        Assert.assertEquals(messageIds.size(), numberOfMessages, "Not all messages published successfully");

        for (int i = 0; i < numberOfMessages; i++) {
            MessageId topicMessageId = consumer.receive().getMessageId();
            MessageId messageId = ((TopicMessageIdImpl)topicMessageId).getInnerMessageId();
            log.info("Message ID Received = " + messageId);
            Assert.assertEquals(topicMessageId.toString(), messageId.toString());
            Assert.assertTrue(messageIds.remove(messageId), "Failed to receive Message");
        }
        log.info("Message IDs = " + messageIds);
        Assert.assertEquals(messageIds.size(), 0, "Not all messages received successfully");
        consumer.unsubscribe();
    }

    @Test(timeOut = 10000)
    public void partitionedProducerSend() throws PulsarClientException, PulsarAdminException {
        // 1. Basic Config
        String key = "partitionedProducerSend";
        final String topicName = "persistent://prop/cluster/namespace/topic-" + key;
        final String subscriptionName = "my-subscription-" + key;
        final String messagePredicate = "my-message-" + key + "-";
        final int numberOfMessages = 30;
        int numberOfPartitions = 7;
        admin.topics().createPartitionedTopic(topicName, numberOfPartitions);

        // 2. Create Producer
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        // 3. Create Consumer
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subscriptionName)
                .subscribe();

        // 4. Publish message and get message id
        Set<MessageId> messageIds = new HashSet<>();
        for (int i = 0; i < numberOfMessages; i++) {
            String message = messagePredicate + i;
            messageIds.add(producer.send(message.getBytes()));
        }

        // 4. Check if message Ids are correct
        log.info("Message IDs = " + messageIds);
        Assert.assertEquals(messageIds.size(), numberOfMessages, "Not all messages published successfully");

        for (int i = 0; i < numberOfMessages; i++) {
            MessageId topicMessageId = consumer.receive().getMessageId();
            MessageId messageId = ((TopicMessageIdImpl)topicMessageId).getInnerMessageId();
            Assert.assertEquals(topicMessageId.toString(), messageId.toString());
            Assert.assertTrue(messageIds.remove(messageId), "Failed to receive Message");
        }
        log.info("Message IDs = " + messageIds);
        Assert.assertEquals(messageIds.size(), 0, "Not all messages received successfully");
        // TODO - this statement causes the broker to hang - need to look into
        // it
        // consumer.unsubscribe();
    }

    /**
     * Verifies: different versions of broker-deployment (one broker understands Checksum and other doesn't in that case
     * remove checksum before sending to broker-2)
     *
     * client first produce message with checksum and then retries to send message due to connection unavailable. But
     * this time, if broker doesn't understand checksum: then client should remove checksum from the message before
     * sending to broker.
     *
     * 1. stop broker 2. client compute checksum and add into message 3. produce 2 messages and corrupt 1 message 4.
     * start broker with lower version (which doesn't support checksum) 5. client reconnects to broker and due to
     * incompatibility of version: removes checksum from message 6. broker doesn't do checksum validation and persist
     * message 7. client receives ack
     *
     * @throws Exception
     */
    @Test
    public void testChecksumVersionComptability() throws Exception {
        final String topicName = "persistent://prop/use/ns-abc/topic1";

        // 1. producer connect
        ProducerImpl<byte[]> prod = (ProducerImpl<byte[]>) pulsarClient.newProducer().topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();
        ProducerImpl<byte[]> producer = spy(prod);
        // return higher version compare to broker : so, it forces client-producer to remove checksum from payload
        doReturn(producer.brokerChecksumSupportedVersion() + 1).when(producer).brokerChecksumSupportedVersion();
        doAnswer(invocationOnMock -> prod.getState()).when(producer).getState();
        doAnswer(invocationOnMock -> prod.getClientCnx()).when(producer).getClientCnx();
        doAnswer(invocationOnMock -> prod.cnx()).when(producer).cnx();

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName("my-sub").subscribe();

        // Stop the broker, and publishes messages. Messages are accumulated in the producer queue and they're checksums
        // would have already been computed. If we change the message content at that point, it should result in a
        // checksum validation error
        stopBroker();

        // stop timer to auto-reconnect as let spy-Producer connect to broker manually so, spy-producer object can get
        // mock-value from brokerChecksumSupportedVersion
        ((PulsarClientImpl) pulsarClient).timer().stop();

        ClientCnx mockClientCnx = spy(
                new ClientCnx(new ClientConfigurationData(), ((PulsarClientImpl) pulsarClient).eventLoopGroup()));
        doReturn(producer.brokerChecksumSupportedVersion() - 1).when(mockClientCnx).getRemoteEndpointProtocolVersion();
        prod.setClientCnx(mockClientCnx);

        CompletableFuture<MessageId> future1 = producer.sendAsync("message-1".getBytes());

        byte[] a2 = "message-2".getBytes();
        TypedMessageBuilder<byte[]> msg2 = producer.newMessage().value(a2);

        CompletableFuture<MessageId> future2 = msg2.sendAsync();

        // corrupt the message, new content would be 'message-3'
        ((TypedMessageBuilderImpl<byte[]>) msg2).getContent().put(a2.length - 1, (byte) '3');

        prod.setClientCnx(null);

        // Restart the broker to have the messages published
        startBroker();

        // grab broker connection with mocked producer which has higher version compare to broker
        prod.grabCnx();

        try {
            // it should not fail: as due to unsupported version of broker: client removes checksum and broker should
            // ignore the checksum validation
            future1.get();
            future2.get();
        } catch (Exception e) {
            e.printStackTrace();
            fail("Broker shouldn't verify checksum for corrupted message and it shouldn't fail");
        }

        ((ConsumerImpl<byte[]>) consumer).grabCnx();
        // We should only receive msg1
        Message<byte[]> msg = consumer.receive(1, TimeUnit.SECONDS);
        assertEquals(new String(msg.getData()), "message-1");
        msg = consumer.receive(1, TimeUnit.SECONDS);
        assertEquals(new String(msg.getData()), "message-3");

    }

    @Test
    public void testChecksumReconnection() throws Exception {
        final String topicName = "persistent://prop/use/ns-abc/topic1";

        // 1. producer connect
        ProducerImpl<byte[]> prod = (ProducerImpl<byte[]>) pulsarClient.newProducer().topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();
        ProducerImpl<byte[]> producer = spy(prod);
        // mock: broker-doesn't support checksum (remote_version < brokerChecksumSupportedVersion) so, it forces
        // client-producer to perform checksum-strip from msg at reconnection
        doReturn(producer.brokerChecksumSupportedVersion() + 1).when(producer).brokerChecksumSupportedVersion();
        doAnswer(invocationOnMock -> prod.getState()).when(producer).getState();
        doAnswer(invocationOnMock -> prod.getClientCnx()).when(producer).getClientCnx();
        doAnswer(invocationOnMock -> prod.cnx()).when(producer).cnx();

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName("my-sub").subscribe();

        stopBroker();

        // stop timer to auto-reconnect as let spy-Producer connect to broker
        // manually so, spy-producer object can get
        // mock-value from brokerChecksumSupportedVersion
        ((PulsarClientImpl) pulsarClient).timer().stop();

        // set clientCnx mock to get non-checksum supported version
        ClientCnx mockClientCnx = spy(
                new ClientCnx(new ClientConfigurationData(), ((PulsarClientImpl) pulsarClient).eventLoopGroup()));
        doReturn(producer.brokerChecksumSupportedVersion() - 1).when(mockClientCnx).getRemoteEndpointProtocolVersion();
        prod.setClientCnx(mockClientCnx);

        CompletableFuture<MessageId> future1 = producer.sendAsync("message-1".getBytes());

        byte[] a2 = "message-2".getBytes();
        TypedMessageBuilder<byte[]> msg2 = producer.newMessage().value(a2);

        CompletableFuture<MessageId> future2 = msg2.sendAsync();

        // corrupt the message, new content would be 'message-3'
        ((TypedMessageBuilderImpl<byte[]>) msg2).getContent().put(a2.length - 1, (byte) '3');

        // unset mock
        prod.setClientCnx(null);

        // Restart the broker to have the messages published
        startBroker();

        // grab broker connection with mocked producer which has higher version
        // compare to broker
        prod.grabCnx();

        try {
            // it should not fail: as due to unsupported version of broker:
            // client removes checksum and broker should
            // ignore the checksum validation
            future1.get(10, TimeUnit.SECONDS);
            future2.get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Broker shouldn't verify checksum for corrupted message and it shouldn't fail");
        }

        ((ConsumerImpl<byte[]>) consumer).grabCnx();
        // We should only receive msg1
        Message<byte[]> msg = consumer.receive(1, TimeUnit.SECONDS);
        assertEquals(new String(msg.getData()), "message-1");
        msg = consumer.receive(1, TimeUnit.SECONDS);
        assertEquals(new String(msg.getData()), "message-3");

    }

    /**
     * Verifies: if message is corrupted before sending to broker and if broker gives checksum error: then 1.
     * Client-Producer recomputes checksum with modified data 2. Retry message-send again 3. Broker verifies checksum 4.
     * client receives send-ack success
     *
     * @throws Exception
     */
    @Test
    public void testCorruptMessageRemove() throws Exception {

        final String topicName = "persistent://prop/use/ns-abc/retry-topic";

        // 1. producer connect
        ProducerImpl<byte[]> prod = (ProducerImpl<byte[]>) pulsarClient.newProducer()
            .topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .sendTimeout(10, TimeUnit.MINUTES)
            .create();
        ProducerImpl<byte[]> producer = spy(prod);
        Field producerIdField = ProducerImpl.class.getDeclaredField("producerId");
        producerIdField.setAccessible(true);
        long producerId = (long) producerIdField.get(producer);
        producer.cnx().registerProducer(producerId, producer); // registered spy ProducerImpl
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName("my-sub").subscribe();

        // 2. Stop the broker, and publishes messages. Messages are accumulated in the producer queue and they're
        // checksums
        // would have already been computed. If we change the message content at that point, it should result in a
        // checksum validation error
        // enable checksum at producer
        stopBroker();

        byte[] a = "message-1".getBytes();
        TypedMessageBuilder<byte[]> msg = producer.newMessage().value(a);

        CompletableFuture<MessageId> future = msg.sendAsync();

        // corrupt the message, new content would be 'message-2'
        ((TypedMessageBuilderImpl<byte[]>) msg).getContent().put(a.length - 1, (byte) '2');

        // 4. Restart the broker to have the messages published
        startBroker();

        try {
            future.get();
            fail("send message should have failed with checksum excetion");
        } catch (Exception e) {
            if (e.getCause() instanceof PulsarClientException.ChecksumException) {
                // ok (callback should get checksum exception as message was modified and corrupt)
            } else {
                fail("Callback should have only failed with ChecksumException", e);
            }
        }

        // 5. Verify
        /**
         * verify: ProducerImpl.verifyLocalBufferIsNotCorrupted() => validates if message is corrupt
         */
        byte[] a2 = "message-2".getBytes();

        TypedMessageBuilderImpl<byte[]> msg2 = (TypedMessageBuilderImpl<byte[]>) producer.newMessage().value("message-1".getBytes());
        ByteBuf payload = Unpooled.wrappedBuffer(msg2.getContent());
        Builder metadataBuilder = ((TypedMessageBuilderImpl<byte[]>) msg).getMetadataBuilder();
        MessageMetadata msgMetadata = metadataBuilder.setProducerName("test").setSequenceId(1).setPublishTime(10L)
                .build();
        ByteBufPair cmd = Commands.newSend(producerId, 1, 1, ChecksumType.Crc32c, msgMetadata, payload);
        // (a) create OpSendMsg with message-data : "message-1"
        OpSendMsg op = OpSendMsg.create(((MessageImpl<byte[]>) msg2.getMessage()), cmd, 1, null);
        // a.verify: as message is not corrupt: no need to update checksum
        assertTrue(producer.verifyLocalBufferIsNotCorrupted(op));

        // (b) corrupt message
        msg2.getContent().put(a2.length - 1, (byte) '2'); // new content would be 'message-2'
        // b. verify: as message is corrupt: update checksum
        assertFalse(producer.verifyLocalBufferIsNotCorrupted(op));

        assertEquals(producer.getPendingQueueSize(), 0);

        // [2] test-recoverChecksumError functionality
        stopBroker();

        TypedMessageBuilderImpl<byte[]> msg1 = (TypedMessageBuilderImpl<byte[]>) producer.newMessage().value("message-1".getBytes());
        future = msg1.sendAsync();
        ClientCnx cnx = spy(
                new ClientCnx(new ClientConfigurationData(), ((PulsarClientImpl) pulsarClient).eventLoopGroup()));
        String exc = "broker is already stopped";
        // when client-try to recover checksum by resending to broker: throw exception as broker is stopped
        doThrow(new IllegalStateException(exc)).when(cnx).ctx();
        try {
            producer.recoverChecksumError(cnx, 1);
            fail("it should call : resendMessages() => which should throw above mocked exception");
        } catch (IllegalStateException e) {
            assertEquals(exc, e.getMessage());
        }

        producer.close();
        consumer.close();
        producer = null; // clean reference of mocked producer
    }

}
