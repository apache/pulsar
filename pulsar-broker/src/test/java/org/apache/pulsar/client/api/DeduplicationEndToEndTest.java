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

import io.netty.util.TimerTask;
import org.apache.pulsar.broker.service.PulsarCommandSender;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.ConnectionHandler;
import org.apache.pulsar.client.impl.MultiTopicsConsumerImpl;
import org.apache.pulsar.client.impl.PartitionedProducerImpl;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.awaitility.Awaitility;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

/**
 * This test class is used to point out cases where message duplication can occur,
 * producer idempotency features can be used to solve which cases and can't solve which cases.
 * There are mainly two kinds of message duplication:
 * 1. Producer sends the message but doesn't receive the ack due to network issue, broker issue, etc.
 * User receives the exception and sends the same message again.
 * This resend operation is executed by the user, so the user can control the sequence id of the message or not.
 * Without message deduplication, the message is duplicated definitely.
 * With message deduplication, the cases vary:
 * - If user don't control the sequence id, the message is duplicated, as the message is resent with a different sequence id.
 * - If user control the sequence id, there are chances that the message is duplicate or not, depending on whether
 *    the topic is single partitioned or multi partitioned, whether the sequence id is used as the key of the message,
 *    whether the partition number is updated between the two messages.
 *
 * 2. The connection between the producer and the broker is broken after the producer sends the message,
 * but before the producer receives the ack. The producer reconnects to the broker and sends the same message again internally.
 * In this case, the producer can't control the sequence id of the message. The resent message remains the same as the original message.
 * - Without message deduplication, the message is duplicated.
 * - With message deduplication, the message is not duplicated.
 */
@Test(groups = "broker-api")
public class DeduplicationEndToEndTest extends ProducerConsumerBase {

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

    private List<Message<byte[]>> assertDuplicate(Consumer<byte[]> consumer, byte[] data) throws PulsarClientException {
        // consume the message, there are at least two messages in the topic
        List<Message<byte[]>> messages = new ArrayList<>(2);
        Message<byte[]> message = consumer.receive(1, TimeUnit.SECONDS);
        assertNotNull(message);
        assertEquals(message.getData(), data);
        messages.add(message);
        message = consumer.receive(1, TimeUnit.SECONDS);
        assertNotNull(message);
        assertEquals(message.getData(), data);
        messages.add(message);
        return messages;
    }

    private Message<byte[]> assertNotDuplicate(Consumer<byte[]> consumer, byte[] data) throws PulsarClientException {
        // consume the message, there are only one messages in the topic
        Message<byte[]> message = consumer.receive(1, TimeUnit.SECONDS);
        assertNotNull(message);
        assertEquals(message.getData(), data);
        assertNull(consumer.receive(1, TimeUnit.SECONDS));
        return message;
    }

    /**
     * Disable the send receipt to simulate the case where the producer sends the message but doesn't receive the ack
     * due to network issue, broker issue, etc.
     * Multiple partitions use the same ServerCnx, so we need to disable the send receipt for one partition only.
     * @param topic
     * @param producerName
     * @return the original commandSender
     * @throws Exception
     */
    private PulsarCommandSender disableSendReceipt(String topic, String producerName) throws Exception {
        PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService()
                .getTopic(topic + "-partition-" + 0, false).get().get();
        assertNotNull(persistentTopic);
        ServerCnx serverCnx = (ServerCnx) persistentTopic.getProducers().get(producerName).getCnx();

        PulsarCommandSender commandSender = serverCnx.getCommandSender();
        PulsarCommandSender spyCommandSender = Mockito.spy(commandSender);
        serverCnx.setCommandSender(spyCommandSender);

        // disable the send receipt
        Mockito.doNothing().when(spyCommandSender).sendSendReceiptResponse(Mockito.anyLong(), Mockito.anyLong(),
                Mockito.anyLong(), Mockito.anyLong(), Mockito.anyLong());
        return commandSender;
    }

    /**
     * Set the original commandSender back to the ServerCnx, so that the producer can receive the ack.
     * @param topic
     * @param producerName
     * @param sender the original commandSender
     * @throws Exception
     */
    private void enableSendReceipt(String topic, String producerName, PulsarCommandSender sender) throws Exception {
        PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService()
                .getTopic(topic + "-partition-" + 0, false).get().get();
        assertNotNull(persistentTopic);
        ServerCnx serverCnx = (ServerCnx) persistentTopic.getProducers().get(producerName).getCnx();
        // set original commandSender back
        serverCnx.setCommandSender(sender);
    }

    /**
     * simulate the case where the producer sends the message but doesn't receive the ack
     * due to network issue, broker issue, etc. User receives the exception and sends the same message again.
     * The message is duplicated in the topic.
     * @throws Exception
     */
    @Test
    public void testProducerDuplicationWithReceiptLost() throws Exception {
        final String topic = "persistent://my-property/my-ns/deduplication-test";
        int partitionCount = 1;
        admin.topics().createPartitionedTopic(topic, partitionCount);
        admin.namespaces().setDeduplicationStatus("my-property/my-ns", false);

        // Create producer with deduplication disabled
        String producerName = "my-producer-name";
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic)
                .producerName(producerName).sendTimeout(1, TimeUnit.SECONDS).create();
        assertEquals(producer.getLastSequenceId(), -1L);
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("my-sub").subscribe();

        // disable the send receipt to simulate the case where the producer sends the message but doesn't receive the ack
        PulsarCommandSender sender = disableSendReceipt(topic, producerName);

        // send a message
        byte[] data = "test".getBytes();
        Assert.assertThrows(PulsarClientException.TimeoutException.class, () -> producer.send(data));

        // set back the send receipt
        enableSendReceipt(topic, producerName, sender);

        // user receive the exception, send the same message again
        producer.send(data);

        // consume the message, there are two messages in the topic
        List<Message<byte[]>> messages = assertDuplicate(consumer, data);
        assertEquals(messages.get(0).getSequenceId(), 0);
        assertEquals(messages.get(1).getSequenceId(), 1);

        // clean up
        producer.close();
        consumer.close();
    }


    /**
     * simulate the case where the producer sends the message but doesn't receive the ack
     * due to network issue, broker issue, etc. User receives the exception and sends the same message again.
     * With deduplication enabled, the message is duplicated too! Because the message newly sent by calling
     * producer.sendAsync has a different sequence id.
     * @throws Exception
     */
    @Test
    public void testProducerDuplicationWithReceiptLostDedupEnabled() throws Exception {
        final String topic = "persistent://my-property/my-ns/deduplication-test-dedup-enabled";
        int partitionCount = 1;
        admin.topics().createPartitionedTopic(topic, partitionCount);
        admin.namespaces().setDeduplicationStatus("my-property/my-ns", true);

        // Create producer with deduplication enabled
        String producerName = "my-producer-name";
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic)
                .producerName(producerName).sendTimeout(1, TimeUnit.SECONDS).create();
        assertEquals(producer.getLastSequenceId(), -1L);
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("my-sub").subscribe();

        // disable the send receipt to simulate the case where the producer sends the message but doesn't receive the ack
        PulsarCommandSender sender = disableSendReceipt(topic, producerName);

        // send a message
        byte[] data = "test".getBytes();
        Assert.assertThrows(PulsarClientException.TimeoutException.class, () -> producer.send(data));

        // set back the send receipt
        enableSendReceipt(topic, producerName, sender);

        // user receive the exception, send the same message again
        // though the message content is the same, the sequence id is different, so the message is duplicated
        producer.send(data);

        // consume the message, there are two messages in the topic
        List<Message<byte[]>> messages = assertDuplicate(consumer, data);
        assertEquals(messages.get(0).getSequenceId(), 0);
        assertEquals(messages.get(1).getSequenceId(), 1);

        // clean up
        producer.close();
        consumer.close();
    }


    /**
     * simulate the case where the producer sends the message but doesn't receive the ack
     * due to network issue, broker issue, etc. User receives the exception and sends the same message again.
     * With deduplication enabled, the message is duplicated too! Because the message newly sent by calling
     * typeMessages.sendAsync() has a different sequence id, though we use the same TypedMessageBuilder.
     * @throws Exception
     */
    @Test
    public void testProducerDuplicationWithReceiptLostDedupEnabled2() throws Exception {
        final String topic = "persistent://my-property/my-ns/deduplication-test-dedup-enabled2";
        int partitionCount = 1;
        admin.topics().createPartitionedTopic(topic, partitionCount);
        admin.namespaces().setDeduplicationStatus("my-property/my-ns", true);

        // Create producer with deduplication enabled
        String producerName = "my-producer-name";
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic)
                .producerName(producerName).sendTimeout(1, TimeUnit.SECONDS).create();
        assertEquals(producer.getLastSequenceId(), -1L);
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("my-sub").subscribe();

        // disable the send receipt to simulate the case where the producer sends the message but doesn't receive the ack
        PulsarCommandSender sender = disableSendReceipt(topic, producerName);

        // send a message
        byte[] data = "test".getBytes();
        TypedMessageBuilder<byte[]> typeMessages = producer.newMessage().value(data);
        Assert.assertThrows(PulsarClientException.TimeoutException.class, () -> typeMessages.send());

        // set back the send receipt
        enableSendReceipt(topic, producerName, sender);

        // user receive the exception, send the same message again
        // though we use the same TypedMessageBuilder, the two messages are different!
        // because the sequence id is different, so the message is duplicated too.
        typeMessages.send();

        // consume the message, there are two messages in the topic
        List<Message<byte[]>> messages = assertDuplicate(consumer, data);
        assertEquals(messages.get(0).getSequenceId(), 0);
        assertEquals(messages.get(1).getSequenceId(), 1);

        // clean up
        producer.close();
        consumer.close();
    }


    /**
     * simulate the case where the producer sends the message but doesn't receive the ack
     * due to network issue, broker issue, etc. User receives the exception and sends the same message again.
     * With deduplication enabled and user control sequence id, the message is not duplicated.
     */
    @Test
    public void testProducerDuplicationWithReceiptLostUserControlSequenceId() throws Exception {
        final String topic = "persistent://my-property/my-ns/deduplication-test-user-control-sequence-id";
        int partitionCount = 1;
        admin.topics().createPartitionedTopic(topic, partitionCount);
        admin.namespaces().setDeduplicationStatus("my-property/my-ns", true);

        // Create producer with deduplication enabled
        String producerName = "my-producer-name";
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic)
                .producerName(producerName).sendTimeout(1, TimeUnit.SECONDS).create();
        assertEquals(producer.getLastSequenceId(), -1L);
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("my-sub").subscribe();

        // disable the send receipt to simulate the case where the producer sends the message but doesn't receive the ack
        PulsarCommandSender sender = disableSendReceipt(topic, producerName);

        // send a message
        long lastId = 0;
        byte[] data = "test".getBytes();
        Assert.assertThrows(PulsarClientException.TimeoutException.class,
                () -> producer.newMessage().value(data).sequenceId(lastId).send());

        // set back the send receipt
        enableSendReceipt(topic, producerName, sender);

        // user receive the exception, send the same message again with the same sequence id.
        producer.newMessage().value(data).sequenceId(lastId).send();

        // consume the message, there are only one messages in the topic
        Message<byte[]> message = assertNotDuplicate(consumer, data);
        assertEquals(message.getSequenceId(), lastId);

        // clean up
        producer.close();
        consumer.close();
    }

    /**
     * simulate the case where the producer sends the message but doesn't receive the ack
     * due to network issue, broker issue, etc. User receives the exception and sends the same message again.
     * With deduplication enabled and user control sequence id, but the topic is multi partitioned,
     * the message is duplicated as message deduplication can't work across partitions.
     */
    @Test
    public void testProducerDuplicationWithReceiptLostUserControlSequenceIdMultiPartitioned() throws Exception {
        final String topic = "persistent://my-property/my-ns/deduplication-test-user-control-sequence-id-multi-partitioned";
        int partitionCount = 2;
        admin.topics().createPartitionedTopic(topic, partitionCount);
        admin.namespaces().setDeduplicationStatus("my-property/my-ns", true);

        // Create producer with deduplication enabled
        String producerName = "my-producer-name";
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic)
                .producerName(producerName).sendTimeout(1, TimeUnit.SECONDS).enableBatching(false).create();
        assertEquals(producer.getLastSequenceId(), -1L);
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("my-sub").subscribe();

        // disable the send receipt to simulate the case where the producer sends the message but doesn't receive the ack
        PulsarCommandSender sender = disableSendReceipt(topic, producerName);

        // send a message
        long lastId = 0;
        byte[] data = "test".getBytes();
        Assert.assertThrows(PulsarClientException.TimeoutException.class,
                () -> producer.newMessage().value(data).sequenceId(lastId).send());

        // set back the send receipt
        enableSendReceipt(topic, producerName, sender);

        // user receive the exception, send the same message again with the same sequence id.
        // but this new message will be routed to another partition, so the message is duplicated.
        producer.newMessage().value(data).sequenceId(lastId).send();

        // consume the message, there are two messages in the topic
        List<Message<byte[]>> messages = assertDuplicate(consumer, data);
        assertEquals(messages.get(0).getSequenceId(), lastId);
        assertEquals(messages.get(1).getSequenceId(), lastId);

        // clean up
        producer.close();
        consumer.close();
    }


    /**
     * simulate the case where the producer sends the message but doesn't receive the ack
     * due to network issue, broker issue, etc. User receives the exception and sends the same message again.
     * With deduplication enabled and user control sequence id, although the topic is multi partitioned,
     * we use the key based routing to route the messages with the same key to the same partition.
     * The message is not duplicated.
     */
    @Test
    public void testProducerDuplicationWithReceiptLostUserControlSequenceIdKeyBasedRoute() throws Exception {
        final String topic = "persistent://my-property/my-ns/deduplication-test-user-control-sequence-id-key-based-route";
        int partitionCount = 2;
        admin.topics().createPartitionedTopic(topic, partitionCount);
        admin.namespaces().setDeduplicationStatus("my-property/my-ns", true);

        // Create producer with deduplication enabled
        String producerName = "my-producer-name";
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic)
                .producerName(producerName).sendTimeout(1, TimeUnit.SECONDS).enableBatching(false).create();
        assertEquals(producer.getLastSequenceId(), -1L);
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("my-sub").subscribe();

        // disable the send receipt to simulate the case where the producer sends the message but doesn't receive the ack
        PulsarCommandSender sender = disableSendReceipt(topic, producerName);

        // send a message, with sequence id as the key, so messages with the same key will be routed to the same partition
        long lastId = 0;
        byte[] data = "test".getBytes();
        Assert.assertThrows(PulsarClientException.TimeoutException.class,
                () -> producer.newMessage().value(data).sequenceId(lastId).key(String.valueOf(lastId)).send());

        // set back the send receipt
        enableSendReceipt(topic, producerName, sender);

        // user receive the exception, send the same message again with the same sequence id.
        // this new message will be routed to the same partition, so the message will not be duplicated.
        producer.newMessage().value(data).sequenceId(lastId).key(String.valueOf(lastId)).send();

        // consume the message, there are only one messages in the topic
        Message<byte[]> message = assertNotDuplicate(consumer, data);
        assertEquals(message.getSequenceId(), lastId);

        // clean up
        producer.close();
        consumer.close();
    }

    /**
     * trigger the partition number update for the producer
     * @param producer
     * @param expectedPartitionCount
     * @throws Exception
     */
    private void triggerPartitionUpdateForPartitionedProducer(PartitionedProducerImpl<byte[]> producer, int expectedPartitionCount) throws Exception {
        Field partitionsAutoUpdateTimerTask = PartitionedProducerImpl.class.getDeclaredField("partitionsAutoUpdateTimerTask");
        partitionsAutoUpdateTimerTask.setAccessible(true);
        TimerTask timerTask = (TimerTask) partitionsAutoUpdateTimerTask.get(producer);
        ((PulsarClientImpl) pulsarClient).getTimer().newTimeout(timerTask, 0, TimeUnit.MILLISECONDS);
        Awaitility.await().until(() -> {
            try {
                return producer.getNumOfPartitions() == expectedPartitionCount;
            } catch (Exception e) {
                return false;
            }
        });
    }

    private void triggerPartitionUpdateForPartitionedConsumer(MultiTopicsConsumerImpl<byte[]> consumer, int expectedPartitionCount) throws Exception {
        Field partitionsAutoUpdateTimerTask = MultiTopicsConsumerImpl.class.getDeclaredField("partitionsAutoUpdateTimerTask");
        partitionsAutoUpdateTimerTask.setAccessible(true);
        TimerTask timerTask = (TimerTask) partitionsAutoUpdateTimerTask.get(consumer);
        ((PulsarClientImpl) pulsarClient).getTimer().newTimeout(timerTask, 0, TimeUnit.MILLISECONDS);
        Awaitility.await().until(() -> {
            try {
                return consumer.getPartitions().size() == expectedPartitionCount;
            } catch (Exception e) {
                return false;
            }
        });
    }

    /**
     * simulate the case where the producer sends the message but doesn't receive the ack
     * due to network issue, broker issue, etc. User receives the exception and sends the same message again.
     * With deduplication enabled and user control sequence id, the topic is multi partitioned,
     * though we use the key based routing to route the messages with the same key to the same partition,
     * If the partition number is updated between the two messages, the two messages will be routed to different partitions,
     * so the message is duplicated.
     */
    @Test
    public void testProducerDuplicationWithReceiptLostUserControlSequenceIdKeyBasedRouteWhileUpdatePartition() throws Exception {
        final String topic = "persistent://my-property/my-ns/deduplication-test-user-control-sequence-id-key-based-route-while-update-partition";
        int partitionCount = 2;
        admin.topics().createPartitionedTopic(topic, partitionCount);
        admin.namespaces().setDeduplicationStatus("my-property/my-ns", true);

        // Create producer with deduplication enabled
        String producerName = "my-producer-name";
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic)
                .producerName(producerName).sendTimeout(1, TimeUnit.SECONDS).enableBatching(false).create();
        assertEquals(producer.getLastSequenceId(), -1L);
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("my-sub").subscribe();

        // disable the send receipt to simulate the case where the producer sends the message but doesn't receive the ack
        PulsarCommandSender sender = disableSendReceipt(topic, producerName);

        // send a message, with sequence id as the key, so messages with the same key will be routed to the same partition
        long lastId = 0;
        byte[] data = "test".getBytes();
        Assert.assertThrows(PulsarClientException.TimeoutException.class,
                () -> producer.newMessage().value(data).sequenceId(lastId).key(String.valueOf(lastId)).send());

        // set back the send receipt
        enableSendReceipt(topic, producerName, sender);

        // update the partition number between the two messages
        admin.topics().updatePartitionedTopic(topic, 5);

        // trigger the partition number update for producer and consumer
        triggerPartitionUpdateForPartitionedProducer((PartitionedProducerImpl<byte[]>) producer, 5);
        triggerPartitionUpdateForPartitionedConsumer((MultiTopicsConsumerImpl<byte[]>) consumer, 5);

        // user receive the exception, send the same message again with the same sequence id.
        // though the key of two messages are the same, the message is routed to different partition due to
        // partition number update, so the message is duplicated.
        producer.newMessage().value(data).sequenceId(lastId).key(String.valueOf(lastId)).send();

        // consume the message, there are two messages in the topic
        List<Message<byte[]>> messages = assertDuplicate(consumer, data);
        assertEquals(messages.get(0).getSequenceId(), lastId);
        assertEquals(messages.get(1).getSequenceId(), lastId);

        // clean up
        producer.close();
        consumer.close();
    }

    /**
     * trigger the reconnection
     * @param producer
     * @throws Exception
     */
    private void triggerReconnection(PartitionedProducerImpl<byte[]> producer) throws Exception {
        for(ProducerImpl<byte[]> p : producer.getProducers()) {
            p.getClientCnx().ctx().channel().close();
            ClientCnx cnx = p.getClientCnx();
            Field connectionHandlerField = ProducerImpl.class.getDeclaredField("connectionHandler");
            connectionHandlerField.setAccessible(true);
            ConnectionHandler connectionHandler = (ConnectionHandler) connectionHandlerField.get(p);
            connectionHandler.connectionClosed(cnx);
            Awaitility.await().until(() -> {
                try {
                    return connectionHandler.cnx() != null;
                } catch (Exception e) {
                    return false;
                }
            });
        }
    }

    @DataProvider(name = "enableDedup")
    public static Object[][] topicVersions() {
        return new Object[][]{
                {false, 2},
                {true, 2},
                {false, 1},
                {true, 1},
        };
    }

    /**
     * simulate the case when the connection is lost, producer resend the message internally with the same sequence id
     * to the same partition.
     * If deduplication is not enabled, the message is duplicated.
     * If deduplication is enabled, the message is not duplicated.
     * @throws Exception
     */
    @Test(dataProvider = "enableDedup")
    public void testProducerDuplicationWhileReconnection(boolean enableDedup, int partitionCount) throws Exception {
        final String topic = "persistent://my-property/my-ns/deduplication-test-reconnection" + enableDedup + partitionCount;
        admin.topics().createPartitionedTopic(topic, partitionCount);
        admin.namespaces().setDeduplicationStatus("my-property/my-ns", enableDedup);

        // Create producer
        String producerName = "my-producer-name";
        PartitionedProducerImpl<byte[]> producer = (PartitionedProducerImpl<byte[]>) pulsarClient.newProducer().topic(topic)
                .producerName(producerName).sendTimeout(0, TimeUnit.SECONDS).enableBatching(false).create();
        assertEquals(producer.getLastSequenceId(), -1L);
        admin.topics().createSubscription(topic, "my-sub", MessageId.earliest);

        // disable the send receipt
        PulsarCommandSender sender = disableSendReceipt(topic, producerName);

        // send a message
        byte[] data = "test".getBytes();
        CompletableFuture sendFuture = producer.sendAsync(data);
        producer.flushAsync();

        // trigger reconnection before the producer receives the ack
        // resend the message internally with the same sequence id
        triggerReconnection(producer);

        // set back the send receipt
        enableSendReceipt(topic, producerName, sender);
        triggerReconnection(producer);

        sendFuture.get(10, TimeUnit.SECONDS);

        // create consumer after the reconnection, because reconnection will trigger the same message to be
        // delivered to the consumer again, which is a duplication problem in consumer side.
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("my-sub").subscribe();
        if (enableDedup) {
            // with deduplication enabled, the message is not duplicated
            assertNotDuplicate(consumer, data);
        } else {
            // with deduplication disabled, the message is duplicated
            assertDuplicate(consumer, data);
        }

        // clean up
        producer.close();
        consumer.close();
    }

}
