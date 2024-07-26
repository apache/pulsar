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
import java.lang.reflect.Method;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

/**
 * This test class is used to point out cases where message duplication can occur,
 * producer idempotency features can be used to solve which cases and can't solve which cases.
 */
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

    /**
     * Disable the send receipt to simulate the case where the producer sends the message but doesn't receive the ack
     * due to network issue, broker issue, etc.
     * Multiple partitions use the same ServerCnx, so we need to disable the send receipt for one partition only.
     * @param topic
     * @param producerName
     * @return
     * @throws Exception
     */
    private PulsarCommandSender disableSendReceipt(String topic, String producerName) throws Exception {
        PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService()
                .getTopic(topic + "-partition-" + 0, false).get().get();
        assertNotNull(persistentTopic);
        ServerCnx serverCnx = (ServerCnx) persistentTopic.getProducers().get(producerName).getCnx();

        // use reflection to spy the commandSender
        Field commandSenderField = ServerCnx.class.getDeclaredField("commandSender");
        commandSenderField.setAccessible(true);
        PulsarCommandSender commandSender = (PulsarCommandSender) commandSenderField.get(serverCnx);
        PulsarCommandSender spyCommandSender = Mockito.spy(commandSender);
        commandSenderField.set(serverCnx, spyCommandSender);

        // disable the send receipt
        Mockito.doNothing().when(spyCommandSender).sendSendReceiptResponse(Mockito.anyLong(), Mockito.anyLong(),
                Mockito.anyLong(), Mockito.anyLong(), Mockito.anyLong());
        return commandSender;
    }

    /**
     * Set the original commandSender back to the ServerCnx, so that the producer can receive the ack.
     * @param topic
     * @param producerName
     * @param sender
     * @throws Exception
     */
    private void enableSendReceipt(String topic, String producerName, PulsarCommandSender sender) throws Exception {
        PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService()
                .getTopic(topic + "-partition-" + 0, false).get().get();
        assertNotNull(persistentTopic);
        ServerCnx serverCnx = (ServerCnx) persistentTopic.getProducers().get(producerName).getCnx();

        // set original commandSender back
        Field commandSenderField = ServerCnx.class.getDeclaredField("commandSender");
        commandSenderField.setAccessible(true);
        commandSenderField.set(serverCnx, sender);
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
        producer.sendAsync(data).thenRun(() -> {
            // should not enter here
            Assert.fail();
        }).exceptionally(e -> {
            // do not receive the ack, should enter here
            return null;
        }).get();

        // set back the send receipt
        enableSendReceipt(topic, producerName, sender);

        // user receive the exception, send the same message again
        producer.sendAsync(data).exceptionally(e -> {
            // should not enter here
            Assert.fail();
            return null;
        }).get();

        // consume the message, there are two messages in the topic
        Message<byte[]> message = consumer.receive(1, TimeUnit.SECONDS);
        assertNotNull(message);
        assertEquals(message.getData(), data);
        message = consumer.receive(1, TimeUnit.SECONDS);
        assertNotNull(message);
        assertEquals(message.getData(), data);

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
        producer.sendAsync(data).thenRun(() -> {
            // should not enter here
            Assert.fail();
        }).exceptionally(e -> {
            // do not receive the ack, should enter here
            return null;
        }).get();

        // set back the send receipt
        enableSendReceipt(topic, producerName, sender);

        // user receive the exception, send the same message again
        // though the message content is the same, the sequence id is different, so the message is duplicated
        producer.sendAsync(data).exceptionally(e -> {
            // should not enter here
            Assert.fail();
            return null;
        }).get();

        // consume the message, there are two messages in the topic
        Message<byte[]> message = consumer.receive(1, TimeUnit.SECONDS);
        assertNotNull(message);
        assertEquals(message.getData(), data);
        assertEquals(message.getSequenceId(), 0);
        message = consumer.receive(1, TimeUnit.SECONDS);
        assertNotNull(message);
        assertEquals(message.getData(), data);
        assertEquals(message.getSequenceId(), 1);

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
        typeMessages.sendAsync().thenRun(() -> {
            // should not enter here
            Assert.fail();
        }).exceptionally(e -> {
            // do not receive the ack, should enter here
            return null;
        }).get();

        // set back the send receipt
        enableSendReceipt(topic, producerName, sender);

        // user receive the exception, send the same message again
        // though we use the same TypedMessageBuilder, the two messages are different!
        // because the sequence id is different, so the message is duplicated too.
        typeMessages.sendAsync().exceptionally(e -> {
            // should not enter here
            Assert.fail();
            return null;
        }).get();

        // consume the message, there are two messages in the topic
        Message<byte[]> message = consumer.receive(1, TimeUnit.SECONDS);
        assertNotNull(message);
        assertEquals(message.getData(), data);
        assertEquals(message.getSequenceId(), 0);
        message = consumer.receive(1, TimeUnit.SECONDS);
        assertNotNull(message);
        assertEquals(message.getData(), data);
        assertEquals(message.getSequenceId(), 1);

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
    public void testProducerDuplicationWithReceiptLostDedupEnabledAndUserControlSequenceId() throws Exception {
        final String topic = "persistent://my-property/my-ns/deduplication-test-dedup-enabled-user-control-sequence-id";
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
        producer.newMessage().value(data).sequenceId(lastId).sendAsync().thenRun(() -> {
            // should not enter here
            Assert.fail();
        }).exceptionally(e -> {
            // do not receive the ack, should enter here
            return null;
        }).get();

        // set back the send receipt
        enableSendReceipt(topic, producerName, sender);

        // user receive the exception, send the same message again with the same sequence id.
        producer.newMessage().value(data).sequenceId(lastId).sendAsync().exceptionally(e -> {
            // should not enter here
            Assert.fail();
            return null;
        }).get();

        // consume the message, there are only one messages in the topic
        Message<byte[]> message = consumer.receive(1, TimeUnit.SECONDS);
        assertNotNull(message);
        assertEquals(message.getData(), data);
        assertEquals(message.getSequenceId(), lastId);
        message = consumer.receive(1, TimeUnit.SECONDS);
        assertNull(message);

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
    public void testProducerDuplicationWithReceiptLostDedupEnabledAndUserControlSequenceIdMultiPartitioned() throws Exception {
        final String topic = "persistent://my-property/my-ns/deduplication-test-dedup-enabled-user-control-sequence-id-multi-partitioned";
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
        producer.newMessage().value(data).sequenceId(lastId).sendAsync().thenRun(() -> {
            // should not enter here
            Assert.fail();
        }).exceptionally(e -> {
            // do not receive the ack, should enter here
            return null;
        }).get();

        // set back the send receipt
        enableSendReceipt(topic, producerName, sender);

        // user receive the exception, send the same message again with the same sequence id.
        // but this new message will be routed to another partition, so the message is duplicated.
        producer.newMessage().value(data).sequenceId(lastId).sendAsync().exceptionally(e -> {
            // should not enter here
            Assert.fail();
            return null;
        }).get();

        // consume the message, there are two messages in the topic
        Message<byte[]> message = consumer.receive(1, TimeUnit.SECONDS);
        assertNotNull(message);
        assertEquals(message.getData(), data);
        assertEquals(message.getSequenceId(), lastId);
        message = consumer.receive(1, TimeUnit.SECONDS);
        assertNotNull(message);
        assertEquals(message.getData(), data);
        assertEquals(message.getSequenceId(), lastId);

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
    public void testProducerDuplicationWithReceiptLostDedupEnabledAndUserControlSequenceIdMultiPartitionedAndKeyBasedRouteProducer() throws Exception {
        final String topic = "persistent://my-property/my-ns/deduplication-test-dedup-enabled-user-control-sequence-id-multi-partitioned-key-based-route-producer";
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
        producer.newMessage().value(data).sequenceId(lastId).key(String.valueOf(lastId)).sendAsync().thenRun(() -> {
            // should not enter here
            Assert.fail();
        }).exceptionally(e -> {
            // do not receive the ack, should enter here
            return null;
        }).get();

        // set back the send receipt
        enableSendReceipt(topic, producerName, sender);

        // user receive the exception, send the same message again with the same sequence id.
        // this new message will be routed to the same partition, so the message will not be duplicated.
        producer.newMessage().value(data).sequenceId(lastId).key(String.valueOf(lastId)).sendAsync().exceptionally(e -> {
            // should not enter here
            Assert.fail();
            return null;
        }).get();

        // consume the message, there are two messages in the topic
        Message<byte[]> message = consumer.receive(1, TimeUnit.SECONDS);
        assertNotNull(message);
        assertEquals(message.getData(), data);
        assertEquals(message.getSequenceId(), lastId);
        message = consumer.receive(1, TimeUnit.SECONDS);
        assertNull(message);

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
     * With deduplication enabled and user control sequence id, although the topic is multi partitioned,
     * we use the key based routing to route the messages with the same key to the same partition.
     * The message is not duplicated.
     */
    @Test
    public void testProducerDuplicationWithReceiptLostDedupEnabledAndUserControlSequenceIdMultiPartitionedAndKeyBasedRouteProducerWhileUpdatePartition() throws Exception {
        final String topic = "persistent://my-property/my-ns/deduplication-test-dedup-enabled-user-control-sequence-id-multi-partitioned-key-based-route-producer-while-update-partition";
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
        producer.newMessage().value(data).sequenceId(lastId).key(String.valueOf(lastId)).sendAsync().thenRun(() -> {
            // should not enter here
            Assert.fail();
        }).exceptionally(e -> {
            // do not receive the ack, should enter here
            return null;
        }).get();

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
        producer.newMessage().value(data).sequenceId(lastId).key(String.valueOf(lastId)).sendAsync().exceptionally(e -> {
            // should not enter here
            Assert.fail();
            return null;
        }).get();

        // consume the message, there are two messages in the topic
        Message<byte[]> message = consumer.receive(1, TimeUnit.SECONDS);
        assertNotNull(message);
        assertEquals(message.getData(), data);
        assertEquals(message.getSequenceId(), lastId);
        message = consumer.receive(1, TimeUnit.SECONDS);
        assertNotNull(message);
        assertEquals(message.getData(), data);
        assertEquals(message.getSequenceId(), lastId);

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
        ProducerImpl<byte[]> producer1 = producer.getProducers().get(0);
        ClientCnx cnx = producer1.getClientCnx();
        Field connectionHandlerField = ProducerImpl.class.getDeclaredField("connectionHandler");
        connectionHandlerField.setAccessible(true);
        ConnectionHandler connectionHandler = (ConnectionHandler) connectionHandlerField.get(producer1);
        connectionHandler.connectionClosed(cnx);
        Awaitility.await().until(() -> {
            try {
                return connectionHandler.cnx() != null;
            } catch (Exception e) {
                return false;
            }
        });
    }

    private void assertDuplicate(Consumer<byte[]> consumer, byte[] data) throws PulsarClientException {
        // consume the message, there are at least two messages in the topic
        Message<byte[]> message = consumer.receive(1, TimeUnit.SECONDS);
        assertNotNull(message);
        assertEquals(message.getData(), data);
        message = consumer.receive(1, TimeUnit.SECONDS);
        assertNotNull(message);
        assertEquals(message.getData(), data);
    }

    private void assertNotDuplicate(Consumer<byte[]> consumer, byte[] data) throws PulsarClientException {
        // consume the message, there are only one messages in the topic
        Message<byte[]> message = consumer.receive(1, TimeUnit.SECONDS);
        assertNotNull(message);
        assertEquals(message.getData(), data);
        message = consumer.receive(1, TimeUnit.SECONDS);
        assertNull(message);
    }

    @DataProvider(name = "enableDedup")
    public static Object[][] topicVersions() {
        return new Object[][] {
                { true },
                { false }
        };
    }

    /**
     * simulate the case when the connection is lost, producer resend the message internally with the same sequence id.
     * If deduplication is not enabled, the message is duplicated.
     * If deduplication is enabled, the message is not duplicated.
     * @throws Exception
     */
    @Test(dataProvider = "enableDedup")
    public void testProducerDuplicationWhileReconnection(boolean enableDedup) throws Exception {
        final String topic = "persistent://my-property/my-ns/deduplication-test-reconnection" + enableDedup;
        int partitionCount = 1;
        admin.topics().createPartitionedTopic(topic, partitionCount);
        admin.namespaces().setDeduplicationStatus("my-property/my-ns", enableDedup);

        // Create producer with deduplication disabled
        String producerName = "my-producer-name";
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic)
                .producerName(producerName).sendTimeout(0, TimeUnit.SECONDS).enableBatching(false).create();
        assertEquals(producer.getLastSequenceId(), -1L);
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("my-sub").subscribe();

        // disable the send receipt
        PulsarCommandSender sender = disableSendReceipt(topic, producerName);

        // send a message
        byte[] data = "test".getBytes();
        CompletableFuture sendFuture = producer.sendAsync(data);
        producer.flushAsync();

        // trigger reconnection before the producer receives the ack
        // resend the message internally with the same sequence id
        triggerReconnection((PartitionedProducerImpl<byte[]>) producer);

        // set back the send receipt
        enableSendReceipt(topic, producerName, sender);
        triggerReconnection((PartitionedProducerImpl<byte[]>) producer);

        sendFuture.get(5, TimeUnit.SECONDS);

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
