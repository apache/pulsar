package org.apache.pulsar.client.api;

import org.apache.pulsar.broker.service.PulsarCommandSender;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
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
     * simulate the case where the producer sends the message but doesn't receive the ack
     * due to network issue, broker issue, etc. User receives the exception and sends the same message again.
     * The message is duplicated in the topic.
     * @throws Exception
     */
    @Test
    public void testProducerDuplicationWithReceiptLost() throws Exception {
        final String topic = "persistent://my-property/my-ns/deduplication-test";
        admin.namespaces().setDeduplicationStatus("my-property/my-ns", false);

        // Create producer with deduplication enabled
        String producerName = "my-producer-name";
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic)
                .producerName(producerName).sendTimeout(1, TimeUnit.SECONDS).create();
        assertEquals(producer.getLastSequenceId(), -1L);
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("my-sub").subscribe();

        // disable the send receipt to simulate the case where the producer sends the message but doesn't receive the ack
        PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService()
                .getTopic(topic, false).get().get();
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

        // send a message
        producer.sendAsync("test".getBytes()).thenRun(() -> {
            // should not enter here
            Assert.fail();
        }).exceptionally(e -> {
            // do not receive the ack, should enter here
            return null;
        }).get();


        // set back the send receipt
        commandSenderField.set(serverCnx, commandSender);

        // user receive the exception, send the same message again
        producer.sendAsync("test".getBytes()).exceptionally(e -> {
            // should not enter here
            Assert.fail();
            return null;
        }).get();

        // consume the message, there are two messages in the topic
        Message<byte[]> message = consumer.receive(1, TimeUnit.SECONDS);
        assertNotNull(message);
        assertEquals(new String(message.getData()), "test");
        message = consumer.receive(1, TimeUnit.SECONDS);
        assertNotNull(message);
        assertEquals(new String(message.getData()), "test");

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
        admin.namespaces().setDeduplicationStatus("my-property/my-ns", true);

        // Create producer with deduplication enabled
        String producerName = "my-producer-name";
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic)
                .producerName(producerName).sendTimeout(1, TimeUnit.SECONDS).create();
        assertEquals(producer.getLastSequenceId(), -1L);
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("my-sub").subscribe();

        // disable the send receipt to simulate the case where the producer sends the message but doesn't receive the ack
        PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService()
                .getTopic(topic, false).get().get();
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

        // send a message
        producer.sendAsync("test".getBytes()).thenRun(() -> {
            // should not enter here
            Assert.fail();
        }).exceptionally(e -> {
            // do not receive the ack, should enter here
            return null;
        }).get();


        // set back the send receipt
        commandSenderField.set(serverCnx, commandSender);

        // user receive the exception, send the same message again
        // though the message content is the same, the sequence id is different, so the message is duplicated
        producer.sendAsync("test".getBytes()).exceptionally(e -> {
            // should not enter here
            Assert.fail();
            return null;
        }).get();

        // consume the message, there are two messages in the topic
        Message<byte[]> message = consumer.receive(1, TimeUnit.SECONDS);
        assertNotNull(message);
        assertEquals(new String(message.getData()), "test");
        assertEquals(message.getSequenceId(), 0);
        message = consumer.receive(1, TimeUnit.SECONDS);
        assertNotNull(message);
        assertEquals(new String(message.getData()), "test");
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
        admin.namespaces().setDeduplicationStatus("my-property/my-ns", true);

        // Create producer with deduplication enabled
        String producerName = "my-producer-name";
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic)
                .producerName(producerName).sendTimeout(1, TimeUnit.SECONDS).create();
        assertEquals(producer.getLastSequenceId(), -1L);
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("my-sub").subscribe();

        // disable the send receipt to simulate the case where the producer sends the message but doesn't receive the ack
        PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService()
                .getTopic(topic, false).get().get();
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

        // send a message
        TypedMessageBuilder<byte[]> typeMessages = producer.newMessage().value("test".getBytes());
        typeMessages.sendAsync().thenRun(() -> {
            // should not enter here
            Assert.fail();
        }).exceptionally(e -> {
            // do not receive the ack, should enter here
            return null;
        }).get();


        // set back the send receipt
        commandSenderField.set(serverCnx, commandSender);

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
        assertEquals(new String(message.getData()), "test");
        assertEquals(message.getSequenceId(), 0);
        message = consumer.receive(1, TimeUnit.SECONDS);
        assertNotNull(message);
        assertEquals(new String(message.getData()), "test");
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
        admin.namespaces().setDeduplicationStatus("my-property/my-ns", true);

        // Create producer with deduplication enabled
        String producerName = "my-producer-name";
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic)
                .producerName(producerName).sendTimeout(1, TimeUnit.SECONDS).create();
        assertEquals(producer.getLastSequenceId(), -1L);
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("my-sub").subscribe();

        // disable the send receipt to simulate the case where the producer sends the message but doesn't receive the ack
        PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService()
                .getTopic(topic, false).get().get();
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

        // send a message
        long lastId = 0;
        producer.newMessage().value("test".getBytes()).sequenceId(lastId).sendAsync().thenRun(() -> {
            // should not enter here
            Assert.fail();
        }).exceptionally(e -> {
            // do not receive the ack, should enter here
            return null;
        }).get();


        // set back the send receipt
        commandSenderField.set(serverCnx, commandSender);

        // user receive the exception, send the same message again with the same sequence id.
        producer.newMessage().value("test".getBytes()).sequenceId(lastId).sendAsync().exceptionally(e -> {
            // should not enter here
            Assert.fail();
            return null;
        }).get();

        // consume the message, there are only one messages in the topic
        Message<byte[]> message = consumer.receive(1, TimeUnit.SECONDS);
        assertNotNull(message);
        assertEquals(new String(message.getData()), "test");
        assertEquals(message.getSequenceId(), lastId);
        message = consumer.receive(1, TimeUnit.SECONDS);
        assertNull(message);

        // clean up
        producer.close();
        consumer.close();
    }



}
