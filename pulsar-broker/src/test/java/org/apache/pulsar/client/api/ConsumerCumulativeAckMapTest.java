package org.apache.pulsar.client.api;

import lombok.Cleanup;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@Test(groups = "broker-api")
public class ConsumerCumulativeAckMapTest extends ProducerConsumerBase{

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

    @DataProvider(name = "ackReceiptEnabled")
    public Object[][] ackReceiptEnabled() {
        return new Object[][] { { true }, { false } };
    }

    @Test(timeOut = 30000, dataProvider = "ackReceiptEnabled")
    public void testCumulativeAckMessageForPartitions(boolean ackReceiptEnabled) throws Exception {
        cumulativeAckMessagesForPartitions(true,true, ackReceiptEnabled);
        cumulativeAckMessagesForPartitions(true,false, ackReceiptEnabled);
        cumulativeAckMessagesForPartitions(false,false, ackReceiptEnabled);
        cumulativeAckMessagesForPartitions(false,true, ackReceiptEnabled);
    }

    private void cumulativeAckMessagesForPartitions(boolean isBatch, boolean isPartitioned, boolean ackReceiptEnabled) throws Exception {
        final String topic = "persistent://my-property/my-ns/batch-ack-" + UUID.randomUUID();
        final String subName = "testBatchAck-sub" + UUID.randomUUID();
        final int messageNum = ThreadLocalRandom.current().nextInt(1, 50);
        if (isPartitioned) {
            admin.topics().createPartitionedTopic(topic, 3);
        }

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .enableBatching(isBatch)
                .batchingMaxPublishDelay(50, TimeUnit.MILLISECONDS)
                .topic(topic).create();

        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .subscriptionType(SubscriptionType.Exclusive)
                .topic(topic)
                .negativeAckRedeliveryDelay(1001, TimeUnit.MILLISECONDS)
                .subscriptionName(subName)
                .enableBatchIndexAcknowledgment(ackReceiptEnabled)
                .isAckReceiptEnabled(ackReceiptEnabled)
                .subscribe();

        sendMessagesAsyncAndWait(producer, messageNum);
        Map<String, MessageId> partitionLatestMessageMap = new HashMap<>();
        for (int i = 0; i < messageNum; i++) {
            Message message = consumer.receive();
            partitionLatestMessageMap.put(message.getTopicName(), message.getMessageId());
        }
        consumer.acknowledgeCumulative(partitionLatestMessageMap);
        //Wait ack send.
        Thread.sleep(1000);
        consumer.redeliverUnacknowledgedMessages();
        Message<String> msg = consumer.receive(2, TimeUnit.SECONDS);
        Assert.assertNull(msg);
    }

    @Test(timeOut = 30000)
    public void testInputNegativeCase() throws Exception {
        final String topic = "persistent://my-property/my-ns/batch-ack-" + UUID.randomUUID();
        final String subName = "testBatchAck-sub" + UUID.randomUUID();
        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .subscriptionType(SubscriptionType.Exclusive)
                .topic(topic)
                .negativeAckRedeliveryDelay(1001, TimeUnit.MILLISECONDS)
                .subscriptionName(subName)
                .subscribe();
        Map<String, MessageId> partitionLatestMessageMap = null;
        try {
            consumer.acknowledgeCumulative(partitionLatestMessageMap);
            Assert.fail("map cannot be null");
        } catch (PulsarClientException e) {
            // Expected
        }

        partitionLatestMessageMap = new HashMap<>();
        partitionLatestMessageMap.put(topic, null);
        try {
            consumer.acknowledgeCumulative(partitionLatestMessageMap);
            Assert.fail("messageId cannot be null");
        } catch (PulsarClientException e) {
            // Expected
        }
    }

    private void sendMessagesAsyncAndWait(Producer<String> producer, int messages) throws Exception {
        CountDownLatch latch = new CountDownLatch(messages);
        for (int i = 0; i < messages; i++) {
            String message = "my-message-" + i;
            producer.sendAsync(message).thenAccept(messageId -> {
                if (messageId != null) {
                    latch.countDown();
                }
            });
        }
        latch.await();
    }

}
