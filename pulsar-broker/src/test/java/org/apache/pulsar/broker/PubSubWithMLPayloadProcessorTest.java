package org.apache.pulsar.broker;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Slf4j
public class PubSubWithMLPayloadProcessorTest extends ProducerConsumerBase {

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        conf.setBrokerEntryPayloadProcessors(
                Collections.singleton("org.apache.pulsar.broker.ManagedLedgerPayloadProcessor0"));
        conf.setBrokerDeduplicationEnabled(false);
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }


    @Test
    public void testPublish() throws Exception {
        String topic = "persistent://public/default/PubSubWithMLPayloadProcessorTest";

        admin.topics().createNonPartitionedTopic(topic);

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topic).enableBatching(false).create();

        CountDownLatch latch = new CountDownLatch(10);
        for (int i = 0; i < 10; i++) {
            producer.sendAsync("message-" + i).whenComplete((ignored, e) -> {
                if (e != null) {
                    log.error("Failed to publish message", e);
                }
                latch.countDown();
            });
        }

        latch.await();

        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic).subscriptionName("my-sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe();

        for (;;) {
            var msg = consumer.receive(3, TimeUnit.SECONDS);
            if (msg == null) {
                break;
            }
            String content = msg.getValue();
            System.out.println("Received: " + content);
            consumer.acknowledge(msg);
        }
    }
}
