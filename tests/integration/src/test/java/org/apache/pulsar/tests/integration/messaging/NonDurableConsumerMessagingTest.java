package org.apache.pulsar.tests.integration.messaging;

import lombok.Cleanup;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.testng.annotations.Test;

import java.util.stream.IntStream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

public class NonDurableConsumerMessagingTest extends MessagingBase {

    @Test(dataProvider = "ServiceUrls")
    public void testNonDurableConsumer(String serviceUrls) throws Exception {
        final String topicName = getNonPartitionedTopic("test-non-durable-consumer", false);
        @Cleanup
        final PulsarClient client = PulsarClient.builder().serviceUrl(serviceUrls).build();

        int numMessages = 20;

        try (final Producer<byte[]> producer = client.newProducer()
            .topic(topicName)
            .create()) {

            IntStream.range(0, numMessages).forEach(i -> {
                String payload = "message-" + i;
                producer.sendAsync(payload.getBytes(UTF_8));
            });
            // flush the producer to make sure all messages are persisted
            producer.flush();

            try (final Consumer<byte[]> consumer = client.newConsumer()
                .topic(topicName)
                .subscriptionName("non-durable-consumer")
                .subscriptionMode(SubscriptionMode.NonDurable)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe()) {

                for (int i = 0; i < numMessages; i++) {
                    Message<byte[]> msg = consumer.receive();
                    assertEquals(
                        "message-" + i,
                        new String(msg.getValue(), UTF_8)
                    );
                }
            }
        }

    }
}
