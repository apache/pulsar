package org.apache.pulsar.broker.stats.sender;

import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PulsarMetricsSenderTest extends BrokerTestBase {

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testPerTopicStats() throws Exception {
        Producer<byte[]> p1 = pulsarClient.newProducer().topic("persistent://my-property/use/my-ns/my-topic1").create();
        Producer<byte[]> p2 = pulsarClient.newProducer().topic("persistent://my-property/use/my-ns/my-topic2").create();

        Consumer<byte[]> c1 = pulsarClient.newConsumer()
                .topic("persistent://my-property/use/my-ns/my-topic1")
                .subscriptionName("test")
                .subscribe();

        Consumer<byte[]> c2 = pulsarClient.newConsumer()
                .topic("persistent://my-property/use/my-ns/my-topic2")
                .subscriptionName("test")
                .subscribe();

        final int messages = 10;

        for (int i = 0; i < messages; i++) {
            String message = "my-message-" + i;
            p1.send(message.getBytes());
            p2.send(message.getBytes());
        }

        for (int i = 0; i < messages; i++) {
            c1.acknowledge(c1.receive());
            c2.acknowledge(c2.receive());
        }

        //Consumer<String> cMetrics = pulsarClient.newConsumer().topic(this.conf.)
    }

}
