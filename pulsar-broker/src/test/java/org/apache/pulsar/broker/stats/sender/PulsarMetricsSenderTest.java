package org.apache.pulsar.broker.stats.sender;

import com.google.common.collect.Sets;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.stats.Metrics;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

public class PulsarMetricsSenderTest extends MockedPulsarServiceBaseTest {

    public PulsarMetricsSenderTest() {
        super();
    }

    @BeforeClass
    @Override
    public void setup() throws Exception {
        conf.setClusterName("c1");
        conf.setAuthorizationEnabled(false);
        conf.setAuthorizationAllowWildcardsMatching(true);
        conf.setSuperUserRoles(Sets.newHashSet("pulsar.super_user"));
        conf.setMetricsSenderEnabled(true);
        conf.setMetricsSenderDestinationTenant("metrics");
        conf.setMetricsSenderIntervalInSeconds(3);
        internalSetup();
    }

    @AfterClass
    @Override
    public void cleanup() throws Exception {
        internalCleanup();
    }

    @Test
    public void testPerTopicStats() throws Exception {
        Producer<byte[]> p1 = pulsarClient.newProducer().topic("persistent://my-property/use/my-ns/my-topic1").create();

        Consumer<byte[]> c1 = pulsarClient.newConsumer()
                .topic("persistent://my-property/use/my-ns/my-topic1")
                .subscriptionName("test")
                .subscribe();

        final int messages = 1;

        for (int i = 0; i < messages; i++) {
            String message = "my-message-" + i;
            p1.send(message.getBytes());
        }

        for (int i = 0; i < messages; i++) {
            c1.acknowledge(c1.receive());
        }

        TopicName metricsTopic = TopicName.get(
                "persistent",
                NamespaceName.get(conf.getMetricsSenderDestinationTenant(), "brokers"),
                this.pulsar.getAdvertisedAddress());

        Consumer<Metrics> cMetrics = pulsarClient.newConsumer(Schema.JSON(Metrics.class))
                .topic(metricsTopic.toString())
                .subscriptionName("consumer-test")
                .subscribe();

        Message<Metrics> s = cMetrics.receive(10, TimeUnit.SECONDS);


        System.out.println("\n\nCONSUMPTION\n\n" +
                "");
        System.out.println(s.getPublishTime());
        System.out.println(s.getEventTime());
        System.out.println(s.getValue());
        System.out.println(s.getValue().toString());
    }

}
