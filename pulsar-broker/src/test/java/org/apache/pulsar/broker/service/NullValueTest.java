package org.apache.pulsar.broker.service;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class NullValueTest extends BrokerTestBase {

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
    public void nullValueBytesSchemaTest() throws PulsarClientException {
        String topic = "persistent://prop/ns-abc/null-value-bytes-test";

        @Cleanup
        Producer producer = pulsarClient.newProducer()
                .topic(topic)
                .create();

        @Cleanup
        Consumer consumer = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName("test")
                .subscribe();

        int numMessage = 10;
        for (int i = 0; i < numMessage; i++) {
            producer.newMessage().value(null).send();
        }

        for (int i = 0; i < numMessage; i++) {
            Message message = consumer.receive();
            Assert.assertNull(message.getValue());
            Assert.assertNull(message.getData());
        }

    }

    @Test
    public void nullValueBooleanSchemaTest() throws PulsarClientException {
        String topic = "persistent://prop/ns-abc/null-value-bool-test";

        @Cleanup
        Producer<Boolean> producer = pulsarClient.newProducer(Schema.BOOL)
                .topic(topic)
                .create();

        @Cleanup
        Consumer<Boolean> consumer = pulsarClient.newConsumer(Schema.BOOL)
                .topic(topic)
                .subscriptionName("test")
                .subscribe();

        int numMessage = 10;
        for (int i = 0; i < numMessage; i++) {
            producer.newMessage().value(null).sendAsync();
        }

        for (int i = 0; i < numMessage; i++) {
            Message<Boolean> message = consumer.receive();
            Assert.assertNull(message.getValue());
            Assert.assertNull(message.getData());
        }

    }

}
