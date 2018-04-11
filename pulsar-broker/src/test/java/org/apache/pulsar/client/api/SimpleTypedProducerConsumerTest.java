package org.apache.pulsar.client.api;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Sets;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SimpleTypedProducerConsumerTest extends ProducerConsumerBase {
    private static final Logger log = LoggerFactory.getLogger(SimpleTypedProducerConsumerTest.class);

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    public static class JsonEncodedPojo {
        private String message;

        public JsonEncodedPojo() {
        }

        public JsonEncodedPojo(String message) {
            this.message = message;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            JsonEncodedPojo that = (JsonEncodedPojo) o;
            return Objects.equals(message, that.message);
        }

        @Override
        public int hashCode() {
            return Objects.hash(message);
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                .add("message", message)
                .toString();
        }
    }

    @Test
    public void testJsonProducerAndConsumer() throws Exception {
        log.info("-- Starting {} test --", methodName);

        Consumer<JsonEncodedPojo> consumer = pulsarClient
            .newConsumer(JSONSchema.of(JsonEncodedPojo.class))
            .topic("persistent://my-property/use/my-ns/my-topic1")
            .subscriptionName("my-subscriber-name")
            .subscribe();

        Producer<JsonEncodedPojo> producer = pulsarClient
            .newProducer(JSONSchema.of(JsonEncodedPojo.class))
            .topic("persistent://my-property/use/my-ns/my-topic1")
            .create();

        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(new JsonEncodedPojo(message));
        }

        Message<JsonEncodedPojo> msg = null;
        Set<JsonEncodedPojo> messageSet = Sets.newHashSet();
        for (int i = 0; i < 10; i++) {
            msg = consumer.receive(5, TimeUnit.SECONDS);
            JsonEncodedPojo receivedMessage = msg.getValue();
            log.debug("Received message: [{}]", receivedMessage);
            JsonEncodedPojo expectedMessage = new JsonEncodedPojo("my-message-" + i);
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }
        // Acknowledge the consumption of all messages at once
        consumer.acknowledgeCumulative(msg);
        consumer.close();
        log.info("-- Exiting {} test --", methodName);
    }

}
