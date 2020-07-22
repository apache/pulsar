package org.apache.pulsar.client.api;

import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static org.testng.Assert.assertNull;

public class MessageFilteringTest extends ProducerConsumerBase {
    private static final Logger log = LoggerFactory.getLogger(MessageFilteringTest.class);

    private static class SampleAvroPojo {
        public String aString;
        public Double aDouble;

        public SampleAvroPojo(String string, double doubl) {
            aString = string;
            aDouble = doubl;
        }
    }

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @DataProvider
    public static Object[][] variationsForExpectedPos() {
        return new Object[][]{
                // batching / start-inclusive / num-of-messages
                {true, true, 10},
                {true, false, 10},
                {false, true, 10},
                {false, false, 10},

                {true, true, 100},
                {true, false, 100},
                {false, true, 100},
                {false, false, 100},
        };
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testBytesPrefixFilter() throws Exception {
        log.info("-- Starting {} test --", methodName);

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic("persistent://my-property/my-ns/my-topic1")
                .messageFilterPolicy(MessageFilterPolicy.bytesPrefixPolicy(new byte[]{'1'}))
                .subscriptionName("my-subscriber-name").subscribe();

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer()
                .topic("persistent://my-property/my-ns/my-topic1");

        producerBuilder.enableBatching(true);
        producerBuilder.batchingMaxPublishDelay(10, TimeUnit.MILLISECONDS);
        producerBuilder.batchingMaxMessages(5);

        Producer<byte[]> producer = producerBuilder.create();
        for (int i = 0; i < 30; i++) {
            String message = i + "-hello-world-" + i;
            producer.send(message.getBytes());
        }

        Set<String> messageSet = Sets.newHashSet();
        Message<byte[]> msg = consumer.receive(5, TimeUnit.SECONDS);
        String receivedMessage = new String(msg.getData());
        log.debug("Received message: [{}]", receivedMessage);
        String expectedMessage = "1-hello-world-1";
        testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        for (int i = 10; i < 20; i++) {
            msg = consumer.receive(5, TimeUnit.SECONDS);
            receivedMessage = new String(msg.getData());
            log.debug("Received message: [{}]", receivedMessage);
            expectedMessage = i + "-hello-world-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }
        // Acknowledge the consumption of all messages at once
        consumer.acknowledgeCumulative(msg);
        assertNull(consumer.receive(1, TimeUnit.SECONDS));
        consumer.close();
        log.info("-- Exiting {} test --", methodName);
    }

    @Test
    public void testRegexPrefixFilter() throws Exception {
        log.info("-- Starting {} test --", methodName);

        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic("persistent://my-property/my-ns/my-topic1")
                .messageFilterPolicy(MessageFilterPolicy.regexFilterPolicy(Pattern.compile("message-[1-5][0-9]")))
                .subscriptionName("my-subscriber-name").subscribe();

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer()
                .topic("persistent://my-property/my-ns/my-topic1");

        producerBuilder.enableBatching(true);
        producerBuilder.batchingMaxPublishDelay(10, TimeUnit.MILLISECONDS);
        producerBuilder.batchingMaxMessages(5);

        Producer<byte[]> producer = producerBuilder.create();
        for (int i = 0; i < 100; i++) {
            String message = "message-" + i;
            producer.send(message.getBytes());
        }

        Set<String> messageSet = Sets.newHashSet();
        Message<String> msg = null;
        for (int i = 10; i < 60; i++) {
            msg = consumer.receive(5, TimeUnit.SECONDS);
            String receivedMessage = new String(msg.getData());
            log.debug("Received message: [{}]", receivedMessage);
            String expectedMessage = "message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }
        // Acknowledge the consumption of all messages at once
        consumer.acknowledgeCumulative(msg);
        assertNull(consumer.receive(1, TimeUnit.SECONDS));
        consumer.close();
        log.info("-- Exiting {} test --", methodName);
    }
}
