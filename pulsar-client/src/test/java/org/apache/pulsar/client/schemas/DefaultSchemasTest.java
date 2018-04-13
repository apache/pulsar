package org.apache.pulsar.client.schemas;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.schemas.BooleanSchema;
import org.apache.pulsar.client.api.schemas.StringSchema;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;

public class DefaultSchemasTest {
    private static final String TOPIC = "test-topic";
    private static final String SUBSCRIPTION = "sub-1";

    private PulsarClient client;

    @BeforeClass
    public void setup() throws PulsarClientException {
        client = PulsarClient.builder().build();
    }

    @Test
    public void testConsumerInstantiation() throws PulsarClientException {
        ConsumerBuilder<String> stringConsumerBuilder = client.newConsumer(new StringSchema());
        ConsumerBuilder<Boolean> booleanConsumerBuilder = client.newConsumer(new BooleanSchema());
        Arrays.asList(stringConsumerBuilder, booleanConsumerBuilder).forEach(Assert::assertNotNull);
    }

    @Test
    public void testProducerInstantiation() throws PulsarClientException {
        ProducerBuilder<String> stringProducerBuilder = client.newProducer(new StringSchema());
        ProducerBuilder<Boolean> booleanProducerBuilder = client.newProducer(new BooleanSchema());
        Arrays.asList(stringProducerBuilder, booleanProducerBuilder).forEach(Assert::assertNotNull);
    }
}
