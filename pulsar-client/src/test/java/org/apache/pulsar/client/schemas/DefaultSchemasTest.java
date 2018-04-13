package org.apache.pulsar.client.schemas;

import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageBuilder;
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
    private PulsarClient client;

    @BeforeClass
    public void setup() throws PulsarClientException {
        client = PulsarClient.builder().build();
    }

    @Test
    public void testConsumerInstantiation() {
        ConsumerBuilder<String> stringConsumerBuilder = client.newConsumer(new StringSchema());
        ConsumerBuilder<Boolean> booleanConsumerBuilder = client.newConsumer(new BooleanSchema());
        Arrays.asList(stringConsumerBuilder, booleanConsumerBuilder).forEach(Assert::assertNotNull);
    }

    @Test
    public void testProducerInstantiation() {
        ProducerBuilder<String> stringProducerBuilder = client.newProducer(new StringSchema());
        ProducerBuilder<Boolean> booleanProducerBuilder = client.newProducer(new BooleanSchema());
        Arrays.asList(stringProducerBuilder, booleanProducerBuilder).forEach(Assert::assertNotNull);
    }

    @Test
    public void testStringSchema() {
        byte[] bytes = "hello world".getBytes();
        StringSchema stringSchema = new StringSchema();
        Assert.assertEquals(stringSchema.decode(bytes), "hello world");
        Assert.assertEquals(stringSchema.encode("hello world"), bytes);

        Message<String> msg1 = MessageBuilder.create(stringSchema)
                .setContent(bytes)
                .build();
        Assert.assertEquals(stringSchema.decode(msg1.getData()), "hello world");

        Message<String> msg2 = MessageBuilder.create(stringSchema)
                .setValue("hello world")
                .build();
        Assert.assertEquals(stringSchema.encode("hello world"), msg2.getData());
    }

    @Test
    public void testBooleanSchema() {
        BooleanSchema booleanSchema = new BooleanSchema();
        Assert.assertTrue(booleanSchema.decode(new byte[]{1}));
        Assert.assertFalse(booleanSchema.decode(new byte[]{0}));
        Assert.assertFalse(booleanSchema.decode(new byte[]{8}));
        Assert.assertFalse(booleanSchema.decode("some string".getBytes()));

        Assert.assertEquals(booleanSchema.encode(true), new byte[]{1});
        Assert.assertEquals(booleanSchema.encode(false), new byte[]{0});

        Message<Boolean> msg1 = MessageBuilder.create(booleanSchema)
                .setContent(new byte[]{1})
                .build();
        Assert.assertTrue(msg1.getValue());

        Message<Boolean> msg2 = MessageBuilder.create(booleanSchema)
                .setValue(false)
                .build();
        Assert.assertFalse(msg2.getValue());
    }
}
