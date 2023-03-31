package org.apache.pulsar.client.impl;

import lombok.Cleanup;
import org.apache.pulsar.client.api.MockBrokerService;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.testng.Assert.assertEquals;

public class ProducerEmptySchemaCacheTest {

    MockBrokerService mockBrokerService;

    @BeforeClass(alwaysRun = true)
    public void setup() {
        mockBrokerService = new MockBrokerService();
        mockBrokerService.start();
    }

    @AfterClass(alwaysRun = true)
    public void teardown() {
        if (mockBrokerService != null) {
            mockBrokerService.stop();
            mockBrokerService = null;
        }
    }

    @org.testng.annotations.Test
    public void testConsumerUnsubscribeReference() throws Exception {
        @Cleanup
        PulsarClientImpl client = (PulsarClientImpl) PulsarClient.builder()
                .serviceUrl(mockBrokerService.getBrokerAddress())
                .build();

        AtomicLong counter = new AtomicLong(0);

        mockBrokerService.setHandleGetOrCreateSchema((ctx, commandGetOrCreateSchema) -> {
            counter.incrementAndGet();
            ctx.writeAndFlush(
                    Commands.newGetOrCreateSchemaResponse(commandGetOrCreateSchema.getRequestId(),
                            SchemaVersion.Empty));
        });

        // this schema mode is used in consumer retry and dlq Producer
        // when the origin consumer has Schema.BYTES schema
        // and when retry message or dlq message is send
        // will use typed message builder set Schema.Bytes to send message.

        @Cleanup
        Producer<byte[]> producer = client.newProducer(Schema.AUTO_PRODUCE_BYTES(Schema.BYTES))
                .topic("testAutoProduceBytesSchemaShouldCache")
                .sendTimeout(5, TimeUnit.SECONDS)
                .maxPendingMessages(0)
                .enableBatching(false)
                .create();

        producer.newMessage(Schema.BYTES).value("hello".getBytes()).send();


        assertEquals(counter.get(), 1);
    }
}
