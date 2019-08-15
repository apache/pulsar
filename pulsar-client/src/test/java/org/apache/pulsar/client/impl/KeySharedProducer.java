package org.apache.pulsar.client.impl;

import org.apache.pulsar.client.api.BatcherBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class KeySharedProducer {

    public static void main(String[] args) throws PulsarClientException, InterruptedException {
        PulsarClient client = PulsarClient.builder().serviceUrl("pulsar://127.0.0.1:6650").statsInterval(5, TimeUnit.SECONDS).build();
        Producer<String> producer = client.newProducer(Schema.STRING)
                .topic("key_shared_latency-1")
                .enableBatching(false)
                .batcherBuilder(BatcherBuilder.KEY_BASED)
                .maxPendingMessages(5000)
                .create();

        int i = 0;
        while (true) {
            producer.newMessage().key(UUID.randomUUID().toString()).value("test").sendAsync();
            if (++i % 20 == 0) {
//                Thread.sleep(1);
            }
        }
    }
}
