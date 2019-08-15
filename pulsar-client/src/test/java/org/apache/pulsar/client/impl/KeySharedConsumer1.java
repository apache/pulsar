package org.apache.pulsar.client.impl;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;

import java.util.concurrent.TimeUnit;

public class KeySharedConsumer1 {

    public static void main(String[] args) throws PulsarClientException {
        PulsarClient client = PulsarClient.builder().serviceUrl("pulsar://127.0.0.1:6650").statsInterval(5, TimeUnit.SECONDS).build();

        for (int i = 0; i < 2000; i++) {
            new Thread(() -> {
                try {
                    Consumer<String> consumer = client.newConsumer(Schema.STRING)
                            .topic("key_shared_latency-1")
                            .subscriptionType(SubscriptionType.Key_Shared)
                            .receiverQueueSize(1000)
                            .subscriptionName("test")
                            .subscribe();
                    while (true) {
                        consumer.acknowledge(consumer.receive());
                    }
                } catch (PulsarClientException e) {
                    e.printStackTrace();
                }
            }).start();
        }

    }
}
