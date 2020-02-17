package org.apache.pulsar.client.impl;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

public class MQTTTests {

    public static void main(String[] args) throws PulsarClientException, InterruptedException {
        PulsarClient client = PulsarClient.builder().serviceUrl("pulsar://127.0.0.1:15002").build();

        Producer<String> producer = client.newProducer(Schema.STRING).topic("persistent://public/default/mop").enableBatching(false).create();
        for (int i = 0 ; i < 100000; i++) {
            producer.send("Mop -> " + i);
        }
    }
}
