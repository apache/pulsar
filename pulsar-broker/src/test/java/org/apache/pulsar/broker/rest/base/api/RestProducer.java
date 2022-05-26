package org.apache.pulsar.broker.rest.base.api;


import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.websocket.data.ProducerMessages;

public interface RestProducer {

    void send(String topic, ProducerMessages producerMessages) throws PulsarAdminException;
}
