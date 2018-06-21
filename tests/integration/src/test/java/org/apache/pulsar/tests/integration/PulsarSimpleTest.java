package org.apache.pulsar.tests.integration;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class PulsarSimpleTest {

    private static final Logger LOG = LoggerFactory.getLogger(PulsarSimpleTest.class);

    private PulsarStandAloneContainer pulsarCluster;
    private PulsarClient client;
    private Producer producer;
    private Consumer consumer;

    @BeforeTest
    public void setup(){
        pulsarCluster = new PulsarStandAloneContainer(PulsarSimpleTest.class.getSimpleName());
        pulsarCluster.start();
    }

    @Test
    public void test() throws PulsarClientException {
        client = PulsarClient.builder().serviceUrl(pulsarCluster.getPulsarUrl()).build();
        consumer = client.newConsumer().topic("test").subscriptionName("subs").subscribe();
        producer = client.newProducer().topic("test").create();
        producer.send("Hello World".getBytes());
        Message message = consumer.receive();
        producer.close();
        consumer.close();
        client.close();
        Assert.assertEquals(new String(message.getData()), "Hello World");
    }

    @AfterTest
    public void cleanup(){
        pulsarCluster.stop();
    }

}
