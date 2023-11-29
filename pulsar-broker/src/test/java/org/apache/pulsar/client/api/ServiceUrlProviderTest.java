/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.client.api;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;

import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-api")
public class ServiceUrlProviderTest extends ProducerConsumerBase {

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();

    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testCreateClientWithServiceUrlProvider() throws Exception {

        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrlProvider(new TestServiceUrlProvider(pulsar.getBrokerServiceUrl()))
                .statsInterval(1, TimeUnit.SECONDS)
                .build();
        Assert.assertTrue(((PulsarClientImpl) client).getConfiguration().getServiceUrlProvider() instanceof TestServiceUrlProvider);
        Producer<String> producer = client.newProducer(Schema.STRING)
                .topic("persistent://my-property/my-ns/my-topic")
                .create();
        Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic("persistent://my-property/my-ns/my-topic")
                .subscriptionName("my-subscribe")
                .subscribe();
        for (int i = 0; i < 100; i++) {
            producer.send("Hello Pulsar[" + i + "]");
        }
        client.updateServiceUrl(pulsar.getBrokerServiceUrl());
        for (int i = 100; i < 200; i++) {
            producer.send("Hello Pulsar[" + i + "]");
        }
        int received = 0;
        do {
            Message<String> message = consumer.receive();
            System.out.println(message.getValue());
            received++;
        } while (received < 200);
        Assert.assertEquals(received, 200);
        producer.close();
        consumer.close();
    }

    @Test
    public void testCreateClientWithAutoChangedServiceUrlProvider() throws Exception {

        AutoChangedServiceUrlProvider serviceUrlProvider = new AutoChangedServiceUrlProvider(pulsar.getBrokerServiceUrl());

        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrlProvider(serviceUrlProvider)
                .statsInterval(1, TimeUnit.SECONDS)
                .build();
        Assert.assertTrue(((PulsarClientImpl) client).getConfiguration().getServiceUrlProvider() instanceof AutoChangedServiceUrlProvider);

        ProducerImpl<String> producer = (ProducerImpl<String>) client.newProducer(Schema.STRING)
                .topic("persistent://my-property/my-ns/my-topic")
                .create();
        ConsumerImpl<String> consumer = (ConsumerImpl<String>) client.newConsumer(Schema.STRING)
                .topic("persistent://my-property/my-ns/my-topic")
                .subscriptionName("my-subscribe")
                .subscribe();

        PulsarService pulsarService1 = pulsar;
        conf.setBrokerShutdownTimeoutMs(0L);
        conf.setBrokerServicePort(Optional.of(0));
        conf.setWebServicePort(Optional.of(0));
        restartBroker();
        PulsarService pulsarService2 = pulsar;

        log.info("Pulsar1 = {}, Pulsar2 = {}", pulsarService1.getBrokerServiceUrl(), pulsarService2.getBrokerServiceUrl());
        Assert.assertNotEquals(pulsarService1.getBrokerServiceUrl(), pulsarService2.getBrokerServiceUrl());

        log.info("Service url : producer = {}, consumer = {}",
            producer.getClient().getLookup().getServiceUrl(),
            consumer.getClient().getLookup().getServiceUrl());

        Assert.assertEquals(producer.getClient().getLookup().getServiceUrl(), pulsarService1.getBrokerServiceUrl());
        Assert.assertEquals(consumer.getClient().getLookup().getServiceUrl(), pulsarService1.getBrokerServiceUrl());

        log.info("Changing service url from {} to {}",
            pulsarService1.getBrokerServiceUrl(),
            pulsarService2.getBrokerServiceUrl());

        serviceUrlProvider.onServiceUrlChanged(pulsarService2.getBrokerServiceUrl());
        log.info("Service url changed : producer = {}, consumer = {}",
            producer.getClient().getLookup().getServiceUrl(),
            consumer.getClient().getLookup().getServiceUrl());
        Assert.assertEquals(producer.getClient().getLookup().getServiceUrl(), pulsarService2.getBrokerServiceUrl());
        Assert.assertEquals(consumer.getClient().getLookup().getServiceUrl(), pulsarService2.getBrokerServiceUrl());
        producer.close();
        consumer.close();
    }

    static class TestServiceUrlProvider implements ServiceUrlProvider {

        private PulsarClient pulsarClient;

        private final String serviceUrl;

        public TestServiceUrlProvider(String serviceUrl) {
            this.serviceUrl = serviceUrl;
        }

        @Override
        public String getServiceUrl() {
            return serviceUrl;
        }

        @Override
        public void initialize(PulsarClient client) {
            this.pulsarClient = client;
        }

        public PulsarClient getPulsarClient() {
            return pulsarClient;
        }
    }

    static class AutoChangedServiceUrlProvider extends TestServiceUrlProvider {

        public AutoChangedServiceUrlProvider(String serviceUrl) {
            super(serviceUrl);
        }

        public void onServiceUrlChanged(String newServiceUrl) throws PulsarClientException {
            this.getPulsarClient().updateServiceUrl(newServiceUrl);
        }
    }
}
