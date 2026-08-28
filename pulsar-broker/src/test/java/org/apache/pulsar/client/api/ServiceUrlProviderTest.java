/*
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

import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.CustomLog;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.testng.Assert;
import org.testng.annotations.Test;

@CustomLog
@Test(groups = "broker-api")
public class ServiceUrlProviderTest extends SharedPulsarBaseTest {
    @SuppressWarnings("deprecation")

    @Test
    public void testCreateClientWithServiceUrlProvider() throws Exception {
        final String topic = newTopicName();

        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrlProvider(new TestServiceUrlProvider(getBrokerServiceUrl()))
                .statsInterval(1, TimeUnit.SECONDS)
                .build();
        Assert.assertTrue(((PulsarClientImpl) client).getConfiguration().getServiceUrlProvider()
                instanceof TestServiceUrlProvider);
        Producer<String> producer = client.newProducer(Schema.STRING)
                .topic(topic)
                .create();
        Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("my-subscribe")
                .subscribe();
        for (int i = 0; i < 100; i++) {
            producer.send("Hello Pulsar[" + i + "]");
        }
        client.updateServiceUrl(getBrokerServiceUrl());
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
    @SuppressWarnings("deprecation")

    @Test
    public void testCreateClientWithAutoChangedServiceUrlProvider() throws Exception {
        final String topic = newTopicName();

        AutoChangedServiceUrlProvider serviceUrlProvider =
                new AutoChangedServiceUrlProvider(getBrokerServiceUrl());

        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrlProvider(serviceUrlProvider)
                .statsInterval(1, TimeUnit.SECONDS)
                .build();
        Assert.assertTrue(((PulsarClientImpl) client).getConfiguration().getServiceUrlProvider()
                instanceof AutoChangedServiceUrlProvider);

        Producer<String> producer = client.newProducer(Schema.STRING)
                .topic(topic)
                .create();
        Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("my-subscribe")
                .subscribe();

        String originalUrl = getBrokerServiceUrl();
        Assert.assertEquals(((PulsarClientImpl) client).getLookup().getServiceUrl(), originalUrl);

        // Simulate URL change by updating to the same URL (no broker restart in shared mode)
        serviceUrlProvider.onServiceUrlChanged(originalUrl);
        Assert.assertEquals(((PulsarClientImpl) client).getLookup().getServiceUrl(), originalUrl);

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
