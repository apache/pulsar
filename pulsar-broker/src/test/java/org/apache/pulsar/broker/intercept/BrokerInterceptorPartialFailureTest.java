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
package org.apache.pulsar.broker.intercept;

import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.testng.Assert.assertEquals;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Cleanup;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeGroups;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class BrokerInterceptorPartialFailureTest extends ProducerConsumerBase {

    private static final String CREATE_FAILURE = "create-failure";
    private static final String CLOSE_FAILURE = "close-failure";

    private CounterBrokerInterceptor counterBrokerInterceptor;

    @BeforeClass(alwaysRun = true)
    @Override
    public void setup() throws Exception {
        conf.setSystemTopicEnabled(false);
        conf.setTopicLevelPoliciesEnabled(false);
        enableBrokerInterceptor = true;
        super.internalSetup();
        super.producerBaseSetup();
    }

    @Override
    protected void customizeMainPulsarTestContextBuilder(PulsarTestContext.Builder pulsarTestContextBuilder) {
        NarClassLoader narClassLoader = org.mockito.Mockito.mock(NarClassLoader.class,
                delegatesTo(new URLClassLoader(new URL[0], getClass().getClassLoader())));
        counterBrokerInterceptor = new CounterBrokerInterceptor();
        Map<String, BrokerInterceptorWithClassLoader> interceptors = new HashMap<>();
        interceptors.put(
                CounterBrokerInterceptor.NAME,
                new BrokerInterceptorWithClassLoader(counterBrokerInterceptor, narClassLoader));
        interceptors.put(
                "selective-failure",
                new BrokerInterceptorWithClassLoader(new SelectiveFailingInterceptor(), narClassLoader));
        pulsarTestContextBuilder.brokerInterceptor(new BrokerInterceptors(interceptors));
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @BeforeGroups(CREATE_FAILURE)
    public void configureCreateFailure() {
        SelectiveFailingInterceptor.FAIL_CREATE.set(true);
        SelectiveFailingInterceptor.FAIL_CLOSE.set(false);
        counterBrokerInterceptor.reset();
    }

    @BeforeGroups(CLOSE_FAILURE)
    public void configureCloseFailure() {
        SelectiveFailingInterceptor.FAIL_CREATE.set(false);
        SelectiveFailingInterceptor.FAIL_CLOSE.set(true);
        counterBrokerInterceptor.reset();
    }

    @Test(groups = CREATE_FAILURE)
    public void testProducerClosedWhenPeerFailsOnCreate() throws PulsarClientException {
        String topic = "persistent://public/default/partial-failure-producer-" + UUID.randomUUID();
        @Cleanup
        Producer<Boolean> producer = pulsarClient.newProducer(Schema.BOOL).topic(topic).create();
        Awaitility.await().until(() -> counterBrokerInterceptor.getProducerCount() == 1);
        producer.close();
        Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                assertEquals(counterBrokerInterceptor.getProducerClosedInvocations().get(), 1));
    }

    @Test(groups = CREATE_FAILURE)
    public void testConsumerClosedWhenPeerFailsOnCreate() throws PulsarClientException {
        String topic = "persistent://public/default/partial-failure-" + UUID.randomUUID();
        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("sub")
                .subscribe();
        Awaitility.await().until(() -> counterBrokerInterceptor.getConsumerCount() == 1);
        consumer.close();
        Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                assertEquals(counterBrokerInterceptor.getConsumerClosedInvocations().get(), 1));
    }

    @Test(groups = CLOSE_FAILURE)
    public void testProducerClosedWhenPeerFailsOnClose() throws PulsarClientException {
        String topic = "persistent://public/default/partial-close-failure-producer-" + UUID.randomUUID();
        @Cleanup
        Producer<Boolean> producer = pulsarClient.newProducer(Schema.BOOL).topic(topic).create();
        Awaitility.await().until(() -> counterBrokerInterceptor.getProducerCount() == 1);
        producer.close();
        Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                assertEquals(counterBrokerInterceptor.getProducerClosedInvocations().get(), 1));
    }

    @Test(groups = CLOSE_FAILURE)
    public void testConsumerClosedWhenPeerFailsOnClose() throws PulsarClientException {
        String topic = "persistent://public/default/partial-close-failure-" + UUID.randomUUID();
        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("sub")
                .subscribe();
        Awaitility.await().until(() -> counterBrokerInterceptor.getConsumerCount() == 1);
        consumer.close();
        Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                assertEquals(counterBrokerInterceptor.getConsumerClosedInvocations().get(), 1));
    }

    /**
     * Second interceptor in the chain; behavior toggled per {@link BeforeGroups} for create vs close failure cases.
     */
    static final class SelectiveFailingInterceptor implements BrokerInterceptor {

        static final AtomicBoolean FAIL_CREATE = new AtomicBoolean(true);
        static final AtomicBoolean FAIL_CLOSE = new AtomicBoolean(false);
        static final java.util.concurrent.atomic.AtomicInteger PRODUCER_CLOSE_INVOCATIONS =
                new java.util.concurrent.atomic.AtomicInteger();
        static final java.util.concurrent.atomic.AtomicInteger CONSUMER_CLOSE_INVOCATIONS =
                new java.util.concurrent.atomic.AtomicInteger();

        static void resetCloseInvocations() {
            PRODUCER_CLOSE_INVOCATIONS.set(0);
            CONSUMER_CLOSE_INVOCATIONS.set(0);
        }

        @Override
        public void producerCreated(org.apache.pulsar.broker.service.ServerCnx cnx,
                                    org.apache.pulsar.broker.service.Producer producer,
                                    Map<String, String> metadata) {
            if (FAIL_CREATE.get()) {
                throw new RuntimeException("fail producer create");
            }
        }

        @Override
        public void producerClosed(org.apache.pulsar.broker.service.ServerCnx cnx,
                                   org.apache.pulsar.broker.service.Producer producer,
                                   Map<String, String> metadata) {
            PRODUCER_CLOSE_INVOCATIONS.incrementAndGet();
            if (FAIL_CLOSE.get()) {
                throw new RuntimeException("fail producer close");
            }
        }

        @Override
        public void consumerCreated(org.apache.pulsar.broker.service.ServerCnx cnx,
                                    org.apache.pulsar.broker.service.Consumer consumer,
                                    Map<String, String> metadata) {
            if (FAIL_CREATE.get()) {
                throw new RuntimeException("fail consumer create");
            }
        }

        @Override
        public void consumerClosed(org.apache.pulsar.broker.service.ServerCnx cnx,
                                   org.apache.pulsar.broker.service.Consumer consumer,
                                   Map<String, String> metadata) {
            CONSUMER_CLOSE_INVOCATIONS.incrementAndGet();
            if (FAIL_CLOSE.get()) {
                throw new RuntimeException("fail consumer close");
            }
        }

        @Override
        public void initialize(org.apache.pulsar.broker.PulsarService pulsarService) {
        }

        @Override
        public void close() {
        }

        @Override
        public void onPulsarCommand(org.apache.pulsar.common.api.proto.BaseCommand command,
                                    org.apache.pulsar.broker.service.ServerCnx cnx) {
        }

        @Override
        public void onConnectionClosed(org.apache.pulsar.broker.service.ServerCnx cnx) {
        }

        @Override
        public void onWebserviceRequest(jakarta.servlet.ServletRequest request) {
        }

        @Override
        public void onWebserviceResponse(jakarta.servlet.ServletRequest request,
                                         jakarta.servlet.ServletResponse response) {
        }
    }
}