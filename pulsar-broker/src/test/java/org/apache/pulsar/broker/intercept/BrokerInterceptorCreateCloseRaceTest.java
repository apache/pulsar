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
import static org.testng.Assert.assertTrue;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class BrokerInterceptorCreateCloseRaceTest extends ProducerConsumerBase {

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

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @AfterMethod(alwaysRun = true)
    public void resetInterceptor() {
        BlockingConsumerCreateInterceptor.reset();
        if (counterBrokerInterceptor != null) {
            counterBrokerInterceptor.reset();
        }
    }

    @Override
    protected void customizeMainPulsarTestContextBuilder(PulsarTestContext.Builder pulsarTestContextBuilder) {
        NarClassLoader narClassLoader = org.mockito.Mockito.mock(NarClassLoader.class,
                delegatesTo(new URLClassLoader(new URL[0], getClass().getClassLoader())));
        counterBrokerInterceptor = new CounterBrokerInterceptor();
        Map<String, BrokerInterceptorWithClassLoader> interceptors = new HashMap<>();
        interceptors.put(
                "blocking-create",
                new BrokerInterceptorWithClassLoader(new BlockingConsumerCreateInterceptor(), narClassLoader));
        interceptors.put(
                CounterBrokerInterceptor.NAME,
                new BrokerInterceptorWithClassLoader(counterBrokerInterceptor, narClassLoader));
        pulsarTestContextBuilder.brokerInterceptor(new BrokerInterceptors(interceptors));
    }

    @Test
    public void testConsumerClosedDeliveredAfterCreateDuringDisconnect() throws Exception {
        String topic = "persistent://public/default/create-close-race-" + UUID.randomUUID();
        String subName = "sub";
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            var subscribeFuture = executor.submit(() -> {
                try {
                    return pulsarClient.newConsumer(Schema.STRING)
                            .topic(topic)
                            .subscriptionName(subName)
                            .subscribe();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            assertTrue(BlockingConsumerCreateInterceptor.createEntered
                    .await(10, TimeUnit.SECONDS));
            PersistentTopic brokerTopic = (PersistentTopic) pulsar.getBrokerService()
                    .getTopic(topic, false).get().orElseThrow();
            PersistentSubscription subscription =
                    (PersistentSubscription) brokerTopic.getSubscription(subName);
            org.apache.pulsar.broker.service.Consumer brokerConsumer = subscription.getConsumers().get(0);
            brokerConsumer.disconnect();
            BlockingConsumerCreateInterceptor.unblockCreate.countDown();
            subscribeFuture.get(10, TimeUnit.SECONDS).close();
            Awaitility.await().atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(() ->
                            assertEquals(counterBrokerInterceptor.getConsumerClosedInvocations().get(), 1));
        } finally {
            executor.shutdownNow();
        }
    }

    private static final class BlockingConsumerCreateInterceptor implements BrokerInterceptor {

        private static final Object LOCK = new Object();
        static CountDownLatch createEntered = new CountDownLatch(1);
        static CountDownLatch unblockCreate = new CountDownLatch(1);

        static void reset() {
            synchronized (LOCK) {
                createEntered = new CountDownLatch(1);
                unblockCreate = new CountDownLatch(1);
            }
        }

        @Override
        public void consumerCreated(org.apache.pulsar.broker.service.ServerCnx cnx,
                                    org.apache.pulsar.broker.service.Consumer consumer,
                                    Map<String, String> metadata) {
            createEntered.countDown();
            try {
                if (!unblockCreate.await(30, TimeUnit.SECONDS)) {
                    throw new RuntimeException("timed out waiting to unblock consumerCreated");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
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