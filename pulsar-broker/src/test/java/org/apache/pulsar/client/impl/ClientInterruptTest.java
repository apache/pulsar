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
package org.apache.pulsar.client.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.intercept.MockBrokerInterceptor;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.naming.TopicName;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-impl")
public class ClientInterruptTest extends ProducerConsumerBase {

    private final CreationInterceptor interceptor = new CreationInterceptor();
    private int index = 0;
    private PulsarClientImpl client;
    private String topic;
    private ExecutorService executor;
    private CompletableFuture<Void> delayTriggered;

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
        client = (PulsarClientImpl) PulsarClient.builder().serviceUrl(pulsar.getBrokerServiceUrl()).build();
        pulsar.getBrokerService().setInterceptor(interceptor);
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @BeforeMethod
    public void setupTopic() {
        interceptor.numConsumerCreated.set(0);
        interceptor.numProducerCreated.set(0);
        executor = Executors.newCachedThreadPool();
        TopicName topicName = TopicName.get("test-topic-" + index++);
        topic = topicName.toString();

        final var mlPath = BrokerService.MANAGED_LEDGER_PATH_ZNODE + "/" + topicName.getPersistenceNamingEncoding();
        delayTriggered = new CompletableFuture<>();
        mockZooKeeper.delay(1000L, (op, path) -> {
            final var result = path.equals(mlPath);
            if (result) {
                log.info("Injected delay for {} {}", op, path);
                delayTriggered.complete(null);
            }
            return result;
        });
    }

    @AfterMethod(alwaysRun = true, timeOut = 10000)
    public void cleanupTopic() {
        executor.shutdown();
    }

    @Test(timeOut = 10000)
    public void testCreateProducer() throws Exception {
        testCreateInterrupt("producer", () -> client.newProducer().topic(topic).create());
    }

    @Test(timeOut = 10000)
    public void testSubscribe() throws Exception {
        testCreateInterrupt("consumer", () -> client.newConsumer().topic(topic).subscriptionName("sub")
                .subscribe());
    }

    @Test(timeOut = 10000)
    public void testCreateReader() throws Exception {
        testCreateInterrupt("reader", () -> client.newReader().topic(topic).startMessageId(MessageId.earliest)
                .create());
    }

    private void testCreateInterrupt(String name, PulsarClientSyncTask task) throws Exception {
        final var exception = new AtomicReference<PulsarClientException>();
        final var threadInterrupted = new CompletableFuture<Boolean>();
        final var future = executor.submit(() -> {
            try {
                task.run();
                exception.set(new PulsarClientException("Task " + name + " succeeded"));
            } catch (PulsarClientException e) {
                exception.set(e);
            }

            try {
                Thread.sleep(1);
                threadInterrupted.complete(false);
            } catch (InterruptedException __) {
                threadInterrupted.complete(true);
            }
        });
        delayTriggered.get();
        future.cancel(true);

        Awaitility.await().untilAsserted(() -> assertNotNull(exception.get()));
        assertTrue(exception.get().getCause() instanceof InterruptedException);

        Awaitility.await().untilAsserted(() -> assertTrue(pulsar.getBrokerService().getTopics().containsKey(topic)));
        final var persistentTopic = (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(topic).get()
                .orElseThrow();

        if (name.equals("producer")) {
            Awaitility.await().untilAsserted(() -> assertEquals(interceptor.numProducerCreated.get(), 1));
            // Verify the created producer will eventually be closed
            Awaitility.await().untilAsserted(() -> {
                assertEquals(persistentTopic.getProducers().size(), 0);
                assertEquals(client.producersCount(), 0);
            });
        } else {
            Awaitility.await().untilAsserted(() -> assertEquals(interceptor.numConsumerCreated.get(), 1));
            // Verify the created consumer will eventually be closed
            Awaitility.await().untilAsserted(() -> {
                persistentTopic.getSubscriptions().values().forEach(subscription ->
                        assertTrue(subscription.getConsumers().isEmpty()));
                assertEquals(client.consumersCount(), 0);
            });
        }
        // The thread's interrupt state should not be set, it's the caller's responsibility to set the interrupt state
        // if necessary when catching the `PulsarClientException` that wraps an `InterruptedException`
        assertFalse(threadInterrupted.get());
    }

    private interface PulsarClientSyncTask {

        void run() throws PulsarClientException;
    }


    private static class CreationInterceptor extends MockBrokerInterceptor  {

        final AtomicInteger numProducerCreated = new AtomicInteger(0);
        final AtomicInteger numConsumerCreated = new AtomicInteger(0);

        @Override
        public void producerCreated(ServerCnx cnx, Producer producer, Map<String, String> metadata) {
            numProducerCreated.incrementAndGet();
        }

        @Override
        public void consumerCreated(ServerCnx cnx, Consumer consumer, Map<String, String> metadata) {
            numConsumerCreated.incrementAndGet();
        }
    }
}
