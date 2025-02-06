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

import static org.apache.pulsar.broker.service.persistent.PersistentTopic.DEDUPLICATION_CURSOR_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.TopicPoliciesService;
import org.apache.pulsar.broker.service.TopicPolicyListener;
import org.apache.pulsar.broker.transaction.buffer.TransactionBuffer;
import org.apache.pulsar.broker.transaction.buffer.TransactionBufferProvider;
import org.apache.pulsar.broker.transaction.buffer.impl.TransactionBufferDisable;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.awaitility.Awaitility;
import org.awaitility.reflect.WhiteboxImpl;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-api")
public class OrphanPersistentTopicTest extends ProducerConsumerBase {

    @BeforeClass(alwaysRun = true)
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
    public void testNoOrphanTopicAfterCreateTimeout() throws Exception {
        // Make the topic loading timeout faster.
        int topicLoadTimeoutSeconds = 2;
        long originalTopicLoadTimeoutSeconds = pulsar.getConfig().getTopicLoadTimeoutSeconds();
        pulsar.getConfig().setTopicLoadTimeoutSeconds(2);

        String tpName = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        String mlPath = BrokerService.MANAGED_LEDGER_PATH_ZNODE + "/" + TopicName.get(tpName).getPersistenceNamingEncoding();

        // Make topic load timeout 5 times.
        AtomicInteger timeoutCounter = new AtomicInteger();
        for (int i = 0; i < 5; i++) {
            mockZooKeeper.delay(topicLoadTimeoutSeconds * 2 * 1000, (op, path) -> {
                if (mlPath.equals(path)) {
                    log.info("Topic load timeout: " + timeoutCounter.incrementAndGet());
                    return true;
                }
                return false;
            });
        }

        // Load topic.
        CompletableFuture<Consumer<byte[]>> consumer = pulsarClient.newConsumer()
                .topic(tpName)
                .subscriptionName("my-sub")
                .subscribeAsync();

        // After create timeout 5 times, the topic will be created successful.
        Awaitility.await().ignoreExceptions().atMost(40, TimeUnit.SECONDS).untilAsserted(() -> {
            CompletableFuture<Optional<Topic>> future = pulsar.getBrokerService().getTopic(tpName, false);
            assertTrue(future.isDone());
            Optional<Topic> optional = future.get();
            assertTrue(optional.isPresent());
        });

        // Assert only one PersistentTopic was not closed.
        TopicPoliciesService topicPoliciesService = pulsar.getTopicPoliciesService();
        Map<TopicName, List<TopicPolicyListener<TopicPolicies>>> listeners =
                WhiteboxImpl.getInternalState(topicPoliciesService, "listeners");
        assertEquals(listeners.get(TopicName.get(tpName)).size(), 1);

        // cleanup.
        consumer.join().close();
        admin.topics().delete(tpName, false);
        pulsar.getConfig().setTopicLoadTimeoutSeconds(originalTopicLoadTimeoutSeconds);
    }

    @Test
    public void testCloseLedgerThatTopicAfterCreateTimeout() throws Exception {
        // Make the topic loading timeout faster.
        long originalTopicLoadTimeoutSeconds = pulsar.getConfig().getTopicLoadTimeoutSeconds();
        int topicLoadTimeoutSeconds = 1;
        pulsar.getConfig().setTopicLoadTimeoutSeconds(topicLoadTimeoutSeconds);
        pulsar.getConfig().setBrokerDeduplicationEnabled(true);
        pulsar.getConfig().setTransactionCoordinatorEnabled(true);
        String tpName = BrokerTestUtil.newUniqueName("persistent://public/default/tp2");

        // Mock message deduplication recovery speed topicLoadTimeoutSeconds
        String mlPath = BrokerService.MANAGED_LEDGER_PATH_ZNODE + "/" +
                TopicName.get(tpName).getPersistenceNamingEncoding() + "/" + DEDUPLICATION_CURSOR_NAME;
        mockZooKeeper.delay(topicLoadTimeoutSeconds * 1000, (op, path) -> {
            if (mlPath.equals(path)) {
                log.info("Topic load timeout: " + path);
                return true;
            }
            return false;
        });

        // First load topic will trigger timeout
        // The first topic load will trigger a timeout. When the topic closes, it will call transactionBuffer.close.
        // Here, we simulate a sleep to ensure that the ledger is not immediately closed.
        TransactionBufferProvider mockTransactionBufferProvider = new TransactionBufferProvider() {
            @Override
            public TransactionBuffer newTransactionBuffer(Topic originTopic) {
                return new TransactionBufferDisable(originTopic) {
                    @SneakyThrows
                    @Override
                    public CompletableFuture<Void> closeAsync() {
                        Thread.sleep(500);
                        return super.closeAsync();
                    }
                };
            }
        };
        TransactionBufferProvider originalTransactionBufferProvider = pulsar.getTransactionBufferProvider();
        pulsar.setTransactionBufferProvider(mockTransactionBufferProvider);
        CompletableFuture<Optional<Topic>> firstLoad = pulsar.getBrokerService().getTopic(tpName, true);
        Awaitility.await().ignoreExceptions().atMost(5, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                // assert first create topic timeout
                .untilAsserted(() -> {
                    assertTrue(firstLoad.isCompletedExceptionally());
                });

        // Once the first load topic times out, immediately to load the topic again.
        Producer<byte[]> producer = pulsarClient.newProducer().topic(tpName).create();
        for (int i = 0; i < 10; i++) {
            MessageId send = producer.send("msg".getBytes());
            Thread.sleep(100);
            assertNotNull(send);
        }

        // set to back
        pulsar.setTransactionBufferProvider(originalTransactionBufferProvider);
        pulsar.getConfig().setTopicLoadTimeoutSeconds(originalTopicLoadTimeoutSeconds);
        pulsar.getConfig().setBrokerDeduplicationEnabled(false);
        pulsar.getConfig().setTransactionCoordinatorEnabled(false);
    }

    @DataProvider(name = "whetherTimeoutOrNot")
    public Object[][] whetherTimeoutOrNot() {
        return new Object[][] {
            {true},
            {false}
        };
    }

    @Test(timeOut = 60 * 1000, dataProvider = "whetherTimeoutOrNot")
    public void testCheckOwnerShipFails(boolean injectTimeout) throws Exception {
        if (injectTimeout) {
            pulsar.getConfig().setTopicLoadTimeoutSeconds(5);
        }
        String ns = "public" + "/" + UUID.randomUUID().toString().replaceAll("-", "");
        String tpName = BrokerTestUtil.newUniqueName("persistent://" + ns + "/tp");
        admin.namespaces().createNamespace(ns);
        admin.topics().createNonPartitionedTopic(tpName);
        admin.namespaces().unload(ns);

        // Inject an error when calling "NamespaceService.isServiceUnitActiveAsync".
        AtomicInteger failedTimes = new AtomicInteger();
        NamespaceService namespaceService = pulsar.getNamespaceService();
        doAnswer(invocation -> {
            TopicName paramTp = (TopicName) invocation.getArguments()[0];
            if (paramTp.toString().equalsIgnoreCase(tpName) && failedTimes.incrementAndGet() <= 2) {
                if (injectTimeout) {
                    Thread.sleep(10 * 1000);
                }
                log.info("Failed {} times", failedTimes.get());
                return CompletableFuture.failedFuture(new RuntimeException("mocked error"));
            }
            return invocation.callRealMethod();
        }).when(namespaceService).isServiceUnitActiveAsync(any(TopicName.class));

        // Verify: the consumer can create successfully eventually.
        Consumer consumer = pulsarClient.newConsumer().topic(tpName).subscriptionName("s1").subscribe();

        // cleanup.
        if (injectTimeout) {
            pulsar.getConfig().setTopicLoadTimeoutSeconds(60);
        }
        consumer.close();
        admin.topics().delete(tpName);
    }

    @Test(timeOut = 60 * 1000, dataProvider = "whetherTimeoutOrNot")
    public void testTopicLoadAndDeleteAtTheSameTime(boolean injectTimeout) throws Exception {
        if (injectTimeout) {
            pulsar.getConfig().setTopicLoadTimeoutSeconds(5);
        }
        String ns = "public" + "/" + UUID.randomUUID().toString().replaceAll("-", "");
        String tpName = BrokerTestUtil.newUniqueName("persistent://" + ns + "/tp");
        admin.namespaces().createNamespace(ns);
        admin.topics().createNonPartitionedTopic(tpName);
        admin.namespaces().unload(ns);

        // Inject a race condition: load topic and delete topic execute at the same time.
        AtomicInteger mockRaceConditionCounter = new AtomicInteger();
        NamespaceService namespaceService = pulsar.getNamespaceService();
        doAnswer(invocation -> {
            TopicName paramTp = (TopicName) invocation.getArguments()[0];
            if (paramTp.toString().equalsIgnoreCase(tpName) && mockRaceConditionCounter.incrementAndGet() <= 1) {
                if (injectTimeout) {
                    Thread.sleep(10 * 1000);
                }
                log.info("Race condition occurs {} times", mockRaceConditionCounter.get());
                pulsar.getManagedLedgerFactory().delete(TopicName.get(tpName).getPersistenceNamingEncoding());
            }
            return invocation.callRealMethod();
        }).when(namespaceService).isServiceUnitActiveAsync(any(TopicName.class));

        // Verify: the consumer create failed due to pulsar does not allow to create topic automatically.
        try {
            pulsar.getBrokerService().getTopic(tpName, false, Collections.emptyMap()).join();
        } catch (Exception ex) {
            log.warn("Expected error", ex);
        }

        // Verify: the consumer create successfully after allowing to create topic automatically.
        Consumer consumer = pulsarClient.newConsumer().topic(tpName).subscriptionName("s1").subscribe();

        // cleanup.
        if (injectTimeout) {
            pulsar.getConfig().setTopicLoadTimeoutSeconds(60);
        }
        consumer.close();
        admin.topics().delete(tpName);
    }
}
