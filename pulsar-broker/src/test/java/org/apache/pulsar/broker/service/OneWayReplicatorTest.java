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
package org.apache.pulsar.broker.service;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import io.netty.util.concurrent.FastThreadLocalThread;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.persistent.GeoPersistentReplicator;
import org.apache.pulsar.broker.service.persistent.PersistentReplicator;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.ProducerBuilderImpl;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.util.FutureUtil;
import org.awaitility.Awaitility;
import org.awaitility.reflect.WhiteboxImpl;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class OneWayReplicatorTest extends OneWayReplicatorTestBase {

    @Override
    @BeforeClass(alwaysRun = true, timeOut = 300000)
    public void setup() throws Exception {
        super.setup();
    }

    @Override
    @AfterClass(alwaysRun = true, timeOut = 300000)
    public void cleanup() throws Exception {
        super.cleanup();
    }

    private void waitReplicatorStopped(String topicName) {
        Awaitility.await().untilAsserted(() -> {
            Optional<Topic> topicOptional2 = pulsar2.getBrokerService().getTopic(topicName, false).get();
            assertTrue(topicOptional2.isPresent());
            PersistentTopic persistentTopic2 = (PersistentTopic) topicOptional2.get();
            assertTrue(persistentTopic2.getProducers().isEmpty());
            Optional<Topic> topicOptional1 = pulsar2.getBrokerService().getTopic(topicName, false).get();
            assertTrue(topicOptional1.isPresent());
            PersistentTopic persistentTopic1 = (PersistentTopic) topicOptional2.get();
            assertTrue(persistentTopic1.getReplicators().isEmpty()
                    || !persistentTopic1.getReplicators().get(cluster2).isConnected());
        });
    }

    /**
     * Override "AbstractReplicator.producer" by {@param producer} and return the original value.
     */
    private ProducerImpl overrideProducerForReplicator(AbstractReplicator replicator, ProducerImpl newProducer)
            throws Exception {
        Field producerField = AbstractReplicator.class.getDeclaredField("producer");
        producerField.setAccessible(true);
        ProducerImpl originalValue = (ProducerImpl) producerField.get(replicator);
        synchronized (replicator) {
            producerField.set(replicator, newProducer);
        }
        return originalValue;
    }

    @Test(timeOut = 45 * 1000)
    public void testReplicatorProducerStatInTopic() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://" + replicatedNamespace + "/tp_");
        final String subscribeName = "subscribe_1";
        final byte[] msgValue = "test".getBytes();

        // Verify replicator works.
        Producer<byte[]> producer1 = client1.newProducer().topic(topicName).create();
        Producer<byte[]> producer2 = client2.newProducer().topic(topicName).create(); // Do not publish messages
        Consumer<byte[]> consumer2 = client2.newConsumer().topic(topicName).subscriptionName(subscribeName).subscribe();
        producer1.newMessage().value(msgValue).send();
        pulsar1.getBrokerService().checkReplicationPolicies();
        assertEquals(consumer2.receive(10, TimeUnit.SECONDS).getValue(), msgValue);

        // Verify that the "publishers" field does not include the producer for replication
        TopicStats topicStats2 = admin2.topics().getStats(topicName);
        assertEquals(topicStats2.getPublishers().size(), 1);
        assertFalse(topicStats2.getPublishers().get(0).getProducerName().startsWith(config1.getReplicatorPrefix()));

        // Update broker stats immediately (usually updated every minute)
        pulsar2.getBrokerService().updateRates();
        String brokerStats2 = admin2.brokerStats().getTopics();

        boolean found = false;
        ObjectMapper mapper = new ObjectMapper();
        JsonNode rootNode = mapper.readTree(brokerStats2);
        if (rootNode.hasNonNull(replicatedNamespace)) {
            Iterator<JsonNode> bundleNodes = rootNode.get(replicatedNamespace).elements();
            while (bundleNodes.hasNext()) {
                JsonNode bundleNode = bundleNodes.next();
                if (bundleNode.hasNonNull("persistent") && bundleNode.get("persistent").hasNonNull(topicName)) {
                    found = true;
                    JsonNode topicNode = bundleNode.get("persistent").get(topicName);
                    // Verify that the "publishers" field does not include the producer for replication
                    assertEquals(topicNode.get("publishers").size(), 1);
                    assertEquals(topicNode.get("producerCount").intValue(), 1);
                    Iterator<JsonNode> publisherNodes = topicNode.get("publishers").elements();
                    while (publisherNodes.hasNext()) {
                        JsonNode publisherNode = publisherNodes.next();
                        assertFalse(publisherNode.get("producerName").textValue()
                                .startsWith(config1.getReplicatorPrefix()));
                    }
                    break;
                }
            }
        }
        assertTrue(found);

        // cleanup.
        consumer2.unsubscribe();
        producer2.close();
        producer1.close();
        cleanupTopics(() -> {
            admin1.topics().delete(topicName);
            admin2.topics().delete(topicName);
        });
    }

    @Test(timeOut = 45 * 1000)
    public void testCreateRemoteConsumerFirst() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://" + replicatedNamespace + "/tp_");
        Producer<String> producer1 = client1.newProducer(Schema.STRING).topic(topicName).create();

        // The topic in cluster2 has a replicator created producer(schema Auto_Produce), but does not have any schema。
        // Verify: the consumer of this cluster2 can create successfully.
        Consumer<String> consumer2 = client2.newConsumer(Schema.STRING).topic(topicName).subscriptionName("s1")
                .subscribe();;
        // Wait for replicator started.
        waitReplicatorStarted(topicName);
        // cleanup.
        producer1.close();
        consumer2.close();
        cleanupTopics(() -> {
            admin1.topics().delete(topicName);
            admin2.topics().delete(topicName);
        });
    }

    @Test(timeOut = 45 * 1000)
    public void testTopicCloseWhenInternalProducerCloseErrorOnce() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://" + replicatedNamespace + "/tp_");
        admin1.topics().createNonPartitionedTopic(topicName);
        // Wait for replicator started.
        waitReplicatorStarted(topicName);
        PersistentTopic topic1 =
                (PersistentTopic) pulsar1.getBrokerService().getTopic(topicName, false).join().get();
        PersistentReplicator replicator1 =
                (PersistentReplicator) topic1.getReplicators().values().iterator().next();
        // Mock an error when calling "replicator.disconnect()"
        AtomicBoolean closeFailed = new AtomicBoolean(true);
        final ProducerImpl mockProducer = Mockito.mock(ProducerImpl.class);
        final AtomicReference<ProducerImpl> originalProducer1 = new AtomicReference();
        doAnswer(invocation -> {
            if (closeFailed.get()) {
                return CompletableFuture.failedFuture(new Exception("mocked ex"));
            } else {
                return originalProducer1.get().closeAsync();
            }
        }).when(mockProducer).closeAsync();
        originalProducer1.set(overrideProducerForReplicator(replicator1, mockProducer));
        // Verify: since the "replicator.producer.closeAsync()" will retry after it failed, the topic unload should be
        // successful.
        admin1.topics().unload(topicName);
        // Verify: After "replicator.producer.closeAsync()" retry again, the "replicator.producer" will be closed
        // successful.
        closeFailed.set(false);
        AtomicReference<PersistentTopic> topic2 = new AtomicReference();
        AtomicReference<PersistentReplicator> replicator2 = new AtomicReference();
        Awaitility.await().untilAsserted(() -> {
            topic2.set((PersistentTopic) pulsar1.getBrokerService().getTopic(topicName, false).join().get());
            replicator2.set((PersistentReplicator) topic2.get().getReplicators().values().iterator().next());
            // It is a new Topic after reloading.
            assertNotEquals(topic2.get(), topic1);
            assertNotEquals(replicator2.get(), replicator1);
        });
        Awaitility.await().untilAsserted(() -> {
            // Old replicator should be closed.
            Assert.assertFalse(replicator1.isConnected());
            Assert.assertFalse(originalProducer1.get().isConnected());
            // New replicator should be connected.
            Assert.assertTrue(replicator2.get().isConnected());
        });
        // cleanup.
        cleanupTopics(() -> {
            admin1.topics().delete(topicName);
            admin2.topics().delete(topicName);
        });
    }

    private Runnable injectMockReplicatorProducerBuilder(
                                BiFunction<ProducerConfigurationData, ProducerImpl, ProducerImpl> producerDecorator)
            throws Exception {
        String cluster2 = pulsar2.getConfig().getClusterName();
        BrokerService brokerService = pulsar1.getBrokerService();
        // Wait for the internal client created.
        final String topicNameTriggerInternalClientCreate =
                BrokerTestUtil.newUniqueName("persistent://" + replicatedNamespace + "/tp_");
        admin1.topics().createNonPartitionedTopic(topicNameTriggerInternalClientCreate);
        waitReplicatorStarted(topicNameTriggerInternalClientCreate);
        cleanupTopics(() -> {
            admin1.topics().delete(topicNameTriggerInternalClientCreate);
            admin2.topics().delete(topicNameTriggerInternalClientCreate);
        });

        // Inject spy client.
        ConcurrentOpenHashMap<String, PulsarClient>
                replicationClients = WhiteboxImpl.getInternalState(brokerService, "replicationClients");
        PulsarClientImpl internalClient = (PulsarClientImpl) replicationClients.get(cluster2);
        PulsarClient spyClient = spy(internalClient);
        assertTrue(replicationClients.remove(cluster2, internalClient));
        assertNull(replicationClients.putIfAbsent(cluster2, spyClient));

        // Inject producer decorator.
        doAnswer(invocation -> {
            Schema schema = (Schema) invocation.getArguments()[0];
            ProducerBuilderImpl<?> producerBuilder = (ProducerBuilderImpl) internalClient.newProducer(schema);
            ProducerBuilder spyProducerBuilder = spy(producerBuilder);
            doAnswer(ignore -> {
                CompletableFuture<Producer> producerFuture = new CompletableFuture<>();
                producerBuilder.createAsync().whenComplete((p, t) -> {
                    if (t != null) {
                        producerFuture.completeExceptionally(t);
                        return;
                    }
                    ProducerImpl pImpl = (ProducerImpl) p;
                    new FastThreadLocalThread(() -> {
                        try {
                            ProducerImpl newProducer = producerDecorator.apply(producerBuilder.getConf(), pImpl);
                            producerFuture.complete(newProducer);
                        } catch (Exception ex) {
                            producerFuture.completeExceptionally(ex);
                        }
                    }).start();
                });

                return producerFuture;
            }).when(spyProducerBuilder).createAsync();
            return spyProducerBuilder;
        }).when(spyClient).newProducer(any(Schema.class));

        // Return a cleanup injection task;
        return () -> {
            assertTrue(replicationClients.remove(cluster2, spyClient));
            assertNull(replicationClients.putIfAbsent(cluster2, internalClient));
        };
    }

    private SpyCursor spyCursor(PersistentTopic persistentTopic, String cursorName) throws Exception {
        ManagedLedgerImpl ml = (ManagedLedgerImpl) persistentTopic.getManagedLedger();
        ManagedCursorImpl cursor = (ManagedCursorImpl) ml.getCursors().get(cursorName);
        ManagedCursorImpl spyCursor = spy(cursor);
        // remove cursor.
        ml.getCursors().removeCursor(cursorName);
        ml.deactivateCursor(cursor);
        // Add the spy one. addCursor(ManagedCursorImpl cursor)
        Method m = ManagedLedgerImpl.class.getDeclaredMethod("addCursor", new Class[]{ManagedCursorImpl.class});
        m.setAccessible(true);
        m.invoke(ml, new Object[]{spyCursor});
        return new SpyCursor(cursor, spyCursor);
    }

    @Data
    @AllArgsConstructor
    static class SpyCursor {
        ManagedCursorImpl original;
        ManagedCursorImpl spy;
    }

    private CursorCloseSignal makeCursorClosingDelay(SpyCursor spyCursor) throws Exception {
        CountDownLatch startCloseSignal = new CountDownLatch(1);
        CountDownLatch startCallbackSignal = new CountDownLatch(1);
        doAnswer(invocation -> {
            AsyncCallbacks.CloseCallback originalCallback = (AsyncCallbacks.CloseCallback) invocation.getArguments()[0];
            Object ctx = invocation.getArguments()[1];
            AsyncCallbacks.CloseCallback newCallback = new AsyncCallbacks.CloseCallback() {
                @Override
                public void closeComplete(Object ctx) {
                    new FastThreadLocalThread(new Runnable() {
                        @Override
                        @SneakyThrows
                        public void run() {
                            startCallbackSignal.await();
                            originalCallback.closeComplete(ctx);
                        }
                    }).start();
                }

                @Override
                public void closeFailed(ManagedLedgerException exception, Object ctx) {
                    new FastThreadLocalThread(new Runnable() {
                        @Override
                        @SneakyThrows
                        public void run() {
                            startCallbackSignal.await();
                            originalCallback.closeFailed(exception, ctx);
                        }
                    }).start();
                }
            };
            startCloseSignal.await();
            spyCursor.original.asyncClose(newCallback, ctx);
            return null;
        }).when(spyCursor.spy).asyncClose(any(AsyncCallbacks.CloseCallback.class), any());
        return new CursorCloseSignal(startCloseSignal, startCallbackSignal);
    }

    @AllArgsConstructor
    static class CursorCloseSignal {
        CountDownLatch startCloseSignal;
        CountDownLatch startCallbackSignal;

        void startClose() {
            startCloseSignal.countDown();
        }

        void startCallback() {
            startCallbackSignal.countDown();
        }
    }

    /**
     * See the description and execution flow: https://github.com/apache/pulsar/pull/21946.
     * Steps:
     * - Create topic, but the internal producer of Replicator created failed.
     * - Unload bundle, the Replicator will be closed, but the internal producer creation retry has not executed yet.
     * - The internal producer creation retry execute successfully, the "repl.cursor" has not been closed yet.
     * - The topic is wholly closed.
     * - Verify: the delayed created internal producer will be closed.
     */
    @Test(timeOut = 120 * 1000)
    public void testConcurrencyOfUnloadBundleAndRecreateProducer() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://" + replicatedNamespace + "/tp_");
        // Inject an error for "replicator.producer" creation.
        // The delay time of next retry to create producer is below:
        //   0.1s, 0.2, 0.4, 0.8, 1.6s, 3.2s, 6.4s...
        //   If the retry counter is larger than 6, the next creation will be slow enough to close Replicator.
        final AtomicInteger createProducerCounter = new AtomicInteger();
        final int failTimes = 6;
        Runnable taskToClearInjection = injectMockReplicatorProducerBuilder((producerCnf, originalProducer) -> {
            if (topicName.equals(producerCnf.getTopicName())) {
                // There is a switch to determine create producer successfully or not.
                if (createProducerCounter.incrementAndGet() > failTimes) {
                    return originalProducer;
                }
                log.info("Retry create replicator.producer count: {}", createProducerCounter);
                // Release producer and fail callback.
                originalProducer.closeAsync();
                throw new RuntimeException("mock error");
            }
            return originalProducer;
        });

        // Create topic.
        admin1.topics().createNonPartitionedTopic(topicName);
        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar1.getBrokerService().getTopic(topicName, false).join().get();
        PersistentReplicator replicator =
                (PersistentReplicator) persistentTopic.getReplicators().values().iterator().next();
        // Since we inject a producer creation error, the replicator can not start successfully.
        assertFalse(replicator.isConnected());

        // Stuck the closing of the cursor("pulsar.repl"), until the internal producer of the replicator started.
        SpyCursor spyCursor =
                spyCursor(persistentTopic, "pulsar.repl." + pulsar2.getConfig().getClusterName());
        CursorCloseSignal cursorCloseSignal = makeCursorClosingDelay(spyCursor);

        // Unload bundle: call "topic.close(false)".
        // Stuck start new producer, until the state of replicator change to Stopped.
        // The next once of "createProducerSuccessAfterFailTimes" to create producer will be successfully.
        Awaitility.await().pollInterval(Duration.ofMillis(100)).atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
            assertTrue(createProducerCounter.get() >= failTimes,
                    "count of retry to create producer is " + createProducerCounter.get());
        });
        CompletableFuture<Void> topicCloseFuture = persistentTopic.close(true);
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            String state = String.valueOf(replicator.getState());
            assertTrue(state.equals("Stopped") || state.equals("Terminated"));
        });

        // Delay close cursor, until "replicator.producer" create successfully.
        // The next once retry time of create "replicator.producer" will be 3.2s.
        Thread.sleep(4 * 1000);
        log.info("Replicator.state: {}", replicator.getState());
        cursorCloseSignal.startClose();
        cursorCloseSignal.startCallback();

        // Wait for topic close successfully.
        // Verify there is no orphan producer on the remote cluster.
        topicCloseFuture.join();
        Awaitility.await().pollInterval(Duration.ofSeconds(1)).untilAsserted(() -> {
            PersistentTopic persistentTopic2 =
                    (PersistentTopic) pulsar2.getBrokerService().getTopic(topicName, false).join().get();
            assertEquals(persistentTopic2.getProducers().size(), 0);
            Assert.assertFalse(replicator.isConnected());
        });

        // cleanup.
        taskToClearInjection.run();
        cleanupTopics(() -> {
            admin1.topics().delete(topicName);
            admin2.topics().delete(topicName);
        });
    }

    @Test
    public void testPartitionedTopicLevelReplication() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://" + nonReplicatedNamespace + "/tp_");
        final String partition0 = TopicName.get(topicName).getPartition(0).toString();
        final String partition1 = TopicName.get(topicName).getPartition(1).toString();
        admin1.topics().createPartitionedTopic(topicName, 2);
        admin1.topics().setReplicationClusters(topicName, Arrays.asList(cluster1, cluster2));
        // Check the partitioned topic has been created at the remote cluster.
        PartitionedTopicMetadata topicMetadata2 = admin2.topics().getPartitionedTopicMetadata(topicName);
        assertEquals(topicMetadata2.partitions, 2);
        // cleanup.
        admin1.topics().setReplicationClusters(topicName, Arrays.asList(cluster1));
        waitReplicatorStopped(partition0);
        waitReplicatorStopped(partition1);
        admin1.topics().deletePartitionedTopic(topicName);
        admin2.topics().deletePartitionedTopic(topicName);
    }

    @Test
    public void testPartitionedTopicLevelReplicationRemoteTopicExist() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://" + nonReplicatedNamespace + "/tp_");
        final String partition0 = TopicName.get(topicName).getPartition(0).toString();
        final String partition1 = TopicName.get(topicName).getPartition(1).toString();
        admin1.topics().createPartitionedTopic(topicName, 2);
        admin2.topics().createPartitionedTopic(topicName, 2);
        admin1.topics().setReplicationClusters(topicName, Arrays.asList(cluster1, cluster2));
        // Check the partitioned topic has been created at the remote cluster.
        Awaitility.await().untilAsserted(() -> {
            PartitionedTopicMetadata topicMetadata2 = admin2.topics().getPartitionedTopicMetadata(topicName);
            assertEquals(topicMetadata2.partitions, 2);
        });

        // Expand partitions
        admin2.topics().updatePartitionedTopic(topicName, 3);
        Awaitility.await().untilAsserted(() -> {
            PartitionedTopicMetadata topicMetadata2 = admin2.topics().getPartitionedTopicMetadata(topicName);
            assertEquals(topicMetadata2.partitions, 3);
        });
        // cleanup.
        admin1.topics().setReplicationClusters(topicName, Arrays.asList(cluster1));
        waitReplicatorStopped(partition0);
        waitReplicatorStopped(partition1);
        admin1.topics().deletePartitionedTopic(topicName);
        admin2.topics().deletePartitionedTopic(topicName);
    }

    @Test
    public void testPartitionedTopicLevelReplicationRemoteConflictTopicExist() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://" + nonReplicatedNamespace + "/tp_");
        admin2.topics().createPartitionedTopic(topicName, 3);
        admin1.topics().createPartitionedTopic(topicName, 2);
        try {
            admin1.topics().setReplicationClusters(topicName, Arrays.asList(cluster1, cluster2));
            fail("Expected error due to a conflict partitioned topic already exists.");
        } catch (Exception ex) {
            Throwable unWrapEx = FutureUtil.unwrapCompletionException(ex);
            assertTrue(unWrapEx.getMessage().contains("with different partitions"));
        }
        // Check nothing changed.
        PartitionedTopicMetadata topicMetadata2 = admin2.topics().getPartitionedTopicMetadata(topicName);
        assertEquals(topicMetadata2.partitions, 3);
        assertEquals(admin1.topics().getReplicationClusters(topicName, true).size(), 1);
        // cleanup.
        admin1.topics().deletePartitionedTopic(topicName);
        admin2.topics().deletePartitionedTopic(topicName);
    }

    /**
     * See the description and execution flow: https://github.com/apache/pulsar/pull/21948.
     * Steps:
     * 1.Create topic, does not enable replication now.
     *   - The topic will be loaded in the memory.
     * 2.Enable namespace level replication.
     *   - Broker creates a replicator, and the internal producer of replicator is starting.
     *   - We inject an error to make the internal producer fail to connect，after few seconds, it will retry to start.
     * 3.Unload bundle.
     *   - Starting to close the topic.
     *   - The replicator will be closed, but it will not close the internal producer, because the producer has not
     *     been created successfully.
     *   - We inject a sleeping into the progress of closing the "repl.cursor" to make it stuck. So the topic is still
     *     in the process of being closed now.
     * 4.Internal producer retry to connect.
     *   - At the next retry, it connected successful. Since the state of "repl.cursor" is not "Closed", this producer
     *     will not be closed now.
     * 5.Topic closed.
     *   - Cancel the stuck of closing the "repl.cursor".
     *   - The topic is wholly closed.
     * 6.Verify: the delayed created internal producer will be closed. In other words, there is no producer is connected
     *   to the remote cluster.
     */
    @Test
    public void testConcurrencyOfUnloadBundleAndRecreateProducer2() throws Exception {
        final String namespaceName = defaultTenant + "/" + UUID.randomUUID().toString().replaceAll("-", "");
        final String topicName = BrokerTestUtil.newUniqueName("persistent://" + namespaceName + "/tp_");
        // 1.Create topic, does not enable replication now.
        admin1.namespaces().createNamespace(namespaceName);
        admin2.namespaces().createNamespace(namespaceName);
        admin1.topics().createNonPartitionedTopic(topicName);
        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar1.getBrokerService().getTopic(topicName, false).join().get();

        // We inject an error to make the internal producer fail to connect.
        // The delay time of next retry to create producer is below:
        //   0.1s, 0.2, 0.4, 0.8, 1.6s, 3.2s, 6.4s...
        //   If the retry counter is larger than 6, the next creation will be slow enough to close Replicator.
        final AtomicInteger createProducerCounter = new AtomicInteger();
        final int failTimes = 6;
        Runnable taskToClearInjection = injectMockReplicatorProducerBuilder((producerCnf, originalProducer) -> {
            if (topicName.equals(producerCnf.getTopicName())) {
                // There is a switch to determine create producer successfully or not.
                if (createProducerCounter.incrementAndGet() > failTimes) {
                    return originalProducer;
                }
                log.info("Retry create replicator.producer count: {}", createProducerCounter);
                // Release producer and fail callback.
                originalProducer.closeAsync();
                throw new RuntimeException("mock error");
            }
            return originalProducer;
        });

        // 2.Enable namespace level replication.
        admin1.namespaces().setNamespaceReplicationClusters(namespaceName, Sets.newHashSet(cluster1, cluster2));
        AtomicReference<PersistentReplicator> replicator = new AtomicReference<PersistentReplicator>();
        Awaitility.await().untilAsserted(() -> {
            assertFalse(persistentTopic.getReplicators().isEmpty());
            replicator.set(
                    (PersistentReplicator) persistentTopic.getReplicators().values().iterator().next());
            // Since we inject a producer creation error, the replicator can not start successfully.
            assertFalse(replicator.get().isConnected());
        });

        // We inject a sleeping into the progress of closing the "repl.cursor" to make it stuck, until the internal
        // producer of the replicator started.
        SpyCursor spyCursor =
                spyCursor(persistentTopic, "pulsar.repl." + pulsar2.getConfig().getClusterName());
        CursorCloseSignal cursorCloseSignal = makeCursorClosingDelay(spyCursor);

        // 3.Unload bundle: call "topic.close(false)".
        // Stuck start new producer, until the state of replicator change to Stopped.
        // The next once of "createProducerSuccessAfterFailTimes" to create producer will be successfully.
        Awaitility.await().pollInterval(Duration.ofMillis(100)).atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
            assertTrue(createProducerCounter.get() >= failTimes);
        });
        CompletableFuture<Void> topicCloseFuture = persistentTopic.close(true);
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            String state = String.valueOf(replicator.get().getState());
            log.error("replicator state: {}", state);
            assertTrue(state.equals("Disconnected") || state.equals("Terminated"));
        });

        // 5.Delay close cursor, until "replicator.producer" create successfully.
        // The next once retry time of create "replicator.producer" will be 3.2s.
        Thread.sleep(4 * 1000);
        log.info("Replicator.state: {}", replicator.get().getState());
        cursorCloseSignal.startClose();
        cursorCloseSignal.startCallback();
        // Wait for topic close successfully.
        topicCloseFuture.join();

        // 6. Verify there is no orphan producer on the remote cluster.
        Awaitility.await().pollInterval(Duration.ofSeconds(1)).untilAsserted(() -> {
            PersistentTopic persistentTopic2 =
                    (PersistentTopic) pulsar2.getBrokerService().getTopic(topicName, false).join().get();
            assertEquals(persistentTopic2.getProducers().size(), 0);
            Assert.assertFalse(replicator.get().isConnected());
        });

        // cleanup.
        taskToClearInjection.run();
        cleanupTopics(namespaceName, () -> {
            admin1.topics().delete(topicName);
            admin2.topics().delete(topicName);
        });
        admin1.namespaces().setNamespaceReplicationClusters(namespaceName, Sets.newHashSet(cluster1));
        admin1.namespaces().deleteNamespace(namespaceName);
        admin2.namespaces().deleteNamespace(namespaceName);
    }

    @Test
    public void testUnFenceTopicToReuse() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://" + replicatedNamespace + "/tp");
        // Wait for replicator started.
        Producer<String> producer1 = client1.newProducer(Schema.STRING).topic(topicName).create();
        waitReplicatorStarted(topicName);

        // Inject an error to make topic close fails.
        final String mockProducerName = UUID.randomUUID().toString();
        final org.apache.pulsar.broker.service.Producer mockProducer =
                mock(org.apache.pulsar.broker.service.Producer.class);
        doAnswer(invocation -> CompletableFuture.failedFuture(new RuntimeException("mocked error")))
                .when(mockProducer).disconnect(any());
        doAnswer(invocation -> CompletableFuture.failedFuture(new RuntimeException("mocked error")))
                .when(mockProducer).disconnect();
        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar1.getBrokerService().getTopic(topicName, false).join().get();
        persistentTopic.getProducers().put(mockProducerName, mockProducer);

        // Do close.
        GeoPersistentReplicator replicator1 =
                (GeoPersistentReplicator) persistentTopic.getReplicators().values().iterator().next();
        try {
            persistentTopic.close(true, false).join();
            fail("Expected close fails due to a producer close fails");
        } catch (Exception ex) {
            log.info("Expected error: {}", ex.getMessage());
        }

        // Broker will call `topic.unfenceTopicToResume` if close clients fails.
        // Verify: the replicator will be re-created.
        Awaitility.await().untilAsserted(() -> {
            assertTrue(producer1.isConnected());
            GeoPersistentReplicator replicator2 =
                    (GeoPersistentReplicator) persistentTopic.getReplicators().values().iterator().next();
            assertNotEquals(replicator1, replicator2);
            assertFalse(replicator1.isConnected());
            assertFalse(replicator1.producer != null && replicator1.producer.isConnected());
            assertTrue(replicator2.isConnected());
            assertTrue(replicator2.producer != null && replicator2.producer.isConnected());
        });

        // cleanup the injection.
        persistentTopic.getProducers().remove(mockProducerName, mockProducer);
        // cleanup.
        producer1.close();
        cleanupTopics(() -> {
            admin1.topics().delete(topicName);
            admin2.topics().delete(topicName);
        });
    }

    @Test
    public void testDeleteNonPartitionedTopic() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://" + replicatedNamespace + "/tp_");
        admin1.topics().createNonPartitionedTopic(topicName);

        // Verify replicator works.
        verifyReplicationWorks(topicName);

        // Disable replication.
        setTopicLevelClusters(topicName, Arrays.asList(cluster1), admin1, pulsar1);
        setTopicLevelClusters(topicName, Arrays.asList(cluster2), admin2, pulsar2);

        // Delete topic.
        admin1.topics().delete(topicName);
        admin2.topics().delete(topicName);

        // Verify the topic was deleted.
        assertFalse(pulsar1.getPulsarResources().getTopicResources()
                .persistentTopicExists(TopicName.get(topicName)).join());
        assertFalse(pulsar2.getPulsarResources().getTopicResources()
                .persistentTopicExists(TopicName.get(topicName)).join());
    }

    @Test
    public void testDeletePartitionedTopic() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://" + replicatedNamespace + "/tp_");
        admin1.topics().createPartitionedTopic(topicName, 2);

        // Verify replicator works.
        verifyReplicationWorks(topicName);

        // Disable replication.
        setTopicLevelClusters(topicName, Arrays.asList(cluster1), admin1, pulsar1);
        setTopicLevelClusters(topicName, Arrays.asList(cluster2), admin2, pulsar2);

        // Delete topic.
        admin1.topics().deletePartitionedTopic(topicName);
        if (!usingGlobalZK) {
            admin2.topics().deletePartitionedTopic(topicName);
        }

        // Verify the topic was deleted.
        assertFalse(pulsar1.getPulsarResources().getNamespaceResources().getPartitionedTopicResources()
                .partitionedTopicExists(TopicName.get(topicName)));
        assertFalse(pulsar2.getPulsarResources().getNamespaceResources().getPartitionedTopicResources()
                .partitionedTopicExists(TopicName.get(topicName)));
        if (!usingGlobalZK) {
            // So far, the topic partitions on the remote cluster are needed to delete manually when using global ZK.
            assertFalse(pulsar1.getPulsarResources().getTopicResources()
                    .persistentTopicExists(TopicName.get(topicName).getPartition(0)).join());
            assertFalse(pulsar2.getPulsarResources().getTopicResources()
                    .persistentTopicExists(TopicName.get(topicName).getPartition(0)).join());
            assertFalse(pulsar1.getPulsarResources().getTopicResources()
                    .persistentTopicExists(TopicName.get(topicName).getPartition(1)).join());
            assertFalse(pulsar2.getPulsarResources().getTopicResources()
                    .persistentTopicExists(TopicName.get(topicName).getPartition(1)).join());
        }
    }

    @Test
    public void testNoExpandTopicPartitionsWhenDisableTopicLevelReplication() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://" + replicatedNamespace + "/tp_");
        admin1.topics().createPartitionedTopic(topicName, 2);

        // Verify replicator works.
        verifyReplicationWorks(topicName);

        // Disable topic level replication.
        setTopicLevelClusters(topicName, Arrays.asList(cluster1), admin1, pulsar1);
        setTopicLevelClusters(topicName, Arrays.asList(cluster2), admin2, pulsar2);

        // Expand topic.
        admin1.topics().updatePartitionedTopic(topicName, 3);
        assertEquals(admin1.topics().getPartitionedTopicMetadata(topicName).partitions, 3);

        // Wait for async tasks that were triggered by expanding topic partitions.
        Thread.sleep(3 * 1000);


        // Verify: the topics on the remote cluster did not been expanded.
        assertEquals(admin2.topics().getPartitionedTopicMetadata(topicName).partitions, 2);

        cleanupTopics(() -> {
            admin1.topics().deletePartitionedTopic(topicName, false);
            admin2.topics().deletePartitionedTopic(topicName, false);
        });
    }

    @Test
    public void testExpandTopicPartitionsOnNamespaceLevelReplication() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://" + replicatedNamespace + "/tp_");
        admin1.topics().createPartitionedTopic(topicName, 2);

        // Verify replicator works.
        verifyReplicationWorks(topicName);

        // Expand topic.
        admin1.topics().updatePartitionedTopic(topicName, 3);
        assertEquals(admin1.topics().getPartitionedTopicMetadata(topicName).partitions, 3);

        // Verify: the topics on the remote cluster will be expanded.
        Awaitility.await().untilAsserted(() -> {
            assertEquals(admin2.topics().getPartitionedTopicMetadata(topicName).partitions, 3);
        });

        cleanupTopics(() -> {
            admin1.topics().deletePartitionedTopic(topicName, false);
            admin2.topics().deletePartitionedTopic(topicName, false);
        });
    }
}
