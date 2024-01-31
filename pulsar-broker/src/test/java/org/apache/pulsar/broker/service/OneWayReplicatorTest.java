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
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import io.netty.util.concurrent.FastThreadLocalThread;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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

    private void waitReplicatorStarted(String topicName) {
        Awaitility.await().untilAsserted(() -> {
            Optional<Topic> topicOptional2 = pulsar2.getBrokerService().getTopic(topicName, false).get();
            assertTrue(topicOptional2.isPresent());
            PersistentTopic persistentTopic2 = (PersistentTopic) topicOptional2.get();
            assertFalse(persistentTopic2.getProducers().isEmpty());
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

    @Test
    public void testReplicatorProducerStatInTopic() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://" + defaultNamespace + "/tp_");
        final String subscribeName = "subscribe_1";
        final byte[] msgValue = "test".getBytes();

        // Verify replicator works.
        Producer<byte[]> producer1 = client1.newProducer().topic(topicName).create();
        Consumer<byte[]> consumer2 = client2.newConsumer().topic(topicName).subscriptionName(subscribeName).subscribe();
        producer1.newMessage().value(msgValue).send();
        pulsar1.getBrokerService().checkReplicationPolicies();
        assertEquals(consumer2.receive(10, TimeUnit.SECONDS).getValue(), msgValue);

        // Verify there has one item in the attribute "publishers" or "replications"
        TopicStats topicStats2 = admin2.topics().getStats(topicName);
        assertTrue(topicStats2.getPublishers().size() + topicStats2.getReplication().size() > 0);

        // cleanup.
        consumer2.close();
        producer1.close();
        cleanupTopics(() -> {
            admin1.topics().delete(topicName);
            admin2.topics().delete(topicName);
        });
    }

    @Test
    public void testCreateRemoteConsumerFirst() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://" + defaultNamespace + "/tp_");
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

    @Test
    public void testTopicCloseWhenInternalProducerCloseErrorOnce() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://" + defaultNamespace + "/tp_");
        admin1.topics().createNonPartitionedTopic(topicName);
        // Wait for replicator started.
        waitReplicatorStarted(topicName);
        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar1.getBrokerService().getTopic(topicName, false).join().get();
        PersistentReplicator replicator =
                (PersistentReplicator) persistentTopic.getReplicators().values().iterator().next();
        // Mock an error when calling "replicator.disconnect()"
        ProducerImpl mockProducer = Mockito.mock(ProducerImpl.class);
        when(mockProducer.closeAsync()).thenReturn(CompletableFuture.failedFuture(new Exception("mocked ex")));
        ProducerImpl originalProducer = overrideProducerForReplicator(replicator, mockProducer);
        // Verify: since the "replicator.producer.closeAsync()" will retry after it failed, the topic unload should be
        // successful.
        admin1.topics().unload(topicName);
        // Verify: After "replicator.producer.closeAsync()" retry again, the "replicator.producer" will be closed
        // successful.
        overrideProducerForReplicator(replicator, originalProducer);
        Awaitility.await().untilAsserted(() -> {
            Assert.assertFalse(replicator.isConnected());
        });
        // cleanup.
        cleanupTopics(() -> {
            admin1.topics().delete(topicName);
            admin2.topics().delete(topicName);
        });
    }

    private void injectMockReplicatorProducerBuilder(
                                BiFunction<ProducerConfigurationData, ProducerImpl, ProducerImpl> producerDecorator)
            throws Exception {
        String cluster2 = pulsar2.getConfig().getClusterName();
        BrokerService brokerService = pulsar1.getBrokerService();
        // Wait for the internal client created.
        // the topic "__change_event" will trigger it created.
//        Awaitility.await().untilAsserted(() -> {
//            ConcurrentOpenHashMap<String, PulsarClient>
//                    replicationClients = WhiteboxImpl.getInternalState(brokerService, "replicationClients");
//            PulsarClientImpl internalClient = (PulsarClientImpl) replicationClients.get(cluster2);
//            assertNotNull(internalClient);
//        });
        final String topicNameTriggerInternalClientCreate =
                BrokerTestUtil.newUniqueName("persistent://" + defaultNamespace + "/tp_");
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
        replicationClients.put(cluster2, spyClient);

        // Inject producer decorator.
        doAnswer(invocation -> {
            Schema schema = (Schema) invocation.getArguments()[0];
            ProducerBuilderImpl producerBuilder = (ProducerBuilderImpl) internalClient.newProducer(schema);
            ProducerBuilder spyProducerBuilder = spy(producerBuilder);
            doAnswer(ignore -> {
                CompletableFuture<Producer> producerFuture = new CompletableFuture<>();
                final ProducerImpl p = (ProducerImpl) producerBuilder.create();
                new FastThreadLocalThread(() -> {
                    try {
                        ProducerImpl newProducer = producerDecorator.apply(producerBuilder.getConf(), p);
                        producerFuture.complete(newProducer);
                    } catch (Exception ex) {
                        producerFuture.completeExceptionally(ex);
                    }
                }).start();
                return producerFuture;
            }).when(spyProducerBuilder).createAsync();
            return spyProducerBuilder;
        }).when(spyClient).newProducer(any(Schema.class));
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
     */
    @Test
    public void testConcurrencyOfUnloadBundleAndRecreateProducer() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://" + defaultNamespace + "/tp_");
        // Inject an error for "replicator.producer" creation.
        // The delay time of next retry to create producer is below:
        //   0.1s, 0.2, 0.4, 0.8, 1.6s, 3.2s, 6.4s...
        //   If the retry counter is larger than 6, the next creation will be slow enough to close Replicator.
        final AtomicInteger createProducerCounter = new AtomicInteger();
        final int failTimes = 6;
        injectMockReplicatorProducerBuilder((producerCnf, orginalProducer) -> {
            if (topicName.equals(producerCnf.getTopicName())) {
                // There is a switch to determine create producer successfully or not.
                if (createProducerCounter.incrementAndGet() > failTimes) {
                    return orginalProducer;
                }
                log.info("Retry create replicator.producer count: {}", createProducerCounter);
                // Release producer and fail callback.
                orginalProducer.closeAsync();
                throw new RuntimeException("mock error");
            }
            return orginalProducer;
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
            assertTrue(createProducerCounter.get() >= failTimes);
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
        cleanupTopics(() -> {
            admin1.topics().delete(topicName);
            admin2.topics().delete(topicName);
        });
    }
}
