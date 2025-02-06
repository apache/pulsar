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
package org.apache.pulsar.broker.transaction.buffer.impl;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.TopicFactory;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentTopic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.transaction.buffer.AbortedTxnProcessor;
import org.apache.pulsar.broker.transaction.buffer.TransactionBufferProvider;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class TransactionPersistentTopicTest extends ProducerConsumerBase {

    private static CountDownLatch topicInitSuccessSignal = new CountDownLatch(1);

    @BeforeClass(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        // Intercept when the `topicFuture` is about to complete and wait until the topic close operation finishes.
        conf.setTopicFactoryClassName(MyTopicFactory.class.getName());
        conf.setTransactionCoordinatorEnabled(true);
        conf.setBrokerDeduplicationEnabled(false);
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testNoOrphanClosedTopicIfTxnInternalFailed() {
        String tpName = BrokerTestUtil.newUniqueName("persistent://public/default/tp2");

        BrokerService brokerService = pulsar.getBrokerService();

        // 1. Mock close topic when create transactionBuffer
        TransactionBufferProvider mockTransactionBufferProvider = originTopic -> {
            AbortedTxnProcessor abortedTxnProcessor = mock(AbortedTxnProcessor.class);
            doAnswer(invocation -> {
                topicInitSuccessSignal.await();
                return CompletableFuture.failedFuture(new RuntimeException("Mock recovery failed"));
            }).when(abortedTxnProcessor).recoverFromSnapshot();
            when(abortedTxnProcessor.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
            return new TopicTransactionBuffer(
                    (PersistentTopic) originTopic, abortedTxnProcessor);
        };
        TransactionBufferProvider originalTransactionBufferProvider = pulsar.getTransactionBufferProvider();
        pulsar.setTransactionBufferProvider(mockTransactionBufferProvider);

        // 2. Trigger create topic and assert topic load success.
        CompletableFuture<Optional<Topic>> firstLoad = brokerService.getTopic(tpName, true);
        Awaitility.await().ignoreExceptions().atMost(10, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    assertTrue(firstLoad.isDone());
                    assertFalse(firstLoad.isCompletedExceptionally());
                });

        // 3. Assert topic removed from cache
        Awaitility.await().ignoreExceptions().atMost(10, TimeUnit.SECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    assertFalse(brokerService.getTopics().containsKey(tpName));
                });

        // 4. Set txn provider to back
        pulsar.setTransactionBufferProvider(originalTransactionBufferProvider);
    }

    public static class MyTopicFactory implements TopicFactory {
        @Override
        public <T extends Topic> T create(String topic, ManagedLedger ledger, BrokerService brokerService,
                                          Class<T> topicClazz) {
            try {
                if (topicClazz == NonPersistentTopic.class) {
                    return (T) new NonPersistentTopic(topic, brokerService);
                } else {
                    return (T) new MyPersistentTopic(topic, ledger, brokerService);
                }
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public void close() throws IOException {
            // No-op
        }
    }

    public static class MyPersistentTopic extends PersistentTopic {

        public MyPersistentTopic(String topic, ManagedLedger ledger, BrokerService brokerService) {
            super(topic, ledger, brokerService);
        }

        @SneakyThrows
        @Override
        public CompletableFuture<Void> checkDeduplicationStatus() {
            topicInitSuccessSignal.countDown();
            // Sleep 1s pending txn buffer recover failed and close topic
            Thread.sleep(1000);
            return CompletableFuture.completedFuture(null);
        }
    }

}
