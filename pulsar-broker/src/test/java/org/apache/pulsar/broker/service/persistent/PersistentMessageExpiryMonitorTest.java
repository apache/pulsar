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
package org.apache.pulsar.broker.service.persistent;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.testng.AssertJUnit.assertEquals;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.Schema;
import org.awaitility.Awaitility;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
public class PersistentMessageExpiryMonitorTest extends ProducerConsumerBase {

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

    /***
     * Confirm the anti-concurrency mechanism "expirationCheckInProgressUpdater" works.
     */
    @Test
    void testConcurrentlyExpireMessages() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        final String cursorName = "s1";
        admin.topics().createNonPartitionedTopic(topicName);
        admin.topics().createSubscriptionAsync(topicName, cursorName, MessageId.earliest);
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicName).create();
        producer.send("1");
        producer.send("2");
        producer.send("3");
        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopic(topicName, false).join().get();
        ManagedLedgerImpl ml = (ManagedLedgerImpl) persistentTopic.getManagedLedger();
        ManagedCursorImpl cursor = (ManagedCursorImpl) ml.getCursors().get(cursorName);
        ManagedCursorImpl spyCursor = Mockito.spy(cursor);

        // Make the mark-deleting delay.
        CountDownLatch firstFindingCompleted = new CountDownLatch(1);
        AtomicInteger calledFindPositionCount = new AtomicInteger();
        doAnswer(invocationOnMock -> {
            firstFindingCompleted.countDown();
            ml.getExecutor().execute(() -> {
                try {
                    Thread.sleep(3000);
                    invocationOnMock.callRealMethod();
                } catch (Throwable ex) {
                    log.error("Unexpected exception when calling mark delete", ex);
                }
            });
            return true;
        }).when(spyCursor).asyncMarkDelete(any(Position.class), any(Map.class),
                any(AsyncCallbacks.MarkDeleteCallback.class), any());
        doAnswer(invocationOnMock -> {
            calledFindPositionCount.incrementAndGet();
            return invocationOnMock.callRealMethod();
        }).when(spyCursor).asyncFindNewestMatching(any(), any(), any(), any(), any(), any(), anyBoolean());
        doAnswer(invocationOnMock -> {
            calledFindPositionCount.incrementAndGet();
            return invocationOnMock.callRealMethod();
        }).when(spyCursor).asyncFindNewestMatching(any(), any(), any(), any(), anyBoolean());
        doAnswer(invocationOnMock -> {
            calledFindPositionCount.incrementAndGet();
            return invocationOnMock.callRealMethod();
        }).when(spyCursor).asyncFindNewestMatching(any(), any(), any(), any());

        // Sleep 2s to make "find(1s)" get a position.
        Thread.sleep(2000);

        // Start two expire tasks concurrently.
        PersistentMessageExpiryMonitor
                monitor = new PersistentMessageExpiryMonitor(persistentTopic, cursorName, spyCursor, null);
        CompletableFuture<Boolean> expireTask1 = new CompletableFuture<>();
        new Thread(() -> {
            expireTask1.complete(monitor.expireMessages(1));
        }).start();
        CompletableFuture<Boolean> expireTask2 = new CompletableFuture<>();
        new Thread(() -> {
            expireTask2.complete(monitor.expireMessages(1));
        }).start();
        firstFindingCompleted.await();
        CompletableFuture<Boolean> expireTask3 = new CompletableFuture<>();
        new Thread(() -> {
            expireTask3.complete(monitor.expireMessages(1));
        }).start();

        Awaitility.await().untilAsserted(() -> {
            Assert.assertEquals(monitor.getTotalMessageExpired(), 3);
        });
        // Verify: since the other 2 tasks have been prevented, the count of calling find position is 1.
        Thread.sleep(1000);
        assertEquals(1, calledFindPositionCount.get());

        // cleanup.
        producer.close();
        admin.topics().delete(topicName);
    }
}
