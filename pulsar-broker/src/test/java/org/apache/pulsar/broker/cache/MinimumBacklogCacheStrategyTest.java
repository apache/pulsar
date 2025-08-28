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
package org.apache.pulsar.broker.cache;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.cache.PendingReadsManager;
import org.apache.bookkeeper.mledger.impl.cache.RangeEntryCacheImpl;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker-api")
@Slf4j
public class MinimumBacklogCacheStrategyTest extends ProducerConsumerBase {
    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        this.conf.setClusterName("test");
        internalSetup();
        producerBaseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        internalCleanup();
    }

    @Override
    protected ServiceConfiguration getDefaultConf() {
        ServiceConfiguration defaultConf = super.getDefaultConf();
        defaultConf.setManagedLedgerMinimumBacklogCursorsForCaching(2);
        defaultConf.setManagedLedgerMinimumBacklogEntriesForCaching(10);
        defaultConf.setManagedLedgerCacheEvictionTimeThresholdMillis(60 * 1000);
        defaultConf.setCacheEvictionByExpectedReadCount(false);
        return defaultConf;
    }

    /**
     * Validates that backlog consumers cache the reads and reused by other backlog consumers while draining the
     * backlog.
     *
     * @throws Exception
     */
    @Test
    public void testBacklogConsumerCacheReads() throws Exception {
        log.info("-- Starting {} test --", methodName);

        final long totalMessages = 200;
        final int receiverSize = 10;
        final String topicName = "cache-read";
        final String sub1 = "sub";
        int totalSub = 10;
        Consumer<byte[]>[] consumers = new Consumer[totalSub];

        for (int i = 0; i < totalSub; i++) {
            consumers[i] = pulsarClient.newConsumer().topic("persistent://my-property/my-ns/" + topicName)
                    .subscriptionName(sub1 + "-" + i).subscriptionType(SubscriptionType.Shared)
                    .receiverQueueSize(receiverSize).subscribe();
        }
        for (int i = 0; i < totalSub; i++) {
            consumers[i].close();
        }

        final String topic = "persistent://my-property/my-ns/" + topicName;
        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer().topic(topic);

        producerBuilder.enableBatching(false);
        @Cleanup
        Producer<byte[]> producer = producerBuilder.create();

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topic).get();
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) topicRef.getManagedLedger();
        Field cacheField = ManagedLedgerImpl.class.getDeclaredField("entryCache");
        cacheField.setAccessible(true);
        RangeEntryCacheImpl entryCache = spy((RangeEntryCacheImpl) cacheField.get(ledger));
        cacheField.set(ledger, entryCache);

        Field pendingReadsManagerField = RangeEntryCacheImpl.class.getDeclaredField("pendingReadsManager");
        pendingReadsManagerField.setAccessible(true);
        PendingReadsManager pendingReadsManager = (PendingReadsManager) pendingReadsManagerField.get(entryCache);
        Field cacheFieldInManager = PendingReadsManager.class.getDeclaredField("rangeEntryCache");
        cacheFieldInManager.setAccessible(true);
        cacheFieldInManager.set(pendingReadsManager, entryCache);

        // 2. Produce messages
        for (int i = 0; i < totalMessages; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }
        ledger.checkCursorsToCacheEntries();

        ledger.getCursors().forEach(cursor -> {
            assertTrue(((ManagedCursorImpl) cursor).isCacheReadEntry());
        });

        // 3. Consume messages
        CountDownLatch latch = new CountDownLatch((int) (totalSub * totalMessages));
        for (int i = 0; i < totalSub; i++) {
            consumers[i] = (Consumer<byte[]>) pulsarClient.newConsumer()
                    .topic("persistent://my-property/my-ns/" + topicName).subscriptionName(sub1 + "-" + i)
                    .subscriptionType(SubscriptionType.Shared).receiverQueueSize(receiverSize)
                    .messageListener((c, m) -> {
                        latch.countDown();
                        try {
                            c.acknowledge(m);
                        } catch (PulsarClientException e) {
                            fail("failed to ack message");
                        }
                    }).subscribe();
        }

        latch.await();

        // Verify: EntryCache has been invalidated
        verify(entryCache, atLeastOnce()).insert(any());

        for (int i = 0; i < totalSub; i++) {
            consumers[i].close();
        }
    }


}
