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

import static org.testng.Assert.assertTrue;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.MultiBrokerTestZKBaseTest;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * This test class current contains test cases that are exploratory in nature and are not intended to be run
 * as part of the regular test suite. This might change later.
 * The current intent is to show how the PIP-430 caching results in better cache hit rates in rolling restarts
 * when there's an active producer producing messages to a topic.
 */
@Test(groups = "broker-api")
@Slf4j
public class BrokerEntryCacheMultiBrokerTest extends MultiBrokerTestZKBaseTest {
    AtomicInteger bkReadCount = new AtomicInteger(0);

    @Override
    protected void additionalSetup() throws Exception {
        super.additionalSetup();
        admin.tenants().createTenant("my-property",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace("my-property/my-ns");
        bkReadCount.set(0);
        pulsarTestContext.getMockBookKeeper()
                .setReadHandleInterceptor((long ledgerId, long firstEntry, long lastEntry, LedgerEntries entries) -> {
                    bkReadCount.incrementAndGet();
                    return CompletableFuture.completedFuture(entries);
                });
    }

    @Override
    protected int numberOfAdditionalBrokers() {
        // test with 2 brokers in the cluster
        return 1;
    }

    @Override
    protected boolean useDynamicBrokerPorts() {
        return false;
    }

    @BeforeMethod(alwaysRun = true)
    public void beforeMethod() {
        bkReadCount.set(0);
    }

    @Override
    protected ServiceConfiguration getDefaultConf() {
        ServiceConfiguration defaultConf = super.getDefaultConf();
        // use cache eviction by expected read count, this is the default behavior so it's not necessary to set it,
        // but it makes the test more explicit
        defaultConf.setCacheEvictionByExpectedReadCount(true);
        // LRU cache eviction behavior is enabled by default, but we set it explicitly
        defaultConf.setManagedLedgerCacheEvictionExtendTTLOfRecentlyAccessed(true);

        // uncomment one or many of these to compare with existing caching behavior
        //defaultConf.setManagedLedgerCacheEvictionExtendTTLOfRecentlyAccessed(false);
        //defaultConf.setCacheEvictionByExpectedReadCount(false);
        //configurePR12258Caching(defaultConf);
        //configureCacheEvictionByMarkDeletedPosition(defaultConf);

        return defaultConf;
    }

    /**
     * Configures ServiceConfiguration for cache eviction based on the slowest markDeletedPosition.
     * This method disables cache eviction by expected read count and enables eviction by markDeletedPosition.
     * Related to https://github.com/apache/pulsar/pull/14985 - "Evicting cache data by the slowest markDeletedPosition"
     *
     * @param defaultConf ServiceConfiguration instance to be configured
     */
    private static void configureCacheEvictionByMarkDeletedPosition(ServiceConfiguration defaultConf) {
        defaultConf.setCacheEvictionByExpectedReadCount(false);
        defaultConf.setCacheEvictionByMarkDeletedPosition(true);
    }

    /**
     * Configures ServiceConfiguration with settings to test PR12258 behavior for caching to drain backlog consumers.
     * This method sets configurations to enable caching for cursors with backlogged messages.
     * To make PR12258 effective, there's an additional change made in the broker codebase to
     * activate the cursor when a consumer connects, instead of waiting for the scheduled task to activate it.
     * Check org.apache.pulsar.broker.service.persistent.PersistentSubscription#addConsumerInternal method
     * for the change.
     * @param defaultConf ServiceConfiguration instance to be modified
     */
    private static void configurePR12258Caching(ServiceConfiguration defaultConf) {
        defaultConf.setCacheEvictionByExpectedReadCount(false);
        defaultConf.setManagedLedgerMinimumBacklogCursorsForCaching(1);
        defaultConf.setManagedLedgerMinimumBacklogEntriesForCaching(1);
        defaultConf.setManagedLedgerMaxBacklogBetweenCursorsForCaching(Integer.MAX_VALUE);
        defaultConf.setManagedLedgerCursorBackloggedThreshold(Long.MAX_VALUE);
    }

    String serviceUrlForFixedPorts() {
        return "pulsar://127.0.0.1:" + mainBrokerPort + "," + additionalBrokerPorts
                .stream()
                .map(port -> "127.0.0.1:" + port)
                .collect(Collectors.joining(",")) + "/";
    }

    // change enabled to true to run the test
    @Test(enabled = false)
    public void testTailingReadsRollingRestart() throws Exception {
        @Cleanup
        PulsarClient pulsarClient = PulsarClient.builder()
                .serviceUrl(serviceUrlForFixedPorts())
                .statsInterval(0, TimeUnit.SECONDS)
                .build();

        final String topicName =
                BrokerTestUtil.newUniqueName("persistent://my-property/my-ns/rolling-restart-cache-test-topic");
        final String subscriptionName = "sub";
        final int numConsumers = 10;
        final int receiverQueueSize = 50;
        int testTimeInSeconds = 30;
        int numberOfRestarts = 3;

        AtomicInteger messagesInFlight = new AtomicInteger();
        final int targetMessagesInFlight = 10000;
        AtomicInteger numberOfMessagesConsumed = new AtomicInteger(0);

        @Cleanup
        Producer<Long> producer = pulsarClient.newProducer(Schema.INT64)
                .topic(topicName)
                .enableBatching(false)
                .blockIfQueueFull(true)
                .create();

        // Create consumers in paused state with receiver queue size of 50
        Consumer<Long>[] consumers = new Consumer[numConsumers];
        for (int i = 0; i < numConsumers; i++) {
            consumers[i] = pulsarClient.newConsumer(Schema.INT64)
                    .topic(topicName)
                    .subscriptionName(subscriptionName + "-" + i)
                    .subscriptionType(SubscriptionType.Shared)
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .startPaused(true) // start consumers in paused state
                    .receiverQueueSize(receiverQueueSize)
                    .subscribe();
        }

        long endTimeMillis = System.currentTimeMillis() + testTimeInSeconds * 1000;

        CompletableFuture<Long> numberOfMessagesProducedFuture = new CompletableFuture<>();

        @Cleanup("interrupt")
        Thread producerThread = new Thread(() -> {
            long messageId = 0;
            log.info("Starting producer thread");
            try {
                while (!Thread.currentThread().isInterrupted() && System.currentTimeMillis() < endTimeMillis + 1000) {
                    producer.sendAsync(messageId++);
                    // each consumer has a unique subscription
                    messagesInFlight.addAndGet(numConsumers);
                    if (messagesInFlight.get() >= targetMessagesInFlight) {
                        try {
                            // start throttling message production
                            Thread.sleep(100L);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }
            } finally {
                log.info("Producer thread interrupted, stopping message production");
                numberOfMessagesProducedFuture.complete(messageId);
            }
        });

        producerThread.start();

        // Unpause all consumers
        for (Consumer<Long> consumer : consumers) {
            consumer.resume();
        }

        @Cleanup("interrupt")
        Thread rollingRestartInjector = new Thread(() -> {
            List<Runnable> restartRunnables = new ArrayList<>();
            restartRunnables.add(() -> {
                try {
                    log.info("Starting restarting main broker");
                    restartBroker();
                    log.info("Finished restarting main broker");
                } catch (Exception e) {
                    log.error("Error while restarting broker ", e);
                }
            });
            for (int i = 0; i < numberOfAdditionalBrokers(); i++) {
                int brokerIndex = i;
                restartRunnables.add(() -> {
                    try {
                        log.info("Starting restarting additional broker {}", brokerIndex);
                        restartAdditionalBroker(brokerIndex);
                        log.info("Finished restarting additional broker {}", brokerIndex);
                    } catch (Exception e) {
                        log.error("Error while restarting additional broker {}", brokerIndex, e);
                    }
                });
            }
            while (!Thread.currentThread().isInterrupted() && System.currentTimeMillis() < endTimeMillis) {
                try {
                    for (Runnable restartRunnable : restartRunnables) {
                        // Wait for some time before restarting the broker
                        Thread.sleep(testTimeInSeconds * 1000 / numberOfRestarts);
                        // Now restart the next broker
                        restartRunnable.run();
                    }
                } catch (InterruptedException e) {
                    log.info("rolling restart injector interrupted");
                    Thread.currentThread().interrupt();
                }
            }
        });
        rollingRestartInjector.start();

        // Start consumer threads to read the catch-up messages
        CountDownLatch consumersLatch = new CountDownLatch(numConsumers);

        for (int i = 0; i < numConsumers; i++) {
            final int consumerId = i;
            Consumer<Long> consumer = consumers[consumerId];
            Thread consumerThread = new Thread(() -> {
                try {
                    while (!Thread.currentThread().isInterrupted() && System.currentTimeMillis() < endTimeMillis) {
                        try {
                            Message<Long> message = consumer.receive();
                            consumer.acknowledge(message);
                            messagesInFlight.decrementAndGet();
                            numberOfMessagesConsumed.incrementAndGet();
                        } catch (PulsarClientException e) {
                            // Continue on errors
                        }
                    }
                } finally {
                    consumersLatch.countDown();
                }
            });
            consumerThread.start();
        }

        // Wait for all consumers to complete
        assertTrue(consumersLatch.await(testTimeInSeconds * 2, TimeUnit.SECONDS),
                "All consumers should have been completed");

        rollingRestartInjector.interrupt();
        rollingRestartInjector.join();

        producerThread.interrupt();
        producerThread.join();

        // Clean up consumers
        for (Consumer<Long> consumer : consumers) {
            consumer.close();
        }

        log.info("Produced {} and Consumed {} messages (across {} consumers with unique subscriptions) in total. "
                        + "Number of BK reads {}",
                numberOfMessagesProducedFuture.get(30, TimeUnit.SECONDS),
                numberOfMessagesConsumed.get(), numConsumers, bkReadCount.get());
    }
}
