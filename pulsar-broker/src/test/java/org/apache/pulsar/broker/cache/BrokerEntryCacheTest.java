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

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryMXBean;
import org.apache.bookkeeper.mledger.impl.cache.RangeCacheTestUtil;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.Ipv4Proxy;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.KeySharedPolicy;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.util.FutureUtil;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Some end-to-end test for Broker cache.
 * The tests disabled by default are exploratory in nature and are not intended to be run as part of the
 * regular test suite.
 */
@Test(groups = "broker-api")
@Slf4j
public class BrokerEntryCacheTest extends ProducerConsumerBase {
    AtomicInteger bkReadCount = new AtomicInteger(0);
    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        this.conf.setClusterName("test");
        internalSetup();
        producerBaseSetup();
        bkReadCount.set(0);
        pulsarTestContext.getMockBookKeeper()
                .setReadHandleInterceptor((long ledgerId, long firstEntry, long lastEntry, LedgerEntries entries) -> {
                    bkReadCount.incrementAndGet();
                    return CompletableFuture.completedFuture(entries);
                });
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        internalCleanup();
    }

    @Override
    protected ServiceConfiguration getDefaultConf() {
        ServiceConfiguration defaultConf = super.getDefaultConf();
        // use cache eviction by expected read count, this is the default behavior so it's not necessary to set it,
        // but it makes the test more explicit
        defaultConf.setCacheEvictionByExpectedReadCount(true);
        // LRU cache eviction behavior is enabled by default, but we set it explicitly
        defaultConf.setManagedLedgerCacheEvictionExtendTTLOfRecentlyAccessed(true);

        // while performing exploratory testing,
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

    // change enabled to true to run the test
    @Test(enabled = false)
    public void testTailingReadsKeySharedSlowConsumer() throws Exception {
        final String topicName = "persistent://my-property/my-ns/cache-test-topic";
        final String subscriptionName = "test-subscription";
        final int numConsumers = 10;
        final int numSubscriptions = numConsumers / 2;
        final int messagesPerSecond = 300 / numSubscriptions;
        final int testDurationSeconds = 10;
        final int numberOfKeys = numConsumers * 10;
        final int totalMessages = messagesPerSecond * testDurationSeconds;
        final long sizePerEntry = 68L;
        final int slowConsumerMessageProbability = 5; // 5% chance
        final int maxPauseForSlowConsumerMessageInMs = 500; // 1-500 ms pause

        // limit the cache size to a relatively small size (about 0.6 seconds of messages)
        pulsar.getDefaultManagedLedgerFactory().getEntryCacheManager()
               .updateCacheSizeAndThreshold((long) (1.1d * messagesPerSecond) * sizePerEntry);

        @Cleanup
        Producer<Long> producer = pulsarClient.newProducer(Schema.INT64)
                .topic(topicName)
                .enableBatching(false)
                .blockIfQueueFull(true)
                .create();

        // Create consumers on the tail (reading from latest)
        Consumer<Long>[] consumers = new Consumer[numConsumers];
        for (int i = 0; i < numConsumers; i++) {
            consumers[i] = pulsarClient.newConsumer(Schema.INT64)
                    .topic(topicName)
                    .receiverQueueSize(10)
                    .subscriptionName(subscriptionName + "-" + (i % numSubscriptions))
                    .subscriptionType(SubscriptionType.Key_Shared)
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Latest)
                    .keySharedPolicy(KeySharedPolicy.autoSplitHashRange())
                    .subscribe();
        }

        ManagedLedgerFactoryMXBean cacheStats = pulsar.getDefaultManagedLedgerFactory().getCacheStats();

        // Record initial cache metrics
        long initialCacheHits = cacheStats.getCacheHitsTotal();
        long initialCacheMisses = cacheStats.getCacheMissesTotal();

        // Start producer thread
        CountDownLatch producerLatch = new CountDownLatch(1);
        Thread producerThread = new Thread(() -> {
            try {
                long startTime = System.currentTimeMillis();
                int messagesSent = 0;
                long messageId = 0;
                while (messagesSent < totalMessages) {
                    long expectedTime = startTime + (messagesSent * 1000L / messagesPerSecond);
                    long currentTime = System.currentTimeMillis();

                    if (currentTime < expectedTime) {
                        Thread.sleep(expectedTime - currentTime);
                    }
                    long value = messageId++;
                    long keyValue = value % numberOfKeys;
                    byte[] keyBytes = new byte[Long.BYTES];
                    ByteBuffer keyBuffer = ByteBuffer.wrap(keyBytes);
                    keyBuffer.putLong(keyValue);
                    producer.newMessage().keyBytes(keyBytes).value(value).send();
                    messagesSent++;
                }
                log.info("Producer finished sending {} messages", messagesSent);
            } catch (Exception e) {
                log.error("Producer error", e);
                fail("Producer failed: " + e.getMessage());
            } finally {
                producerLatch.countDown();
            }
        });

        // Start consumer threads
        CountDownLatch consumersLatch = new CountDownLatch(numConsumers);
        for (int i = 0; i < numConsumers; i++) {
            final int consumerId = i;
            Thread consumerThread = new Thread(() -> {
                Random random = new Random();
                try {
                    int messagesReceived = 0;
                    long startTime = System.currentTimeMillis();

                    while (System.currentTimeMillis() - startTime < (testDurationSeconds + 2) * 1000) {
                        try {
                            Message<Long> message =
                                    consumers[consumerId].receive(1000, TimeUnit.MILLISECONDS);
                            if (message != null) {
                                // sleep for a random time with small probability to simulate slow consumer
                                if (random.nextInt(100) < slowConsumerMessageProbability) {
                                    try {
                                        // Simulate slow consumer by sleeping for a random time
                                        Thread.sleep(random.nextInt(maxPauseForSlowConsumerMessageInMs) + 1);
                                    } catch (InterruptedException e) {
                                        Thread.currentThread().interrupt();
                                    }
                                }
                                consumers[consumerId].acknowledge(message);
                                messagesReceived++;
                            }
                        } catch (PulsarClientException.TimeoutException e) {
                            // Expected timeout, continue
                        }
                    }
                    log.info("Consumer {} received {} messages", consumerId, messagesReceived);
                } catch (Exception e) {
                    log.error("Consumer {} error", consumerId, e);
                } finally {
                    consumersLatch.countDown();
                }
            });
            consumerThread.start();
        }

        // Start producer
        producerThread.start();

        // Wait for test completion
        assertTrue(producerLatch.await(testDurationSeconds + 5, TimeUnit.SECONDS),
                "Producer should complete within timeout");
        assertTrue(consumersLatch.await(testDurationSeconds + 10, TimeUnit.SECONDS),
                "Consumers should complete within timeout");

        // Clean up consumers
        for (Consumer<Long> consumer : consumers) {
            consumer.close();
        }

        // Get final cache metrics
        long finalCacheHits = cacheStats.getCacheHitsTotal();
        long finalCacheMisses = cacheStats.getCacheMissesTotal();

        // Calculate metrics similar to testStorageReadCacheMissesRate
        long cacheHitsDelta = finalCacheHits - initialCacheHits;
        long cacheMissesDelta = finalCacheMisses - initialCacheMisses;

        log.info("Cache metrics - Hits: {} -> {} (delta: {}), Misses: {} -> {} (delta: {})",
                initialCacheHits, finalCacheHits, cacheHitsDelta,
                initialCacheMisses, finalCacheMisses, cacheMissesDelta);
        log.info("Bk read count: {}", bkReadCount.get());

        // Verify that cache activity occurred
        assertTrue(cacheHitsDelta + cacheMissesDelta > 0,
                "Expected cache activity (hits or misses) during the test");

        // Verify metrics make sense for the workload
        assertTrue(cacheHitsDelta >= 0, "Cache hits should not decrease");
        assertTrue(cacheMissesDelta >= 0, "Cache misses should not decrease");

        // With multiple consumers reading from the tail, we expect some cache activity
        // The exact ratio depends on cache size and message patterns
        double totalCacheRequests = cacheHitsDelta + cacheMissesDelta;
        if (totalCacheRequests > 0) {
            double cacheHitRate = cacheHitsDelta / totalCacheRequests;
            log.info("Cache hit rate: {}%", String.format("%.2f", cacheHitRate * 100));

            // With tail consumers, we might expect good cache hit rates
            // since recent messages are more likely to be cached
            assertTrue(cacheHitRate >= 0.0 && cacheHitRate <= 1.0,
                    "Cache hit rate should be between 0 and 1");
        }
    }

    // change enabled to true to run the test
    @Test(enabled = false)
    public void testCatchUpReadsWithFailureProxyDisconnectingAllConnections() throws Exception {
        final String topicName = "persistent://my-property/my-ns/cache-catchup-test-topic";
        final String subscriptionName = "test-catchup-subscription";
        final int numConsumers = 5;
        final int totalMessages = 1000;
        final int receiverQueueSize = 50;


        // Wire a failure proxy so that it's possible to disconnect broker connections forcefully
        @Cleanup("stop")
        Ipv4Proxy failureProxy = new Ipv4Proxy(0, "localhost", pulsar.getBrokerListenPort().get());
        failureProxy.startup();
        @Cleanup("stop")
        PulsarLookupProxy lookupProxy = new PulsarLookupProxy(0, pulsar.getWebService().getListenPortHTTP().get(),
                pulsar.getBrokerListenPort().get(), failureProxy.getLocalPort());

        @Cleanup
        PulsarClient pulsarClient = PulsarClient.builder()
                .serviceUrl("http://localhost:" + lookupProxy.getBindPort())
                .statsInterval(0, TimeUnit.SECONDS)
                .build();

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

        ManagedLedgerFactoryMXBean cacheStats = pulsar.getDefaultManagedLedgerFactory().getCacheStats();

        // Record initial cache metrics
        long initialCacheHits = cacheStats.getCacheHitsTotal();
        long initialCacheMisses = cacheStats.getCacheMissesTotal();

        // Produce all messages while consumers are paused
        log.info("Starting to produce {} messages", totalMessages);
        for (long messageId = 0; messageId < totalMessages; messageId++) {
            producer.send(messageId);
        }
        log.info("Finished producing {} messages", totalMessages);

        // Record cache metrics after production
        long afterProductionCacheHits = cacheStats.getCacheHitsTotal();
        long afterProductionCacheMisses = cacheStats.getCacheMissesTotal();

        // Unpause all consumers
        for (Consumer<Long> consumer : consumers) {
            consumer.resume();
        }

        @Cleanup("interrupt")
        Thread failureInjector = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    // Simulate a failure by disconnecting the broker connections
                    Thread.sleep(2000); // Wait for some messages to be consumed
                    failureProxy.disconnectFrontChannels();
                    log.info("Injected failure by disconnecting all broker connections");
                } catch (InterruptedException e) {
                    log.info("Failure injector interrupted");
                    Thread.currentThread().interrupt();
                }
            }
        });
        failureInjector.start();

        // Start consumer threads to read the catch-up messages
        CountDownLatch consumersLatch = new CountDownLatch(numConsumers);
        int[] messagesReceivedPerConsumer = new int[numConsumers];

        for (int i = 0; i < numConsumers; i++) {
            final int consumerId = i;
            Thread consumerThread = new Thread(() -> {
                try {
                    long startTime = System.currentTimeMillis();
                    ConcurrentHashMap<Long, Long> messagesReceived = new ConcurrentHashMap<>();

                    // Give consumers enough time to catch up
                    while (messagesReceived.size() < totalMessages && System.currentTimeMillis() - startTime < 30000) {
                        try {
                            Message<Long> message = consumers[consumerId].receive(1000, TimeUnit.MILLISECONDS);
                            Thread.sleep(20); // Simulate processing time
                            if (message != null) {
                                long messageId = message.getValue();
                                messagesReceived.put(messageId, messageId);
                                consumers[consumerId].acknowledge(message);
                            }
                        } catch (PulsarClientException.TimeoutException e) {
                            // Continue on timeout
                        }
                    }
                    messagesReceivedPerConsumer[consumerId] = messagesReceived.size();
                    log.info("Consumer {} received {} messages", consumerId, messagesReceived.size());
                } catch (Exception e) {
                    log.error("Consumer {} error", consumerId, e);
                } finally {
                    consumersLatch.countDown();
                }
            });
            consumerThread.start();
        }

        // Wait for all consumers to complete
        assertTrue(consumersLatch.await(60, TimeUnit.SECONDS),
                "All consumers should complete catch-up reads within timeout");

        failureInjector.interrupt();
        failureInjector.join();

        // Clean up consumers
        for (Consumer<Long> consumer : consumers) {
            consumer.close();
        }

        // Get final cache metrics
        long finalCacheHits = cacheStats.getCacheHitsTotal();
        long finalCacheMisses = cacheStats.getCacheMissesTotal();

        // Calculate metrics
        long productionCacheHitsDelta = afterProductionCacheHits - initialCacheHits;
        long productionCacheMissesDelta = afterProductionCacheMisses - initialCacheMisses;
        long consumptionCacheHitsDelta = finalCacheHits - afterProductionCacheHits;
        long consumptionCacheMissesDelta = finalCacheMisses - afterProductionCacheMisses;

        log.info("Production phase - Cache hits delta: {}, Cache misses delta: {}",
                productionCacheHitsDelta, productionCacheMissesDelta);
        log.info("Consumption phase - Cache hits delta: {}, Cache misses delta: {}",
                consumptionCacheHitsDelta, consumptionCacheMissesDelta);
        log.info("Bk read count: {}", bkReadCount.get());

        // Verify all consumers received all messages
        for (int i = 0; i < numConsumers; i++) {
            assertTrue(messagesReceivedPerConsumer[i] == totalMessages,
                    String.format("Consumer %d should receive all %d messages, but received %d",
                            i, totalMessages, messagesReceivedPerConsumer[i]));
        }

        // Verify cache activity occurred during consumption
        assertTrue(consumptionCacheHitsDelta + consumptionCacheMissesDelta > 0,
                "Expected cache activity during catch-up reads");

        // For catch-up reads, we expect minimal cache misses since messages should be cached
        // or efficiently retrieved in sequence
        double totalConsumptionCacheRequests = consumptionCacheHitsDelta + consumptionCacheMissesDelta;
        if (totalConsumptionCacheRequests > 0) {
            double cacheHitRate = consumptionCacheHitsDelta / totalConsumptionCacheRequests;
            log.info("Consumption cache hit rate: {}%", String.format("%.2f", cacheHitRate * 100));

            // For catch-up scenarios, we expect very few cache misses
            assertTrue(consumptionCacheMissesDelta == 0 || cacheHitRate > 0.6,
                    String.format("Expected no cache misses or very high hit rate for catch-up reads. "
                                    + "Cache misses: %d, Hit rate: %.2f%%",
                            consumptionCacheMissesDelta, cacheHitRate * 100));
        }

        log.info("Catch-up read test completed successfully with {} consumers and {} messages",
                numConsumers, totalMessages);
    }

    @Test
    public void testTailingReadsClearsCacheAfterCacheTimeout() throws Exception {
        final String topicName = "persistent://my-property/my-ns/cache-test-topic";
        final String subscriptionName = "test-subscription";
        final int numConsumers = 5;
        final int messagesPerSecond = 150;
        final int testDurationSeconds = 3;
        final int totalMessages = messagesPerSecond * testDurationSeconds;

        @Cleanup
        Producer<Long> producer = pulsarClient.newProducer(Schema.INT64)
                .topic(topicName)
                .enableBatching(false)
                .blockIfQueueFull(true)
                .create();

        // Create consumers on the tail (reading from latest)
        Consumer<Long>[] consumers = new Consumer[numConsumers];
        for (int i = 0; i < numConsumers; i++) {
            consumers[i] = pulsarClient.newConsumer(Schema.INT64)
                    .topic(topicName)
                    .subscriptionName(subscriptionName + "-" + i)
                    .subscriptionType(SubscriptionType.Exclusive)
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Latest)
                    .subscribe();
        }

        // Start producer thread
        CountDownLatch producerLatch = new CountDownLatch(1);
        Thread producerThread = new Thread(() -> {
            try {
                long startTime = System.currentTimeMillis();
                int messagesSent = 0;
                long messageId = 0;
                while (messagesSent < totalMessages) {
                    long expectedTime = startTime + (messagesSent * 1000L / messagesPerSecond);
                    long currentTime = System.currentTimeMillis();

                    if (currentTime < expectedTime) {
                        Thread.sleep(expectedTime - currentTime);
                    }

                    producer.send(messageId++);
                    messagesSent++;
                }
                log.info("Producer finished sending {} messages", messagesSent);
            } catch (Exception e) {
                log.error("Producer error", e);
                fail("Producer failed: " + e.getMessage());
            } finally {
                producerLatch.countDown();
            }
        });

        // Start consumer threads
        CountDownLatch consumersLatch = new CountDownLatch(numConsumers);
        for (int i = 0; i < numConsumers; i++) {
            final int consumerId = i;
            Thread consumerThread = new Thread(() -> {
                try {
                    int messagesReceived = 0;
                    while (!Thread.currentThread().isInterrupted() && messagesReceived < totalMessages) {
                        try {
                            Message<Long> message =
                                    consumers[consumerId].receive(1000, TimeUnit.MILLISECONDS);
                            if (message != null) {
                                consumers[consumerId].acknowledge(message);
                                messagesReceived++;
                            }
                        } catch (PulsarClientException.TimeoutException e) {
                            // Expected timeout, continue
                        }
                    }
                    log.info("Consumer {} received {} messages", consumerId, messagesReceived);
                } catch (Exception e) {
                    log.error("Consumer {} error", consumerId, e);
                } finally {
                    consumersLatch.countDown();
                }
            });
            consumerThread.start();
        }

        // Start producer
        producerThread.start();

        // Wait for test completion
        assertTrue(producerLatch.await(testDurationSeconds + 5, TimeUnit.SECONDS),
                "Producer should complete within timeout");
        assertTrue(consumersLatch.await(testDurationSeconds + 10, TimeUnit.SECONDS),
                "Consumers should complete within timeout");

        RangeCacheTestUtil.forEachCachedEntry(pulsar, entry -> {
            if (entry.hasExpectedReads()) {
                assertThat(entry.getReadCountHandler().getExpectedReadCount())
                        .isEqualTo(0)
                        .describedAs("Expected read count for entry " + entry.getPosition() + " is not zero");
            }
        });

        // sleep for 3 * cache eviction time threshold to count for TTL
        // and managedLedgerCacheEvictionExtendTTLOfRecentlyAccessed behavior
        Thread.sleep(3 * conf.getManagedLedgerCacheEvictionTimeThresholdMillis());
        assertThat(pulsar.getDefaultManagedLedgerFactory().getEntryCacheManager().getSize()).isEqualTo(0L);

        // Clean up consumers
        for (Consumer<Long> consumer : consumers) {
            consumer.close();
        }
    }

    @Test
    public void testExpectedReads() throws Exception {
        final String topicName = "persistent://my-property/my-ns/cache-test-topic";
        final String subscriptionName = "test-subscription";
        final int numConsumers = 5;
        final int messagesPerSecond = 150;
        final int testDurationSeconds = 3;
        final int totalMessages = messagesPerSecond * testDurationSeconds;

        @Cleanup
        Producer<Long> producer = pulsarClient.newProducer(Schema.INT64)
                .topic(topicName)
                .enableBatching(false)
                .blockIfQueueFull(true)
                .create();

        // Create consumers on the tail (reading from latest)
        Consumer<Long>[] consumers = new Consumer[numConsumers];
        for (int i = 0; i < numConsumers; i++) {
            consumers[i] = pulsarClient.newConsumer(Schema.INT64)
                    .topic(topicName)
                    .subscriptionName(subscriptionName + "-" + i)
                    .subscriptionType(SubscriptionType.Exclusive)
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Latest)
                    .receiverQueueSize(messagesPerSecond)
                    .subscribe();
        }

        // Start producer thread
        CountDownLatch producerLatch = new CountDownLatch(1);
        Thread producerThread = new Thread(() -> {
            try {
                long startTime = System.currentTimeMillis();
                int messagesSent = 0;
                long messageId = 0;
                while (messagesSent < totalMessages) {
                    long expectedTime = startTime + (messagesSent * 1000L / messagesPerSecond);
                    long currentTime = System.currentTimeMillis();

                    if (currentTime < expectedTime) {
                        Thread.sleep(expectedTime - currentTime);
                    }

                    producer.send(messageId++);
                    messagesSent++;
                }
                log.info("Producer finished sending {} messages", messagesSent);
            } catch (Exception e) {
                log.error("Producer error", e);
                fail("Producer failed: " + e.getMessage());
            } finally {
                producerLatch.countDown();
            }
        });

        // Start consumer threads
        CountDownLatch consumersLatch = new CountDownLatch(numConsumers - 1);
        Thread lastConsumerThread = null;
        for (int i = 0; i < numConsumers; i++) {
            final int consumerId = i;
            Thread consumerThread = new Thread(() -> {
                try {
                    int messagesReceived = 0;
                    while (!Thread.currentThread().isInterrupted() && messagesReceived < totalMessages) {
                        try {
                            Message<Long> message =
                                    consumers[consumerId].receive(1000, TimeUnit.MILLISECONDS);
                            if (message != null) {
                                consumers[consumerId].acknowledge(message);
                                messagesReceived++;
                            }
                        } catch (PulsarClientException.TimeoutException e) {
                            // Expected timeout, continue
                        }
                    }
                    log.info("Consumer {} received {} messages", consumerId, messagesReceived);
                } catch (Exception e) {
                    log.error("Consumer {} error", consumerId, e);
                } finally {
                    consumersLatch.countDown();
                }
            });
            if (i != numConsumers - 1) {
                consumerThread.start();
            } else {
                lastConsumerThread = consumerThread;
            }
        }

        // Start producer
        producerThread.start();

        // Wait for test completion
        assertTrue(producerLatch.await(testDurationSeconds + 5, TimeUnit.SECONDS),
                "Producer should complete within timeout");
        assertTrue(consumersLatch.await(testDurationSeconds + 10, TimeUnit.SECONDS),
                "Consumers should complete within timeout");

        MutableInt expectedReadCountWith1 = new MutableInt(0);
        RangeCacheTestUtil.forEachCachedEntry(pulsar, entry -> {
            if (entry.hasExpectedReads()) {
                assertThat(entry.getReadCountHandler().getExpectedReadCount())
                        .isEqualTo(1)
                        .describedAs("Expected read count for entry " + entry.getPosition() + " is not 1");
                expectedReadCountWith1.increment();
            }
        });
        assertThat(expectedReadCountWith1.intValue()).isGreaterThan(0);

        lastConsumerThread.start();
        lastConsumerThread.join();

        // sleep for 3 * cache eviction time threshold to count for TTL
        // and managedLedgerCacheEvictionExtendTTLOfRecentlyAccessed behavior
        Thread.sleep(3 * conf.getManagedLedgerCacheEvictionTimeThresholdMillis());
        // now the cache should be empty
        assertThat(pulsar.getDefaultManagedLedgerFactory().getEntryCacheManager().getSize()).isEqualTo(0L);

        // Clean up consumers
        for (Consumer<Long> consumer : consumers) {
            consumer.close();
        }
    }

    // Test case for https://github.com/apache/pulsar/issues/16421
    @Test
    public void testConsumerFlowOnSharedSubscriptionIssue16421() throws Exception {
        String topic = BrokerTestUtil.newUniqueName("persistent://my-property/my-ns/topic");
        admin.topics().createNonPartitionedTopic(topic);
        String subName = "my-sub";
        int numMessages = 20_000;
        final CountDownLatch count = new CountDownLatch(numMessages);
        try (Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .subscriptionType(SubscriptionType.Shared)
                .topic(topic)
                .subscriptionName(subName)
                .messageListener(new MessageListener<byte[]>() {
                    @Override
                    public void received(Consumer<byte[]> consumer, Message<byte[]> msg) {
                        //log.info("received {} - {}", msg, count.getCount());
                        consumer.acknowledgeAsync(msg);
                        count.countDown();
                    }
                })
                .subscribe();
             Producer<byte[]> producer = pulsarClient
                     .newProducer()
                     .blockIfQueueFull(true)
                     .enableBatching(true)
                     .topic(topic)
                     .create()) {
            consumer.pause();
            byte[] message = "foo".getBytes(StandardCharsets.UTF_8);
            List<CompletableFuture<?>> futures = new ArrayList<>();
            for (int i = 0; i < numMessages; i++) {
                futures.add(producer.sendAsync(message).whenComplete((id, e) -> {
                    if (e != null) {
                        log.error("error", e);
                    }
                }));
                if (futures.size() == 1000) {
                    FutureUtil.waitForAll(futures).get();
                    futures.clear();
                }
            }
            producer.flush();
            consumer.resume();
            assertTrue(count.await(20, TimeUnit.SECONDS));
        }
        // no BookKeeper reads should occur in this use case
        assertThat(bkReadCount.get()).isEqualTo(0);
    }
}
