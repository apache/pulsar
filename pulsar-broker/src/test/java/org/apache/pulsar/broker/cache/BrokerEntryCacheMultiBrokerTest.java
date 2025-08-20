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
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import java.io.Closeable;
import java.io.File;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.PulsarMockBookKeeper;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.MultiBrokerTestZKBaseTest;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.InjectedClientCnxClientBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SizeUnit;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.util.netty.ChannelFutures;
import org.testng.SkipException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

/**
 * This test class current contains test cases that are exploratory in nature and are not intended to be run
 * as part of the regular test suite. This might change later.
 * The current intent is to show how the PIP-430 caching results in better cache hit rates in rolling restarts
 * when there's an active producer producing messages to a topic.
 * To avoid OOME, run this test on the command line (after enabling the test case) with this command:
 * ENABLE_MANUAL_TEST=true NETTY_LEAK_DETECTION=off mvn -pl pulsar-broker test -Dtest=BrokerEntryCacheMultiBrokerTest
 */
@Test(groups = "broker-api")
@Slf4j
public class BrokerEntryCacheMultiBrokerTest extends MultiBrokerTestZKBaseTest {
    public enum BrokerEntryCacheType {
        CacheEvictionByExpectedReadCount("PIP430", BrokerEntryCacheMultiBrokerTest::configurePIP430),
        CacheEvictionByExpectedReadCountDisabled("expectedReadCountDisabled",
                BrokerEntryCacheMultiBrokerTest::disableExpectedReadCount),
        CacheEvictionByMarkDeletedPosition("cacheEvictionByMarkDeletedPosition",
                BrokerEntryCacheMultiBrokerTest::configureCacheEvictionByMarkDeletedPosition),
        PR12258Caching("PR12258", BrokerEntryCacheMultiBrokerTest::configurePR12258Caching);

        private final String description;
        private final java.util.function.Consumer<ServiceConfiguration> configurer;

        BrokerEntryCacheType(String description, java.util.function.Consumer<ServiceConfiguration> configurer) {
            this.description = description;
            this.configurer = configurer;
        }

        public void configure(ServiceConfiguration config) {
            configurer.accept(config);
        }

        public String getDescription() {
            return description;
        }
    }

    AtomicInteger bkReadCount = new AtomicInteger(0);
    AtomicInteger bkReadEntryCount = new AtomicInteger(0);
    private static final DateTimeFormatter CSV_DATETIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    // comment out next line to run a single test run for the default cache type
    @Factory
    public static Object[] createTestInstances() {
        return Arrays.stream(BrokerEntryCacheType.values()).map(BrokerEntryCacheMultiBrokerTest::new)
                .toArray();
    }

    private final BrokerEntryCacheType cacheType;

    public BrokerEntryCacheMultiBrokerTest() {
        this(BrokerEntryCacheType.CacheEvictionByExpectedReadCount);
    }

    public BrokerEntryCacheMultiBrokerTest(BrokerEntryCacheType cacheType) {
        this.cacheType = cacheType;
    }

    @Override
    protected void beforeSetup() {
        if (!BooleanUtils.toBoolean(System.getenv("ENABLE_MANUAL_TEST")) && !isRunningInIntelliJ()) {
            throw new SkipException("This test requires setting ENABLE_MANUAL_TEST=true environment variable.");
        }
    }

    static boolean isRunningInIntelliJ() {
        // Check for IntelliJ-specific system properties
        return System.getProperty("idea.test.cyclic.buffer.size") != null
                || System.getProperty("java.class.path", "").contains("idea_rt.jar");
    }

    @Override
    protected void additionalSetup() throws Exception {
        super.additionalSetup();
        admin.tenants().createTenant("my-property",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace("my-property/my-ns");
        bkReadCount.set(0);
        bkReadEntryCount.set(0);
        PulsarMockBookKeeper mockBookKeeper = pulsarTestContext.getMockBookKeeper();
        mockBookKeeper.setReadHandleInterceptor(
                (long ledgerId, long firstEntry, long lastEntry, LedgerEntries entries) -> {
                    bkReadCount.incrementAndGet();
                    int numberOfEntries = (int) (lastEntry - firstEntry + 1);
                    bkReadEntryCount.addAndGet(numberOfEntries);
                    log.info("BK read for ledgerId {}, firstEntry {}, lastEntry {}, numberOfEntries {}",
                            ledgerId, firstEntry, lastEntry, numberOfEntries);
                    return CompletableFuture.completedFuture(entries);
                });
        // disable delays in bookkeeper writes and reads since they can skew the test results
        mockBookKeeper.setDefaultAddEntryDelayMillis(0L);
        mockBookKeeper.setDefaultReadEntriesDelayMillis(0L);
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
        bkReadEntryCount.set(0);
    }

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();

        // configure ledger rollover to avoid OOM in test runs and slowdowns due to GC happening frequently when
        // available heap memory is low
        conf.setManagedLedgerMinLedgerRolloverTimeMinutes(0);
        conf.setManagedLedgerMaxLedgerRolloverTimeMinutes(1);
        conf.setManagedLedgerMaxEntriesPerLedger(100000);

        // configure the cache type
        cacheType.configure(conf);
    }

    private static void configurePIP430(ServiceConfiguration defaultConf) {
        // use cache eviction by expected read count, this is the default behavior so it's not necessary to set it,
        // but it makes the test more explicit
        defaultConf.setCacheEvictionByExpectedReadCount(true);
        // LRU cache eviction behavior is enabled by default, but we set it explicitly
        defaultConf.setManagedLedgerCacheEvictionExtendTTLOfRecentlyAccessed(true);
    }

    private static void disableExpectedReadCount(ServiceConfiguration defaultConf) {
        defaultConf.setCacheEvictionByExpectedReadCount(false);
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
    @Test(invocationCount = 5)
    public void testTailingReadsRollingRestart() throws Exception {
        // this description is showed in result CSV files
        String testConfigDescriptionInResult = cacheType.getDescription();

        @Cleanup("shutdownGracefully")
        EventLoopGroup eventLoopGroup = InjectedClientCnxClientBuilder.createEventLoopGroup(2, false);

        @Cleanup
        PulsarClient producerPulsarClient = createPulsarClient(eventLoopGroup, () -> 0L);

        final String topicName =
                BrokerTestUtil.newUniqueName("persistent://my-property/my-ns/rolling-restart-cache-test-topic");
        final String subscriptionName = "sub";
        final int numConsumers = 10;
        final int receiverQueueSize = 200;
        int testTimeInSeconds = 30;
        int numberOfRestarts = 3;

        AtomicInteger messagesInFlight = new AtomicInteger();
        final int targetMessagesInFlight = 500000;
        AtomicInteger numberOfMessagesConsumed = new AtomicInteger(0);

        @Cleanup
        Producer<Long> producer = producerPulsarClient.newProducer(Schema.INT64)
                .topic(topicName)
                .enableBatching(false)
                .blockIfQueueFull(true)
                .create();

        // Create consumers in paused state with receiver queue size of 50
        Consumer<Long>[] consumers = new Consumer[numConsumers];
        List<PulsarClient> consumerPulsarClients = new ArrayList<>();
        @Cleanup
        Closeable consumerPulsarClientsCloseable = () -> {
            for (PulsarClient consumerPulsarClient : consumerPulsarClients) {
                consumerPulsarClient.close();
            }
        };

        // introduce 40-100ms delay in consumer connection to simulate network latency
        LongSupplier consumerClientConnectionDelaySupplier = () -> ThreadLocalRandom.current().nextLong(20L, 40L);
        for (int i = 0; i < numConsumers; i++) {
            PulsarClientImpl consumerPulsarClient =
                    createPulsarClient(eventLoopGroup, consumerClientConnectionDelaySupplier);
            consumerPulsarClients.add(consumerPulsarClient);
            consumers[i] = consumerPulsarClient.newConsumer(Schema.INT64)
                    .topic(topicName)
                    .subscriptionName(subscriptionName + "-" + i)
                    .subscriptionType(SubscriptionType.Shared)
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .startPaused(true) // start consumers in paused state
                    .receiverQueueSize(receiverQueueSize)
                    .poolMessages(true)
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

        AtomicInteger actualNumberOfRestarts = new AtomicInteger(0);

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
            long delayBetweenRestarts = testTimeInSeconds * 1000 / (numberOfRestarts + 1);
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    for (Runnable restartRunnable : restartRunnables) {
                        if (Thread.currentThread().isInterrupted()
                                || System.currentTimeMillis() >= endTimeMillis - delayBetweenRestarts / 2) {
                            return;
                        }
                        // Wait for some time before restarting the broker
                        Thread.sleep(delayBetweenRestarts);
                        // Now restart the next broker
                        restartRunnable.run();
                        actualNumberOfRestarts.incrementAndGet();
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
                            message.release();
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

        double cacheHitPercentage = 100.0d * (numberOfMessagesConsumed.get() - bkReadEntryCount.get())
                / numberOfMessagesConsumed.get();
        double cacheMissPercentage = 100.0d - cacheHitPercentage;

        Object[] msgArgs = {testConfigDescriptionInResult, numberOfMessagesProducedFuture.get(30, TimeUnit.SECONDS),
                numberOfMessagesConsumed.get(), numConsumers, bkReadCount.get(), bkReadEntryCount.get(),
                String.format("%.2f", cacheHitPercentage), String.format("%.2f", cacheMissPercentage),
                actualNumberOfRestarts.get()};
        log.info("[{}] Produced {} and Consumed {} messages (across {} consumers with unique subscriptions) in total. "
                        + "Number of BK reads {} with {} entries. Cache hits {}%, Cache misses {}%, Number of "
                        + "restarts {}", msgArgs);

        // record results in csv files for later processing with DuckDB, for example:
        // { cat pulsar-broker/target/rolling_restarts_result_header.txt; \
        //   cat pulsar-broker/target/rolling_restarts_result_*.csv } \
        //   | duckdb -c "select * from read_csv('/dev/stdin')" | cat
        File resultHeaderFile = new File("target/rolling_restarts_result_header.txt");
        String resultHeader =
                "description\tproduced\tconsumed\tconsumers\tbk_reads\tbk_read_entries\thits\tmisses\trestarts\tts\n";
        FileUtils.write(resultHeaderFile, resultHeader, StandardCharsets.UTF_8);
        long ts = System.currentTimeMillis();
        File resultFile = new File("target/rolling_restarts_result_" + ts + ".csv");
        List<String> csvColumns = new ArrayList<>();
        Arrays.stream(msgArgs).map(Object::toString).forEach(csvColumns::add);
        csvColumns.add(
                CSV_DATETIME_FORMATTER.format(ZonedDateTime.ofInstant(Instant.ofEpochMilli(ts), ZoneOffset.UTC)));
        String resultMessage = csvColumns.stream().collect(Collectors.joining("\t")) + "\n";
        FileUtils.write(resultFile, resultMessage, StandardCharsets.UTF_8);
    }

    private PulsarClientImpl createPulsarClient(EventLoopGroup eventLoopGroup, LongSupplier connectionDelaySupplier)
            throws Exception {
        return InjectedClientCnxClientBuilder.builder()
                .eventLoopGroup(eventLoopGroup)
                .clientBuilder(
                        PulsarClient.builder()
                                .serviceUrl(serviceUrlForFixedPorts())
                                .statsInterval(0, TimeUnit.SECONDS)
                                .memoryLimit(32, SizeUnit.MEGA_BYTES)
                )
                .connectToAddressFutureCustomizer((Channel channel, InetSocketAddress inetSocketAddress) -> {
                    // Introduce a delay to simulate network latency in connecting to the broker.
                    long delayMillis = connectionDelaySupplier.getAsLong();
                    if (delayMillis > 0) {
                        return CompletableFuture.supplyAsync(() -> null,
                                        CompletableFuture.delayedExecutor(delayMillis, TimeUnit.MILLISECONDS))
                                .thenCompose(
                                        __ -> ChannelFutures.toCompletableFuture(channel.connect(inetSocketAddress)));
                    } else {
                        return ChannelFutures.toCompletableFuture(channel.connect(inetSocketAddress));
                    }
                })
                .build();
    }
}
