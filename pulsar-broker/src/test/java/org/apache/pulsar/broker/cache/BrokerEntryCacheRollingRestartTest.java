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
import io.netty.channel.EventLoopGroup;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.commons.io.FileUtils;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.qos.AsyncTokenBucket;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.InjectedClientCnxClientBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

/**
 * This test class current contains test cases that are exploratory in nature and are not intended to be run
 * as part of the regular test suite. This might change later.
 * The current intent is to show how the PIP-430 caching results in better cache hit rates in rolling restarts
 * when there's an active producer producing messages to a topic.
 * To avoid OOME, run this test on the command line (after enabling the test case) with this command:
 * ENABLE_MANUAL_TEST=true NETTY_LEAK_DETECTION=off mvn -pl pulsar-broker test -Dtest=BrokerEntryCacheRollingRestartTest
 */
@Slf4j
public class BrokerEntryCacheRollingRestartTest extends AbstractBrokerEntryCacheMultiBrokerTest {
    // comment out next line to run a single test run for the default cache type
    @Factory
    public static Object[] createTestInstances() {
        return Arrays.stream(BrokerEntryCacheType.values()).map(BrokerEntryCacheRollingRestartTest::new)
                .toArray();
    }

    volatile MessageId lastPublishedMessageId;
    volatile boolean restarting;

    public BrokerEntryCacheRollingRestartTest() {
        this(BrokerEntryCacheType.PIP430);
    }

    public BrokerEntryCacheRollingRestartTest(BrokerEntryCacheType cacheType) {
        super(cacheType);
    }

    @Override
    public void beforeMethod() {
        super.beforeMethod();
        lastPublishedMessageId = null;
        restarting = false;
    }

    @Override
    protected void logBKRead(long ledgerId, long firstEntry, long lastEntry, int numberOfEntries) {
        log.info(
                "BK read for ledgerId {}, firstEntry {}, lastEntry {}, numberOfEntries {}, "
                        + "last msgid {}, restarting={}",
                ledgerId, firstEntry, lastEntry, numberOfEntries, lastPublishedMessageId, restarting);
    }

    private static final int CACHE_SIZE_MB = 250;
    private static final int KB = 1024;
    private static final int MESSAGE_SIZE = 8 * KB; // 8 KB

    @Override
    protected int getManagedLedgerCacheSizeMB() {
        return CACHE_SIZE_MB;
    }

    @Override
    protected int calculateEntryLength(ManagedLedgerImpl ml, Entry entry) {
        // simulate message size for cache entry size calculation
        return MESSAGE_SIZE;
    }

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
        int producerRatePerSecond = 50000;
        int numberOfPartitions = 10;
        if (numberOfPartitions > 1) {
            admin.topics().createPartitionedTopic(topicName, numberOfPartitions);
        }

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
                    // use shorter ack group time to lose less acks in restarts
                    .acknowledgmentGroupTime(5, TimeUnit.MILLISECONDS)
                    .subscribe();
        }

        long endTimeMillis = System.currentTimeMillis() + testTimeInSeconds * 1000;

        CompletableFuture<Long> numberOfMessagesProducedFuture = new CompletableFuture<>();

        @Cleanup("interrupt")
        Thread producerThread = new Thread(() -> {
            long messageId = 0;
            log.info("Starting producer thread");
            AsyncTokenBucket producerTokenBucket = AsyncTokenBucket.builder()
                    .rate(producerRatePerSecond)
                    .build();
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    producerTokenBucket.consumeTokens(1);
                    producer.sendAsync(messageId++).thenApply(msgId -> {
                        lastPublishedMessageId = msgId;
                        return msgId;
                    });
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
                    // limit the rate by throttling message production if necessary
                    long throttlingDurationNanos = producerTokenBucket.calculateThrottlingDuration();
                    if (throttlingDurationNanos > 0) {
                        try {
                            Thread.sleep(TimeUnit.NANOSECONDS.toMillis(throttlingDurationNanos));
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
                    long startTimeMillis = System.currentTimeMillis();
                    log.info("Starting restarting main broker");
                    restartBroker();
                    log.info("Finished restarting main broker, took {} ms",
                            System.currentTimeMillis() - startTimeMillis);
                } catch (Exception e) {
                    log.error("Error while restarting broker ", e);
                }
            });
            for (int i = 0; i < numberOfAdditionalBrokers(); i++) {
                int brokerIndex = i;
                restartRunnables.add(() -> {
                    try {
                        long startTimeMillis = System.currentTimeMillis();
                        log.info("Starting restarting additional broker {}", brokerIndex);
                        restartAdditionalBroker(brokerIndex);
                        log.info("Finished restarting additional broker {}, took {} ms", brokerIndex,
                                System.currentTimeMillis() - startTimeMillis);
                    } catch (Exception e) {
                        log.error("Error while restarting additional broker {}", brokerIndex, e);
                    }
                });
            }
            long targetIntervalBetweenRestarts = testTimeInSeconds * 1000 / (numberOfRestarts + 1);
            log.info("Rolling restart injector will restart the brokers every {} ms", targetIntervalBetweenRestarts);
            long restartsStartTimeMillis = System.currentTimeMillis();
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    for (Runnable restartRunnable : restartRunnables) {
                        if (Thread.currentThread().isInterrupted()
                                || System.currentTimeMillis() >= endTimeMillis - targetIntervalBetweenRestarts) {
                            return;
                        }
                        long waitingTimeMillis =
                                targetIntervalBetweenRestarts - ((System.currentTimeMillis() - restartsStartTimeMillis)
                                        % targetIntervalBetweenRestarts);
                        log.info("Waiting for {} ms before restarting broker", waitingTimeMillis);
                        // Wait for some time before restarting the broker
                        Thread.sleep(waitingTimeMillis);
                        // Now restart the next broker
                        try {
                            restarting = true;
                            restartRunnable.run();
                        } finally {
                            restarting = false;
                        }
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

        List<Thread> consumerThreads = new ArrayList<>();

        for (int i = 0; i < numConsumers; i++) {
            final int consumerId = i;
            Consumer<Long> consumer = consumers[consumerId];
            Thread consumerThread = new Thread(() -> {
                try {
                    while (!Thread.currentThread().isInterrupted()) {
                        try {
                            Message<Long> message = consumer.receive(2000, TimeUnit.MILLISECONDS);
                            if (message == null) {
                                break;
                            }
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
            consumerThreads.add(consumerThread);
        }
        @Cleanup
        Closeable consumerThreadsCloseable = () -> {
            for (Thread consumerThread : consumerThreads) {
                consumerThread.interrupt();
            }
            for (Thread consumerThread : consumerThreads) {
                try {
                    consumerThread.join();
                } catch (InterruptedException e) {
                }
            }
            for (Consumer<Long> consumer : consumers) {
                consumer.close();
            }
        };

        long waitingTimeMillis = endTimeMillis - System.currentTimeMillis();
        if (waitingTimeMillis > 0) {
            log.info("Waiting for {} ms for test to complete.", waitingTimeMillis);
            Thread.sleep(waitingTimeMillis);
        }

        log.info("Stopping rolling restart injector.");
        rollingRestartInjector.interrupt();
        rollingRestartInjector.join();

        log.info("Stopping producer thread.");
        producerThread.interrupt();
        producerThread.join();

        log.info("Waiting up to 5 seconds for consumers to complete.");
        boolean allCompleted = consumersLatch.await(5, TimeUnit.SECONDS);
        log.info("All consumers completed: {} (latch count: {})", allCompleted, consumersLatch.getCount());

        log.info("Stopping consumers.");
        consumerThreadsCloseable.close();
        // Wait for all consumers to complete
        assertTrue(consumersLatch.await(2, TimeUnit.SECONDS),
                "All consumers should have been completed");

        logAndRecordResultsToCsv(testConfigDescriptionInResult, numberOfMessagesProducedFuture.getNow(0L),
                numberOfMessagesConsumed.get(), numConsumers, actualNumberOfRestarts.get());
    }

    private void logAndRecordResultsToCsv(String testConfigDescriptionInResult, long numberOfMessagesProduced,
                                          int numberOfMessagesConsumed, int numConsumers, int actualNumberOfRestarts)
            throws InterruptedException, ExecutionException, TimeoutException, IOException {
        int readEntryCount = bkReadEntryCount.get();
        double cacheHitPercentage = 100.0d * (numberOfMessagesConsumed - readEntryCount)
                / numberOfMessagesConsumed;
        double cacheMissPercentage = 100.0d - cacheHitPercentage;
        int readCount = bkReadCount.get();

        Object[] msgArgs = {testConfigDescriptionInResult, numberOfMessagesProduced,
                numberOfMessagesConsumed, numConsumers, readCount, readEntryCount,
                String.format("%.2f", cacheHitPercentage), String.format("%.2f", cacheMissPercentage),
                actualNumberOfRestarts};
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
        csvColumns.add(formatTimestampForCsv(ts));
        String resultMessage = csvColumns.stream().collect(Collectors.joining("\t")) + "\n";
        FileUtils.write(resultFile, resultMessage, StandardCharsets.UTF_8);
    }
}
