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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.testng.Assert.assertEquals;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.IntStream;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.service.persistent.DispatchRateLimiterFactoryAsyncTokenBucket;
import org.apache.pulsar.broker.service.persistent.DispatchRateLimiterFactoryClassic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.assertj.core.data.Percentage;
import org.awaitility.Awaitility;
import org.testng.ITest;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class DispatchRateLimiterOverconsumingTest extends BrokerTestBase implements ITest {
    public enum DispatchRateLimiterImplType {
        PIP322(DispatchRateLimiterFactoryAsyncTokenBucket.class.getName()),
        Classic(DispatchRateLimiterFactoryClassic.class.getName());

        private final String factoryClassName;

        DispatchRateLimiterImplType(String implementationClassName) {
            this.factoryClassName = implementationClassName;
        }

        public String getFactoryClassName() {
            return factoryClassName;
        }

        public static Object[] generateTestInstances(
                Function<DispatchRateLimiterImplType, Object> testInstanceFactory) {
            return Arrays.stream(values()).map(testInstanceFactory).toArray();
        }
    }

    // Comment out the next line (Factory annotation) to run tests manually in IntelliJ, one-by-one
    @Factory
    public static Object[] createTestInstances() {
        return DispatchRateLimiterImplType.generateTestInstances(DispatchRateLimiterOverconsumingTest::new);
    }

    public DispatchRateLimiterOverconsumingTest() {
        // set the default implementation type for manual running in IntelliJ
        this(DispatchRateLimiterImplType.Classic);
    }

    public DispatchRateLimiterOverconsumingTest(DispatchRateLimiterImplType implType) {
        this.implType = implType;
    }

    private DispatchRateLimiterImplType implType;
    private String testName;

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
    }

    @Override
    public String getTestName() {
        return testName;
    }

    @BeforeMethod
    public void applyTestName(Method method) {
        testName = method.getName() + " with " + implType.name() + " rate limiter implementation";
    }

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        // configure dispatch rate limiter factory class name
        conf.setDispatchRateLimiterFactoryClassName(implType.getFactoryClassName());
        // simplify testing by enabling throttling on non-backlog consumers
        conf.setDispatchThrottlingOnNonBacklogConsumerEnabled(true);
        // set the dispatch max read batch size to 1 to stress the rate limiting behavior more effectively
        conf.setDispatcherMaxReadBatchSize(1);
        // avoid dispatching messages in a separate thread to simplify testing and reduce variance
        conf.setDispatcherDispatchMessagesInSubscriptionThread(false);
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        this.testName = null;
        super.internalCleanup();
    }

    /**
     * This test verifies the broker dispatch rate limiting behavior with multiple concurrent consumers.
     * Reproduces issue "with a huge spike in a traffic consume is stuck for a long time" mentioned in
     * issue https://github.com/apache/pulsar/issues/24001 and prevents future regressions.
     */
    @Test
    public void testOverconsumingTokensWithBrokerDispatchRateLimiter() throws Exception {
        int rateInMsg = 50;
        int durationSeconds = 5;
        int numberOfConsumers = 20;
        int numberOfMessages = rateInMsg * durationSeconds;

        // configure dispatch throttling rate
        BrokerService brokerService = pulsar.getBrokerService();
        admin.brokers().updateDynamicConfiguration("dispatchThrottlingRateInMsg", String.valueOf(rateInMsg));
        Awaitility.await().untilAsserted(() ->
                assertEquals(brokerService.getBrokerDispatchRateLimiter()
                        .getAvailableDispatchRateLimitOnMsg(), rateInMsg));
        assertEquals(brokerService.getBrokerDispatchRateLimiter().getDispatchRateOnByte(), -1L);

        final String topicName = "persistent://" + newTopicName();

        // state for calculating message rate
        AtomicLong startTimeNanos = new AtomicLong();
        AtomicLong lastReceivedMessageTimeNanos = new AtomicLong();
        AtomicInteger totalMessagesReceived = new AtomicInteger();
        AtomicInteger currentSecondMessagesCount = new AtomicInteger();
        AtomicInteger lastCalculatedSecond = new AtomicInteger(0);
        List<Integer> collectedRatesForEachSecond = Collections.synchronizedList(new ArrayList<>());

        // track actual consuming rate of messages per second
        Runnable rateTracker = () -> {
            long startTime = startTimeNanos.get();
            if (startTime == 0) {
                return;
            }
            long durationNanos = System.nanoTime() - startTime;
            int elapsedFullSeconds = (int) (durationNanos / 1e9);
            if (elapsedFullSeconds > 0 && lastCalculatedSecond.compareAndSet(elapsedFullSeconds - 1,
                    elapsedFullSeconds)) {
                int messagesCountForPreviousSecond = currentSecondMessagesCount.getAndSet(0);
                log.info("Rate for second {}: {} msg/s {}", elapsedFullSeconds, messagesCountForPreviousSecond, TimeUnit.NANOSECONDS.toMillis(durationNanos));
                collectedRatesForEachSecond.add(messagesCountForPreviousSecond);
            }
        };
        @Cleanup("shutdownNow")
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        executor.scheduleAtFixedRate(rateTracker, 0, 500, TimeUnit.MILLISECONDS);

        // message listener implementation used for all consumers
        MessageListener<Integer> messageListener = new MessageListener<>() {
            @Override
            public void received(Consumer<Integer> consumer, Message<Integer> msg) {
                lastReceivedMessageTimeNanos.set(System.nanoTime());
                currentSecondMessagesCount.incrementAndGet();
                totalMessagesReceived.incrementAndGet();
                consumer.acknowledgeAsync(msg);
            }
        };

        // create consumers using a shared subscription called "sub"
        List<Consumer<Integer>> consumerList = IntStream.range(0, numberOfConsumers)
                .mapToObj(i -> {
                    try {
                        return pulsarClient.newConsumer(Schema.INT32)
                                .topic(topicName)
                                .consumerName("consumer-" + (i + 1))
                                .subscriptionType(SubscriptionType.Shared)
                                .subscriptionName("sub")
                                .receiverQueueSize(10)
                                .messageListener(messageListener)
                                // start paused so that there's a backlog when consumers are resumed
                                .startPaused(true)
                                .subscribe();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }).toList();
        // handle consumer cleanup when the test completes
        @Cleanup
        AutoCloseable consumerCloser = () -> {
            consumerList.forEach(c -> {
                try {
                    c.close();
                } catch (Exception e) {
                    // ignore
                }
            });
        };

        @Cleanup
        Producer<Integer> producer =
                pulsarClient.newProducer(Schema.INT32).enableBatching(false).topic(topicName).create();
        // send messages
        IntStream.range(0, numberOfMessages).forEach(i -> {
            try {
                int messageNumber = i + 1;
                producer.sendAsync(messageNumber).exceptionally(e -> {
                    log.error("Failed to send message #{}", messageNumber, e);
                    return null;
                });
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        log.info("Waiting for messages to be sent");
        // wait until the messages are sent
        producer.flush();

        // resume the consumers
        log.info("Resuming consumers");
        startTimeNanos.set(System.nanoTime());
        consumerList.forEach(Consumer::resume);

        // wait for results
        Awaitility.await()
                .atMost(Duration.ofSeconds(durationSeconds * 2))
                .pollInterval(Duration.ofMillis(100))
                .untilAsserted(() -> assertThat(totalMessagesReceived).hasValue(numberOfMessages));
        List<Integer> collectedRatesSnapshot = new ArrayList<>(collectedRatesForEachSecond);
        int messagesCountForPreviousSecond = currentSecondMessagesCount.getAndSet(0);
        if (messagesCountForPreviousSecond > 0) {
            collectedRatesSnapshot.add(messagesCountForPreviousSecond);
        }
        log.info("[{}] Collected rates for each second: {}", implType, collectedRatesSnapshot);
        long avgMsgRate =
                totalMessagesReceived.get() / TimeUnit.NANOSECONDS.toSeconds(
                        lastReceivedMessageTimeNanos.get() - startTimeNanos.get());
        log.info("[{}] Average rate during the test run: {} msg/s", implType, avgMsgRate);

        assertSoftly(softly -> {
            // check the rate during the test run
            softly.assertThat(avgMsgRate).describedAs("average rate during the test run")
                    // allow rate in 40% range
                    .isCloseTo(rateInMsg, Percentage.withPercentage(40));

            // check that rates were collected
            softly.assertThat(collectedRatesSnapshot).describedAs("actual rates for each second").size()
                    .isGreaterThanOrEqualTo(durationSeconds - 1).returnToIterable().satisfies(rates -> {
                        for (int i = 1; i < rates.size() - 1; i++) {
                            int rateAvg = (rates.get(i - 1) + rates.get(i)) / 2;
                            softly.assertThat(rateAvg).describedAs("Average of second %d and %d", i, i + 1)
                                    // TODO: relax the rate check by calculating an average of 2 subsequent
                                    //  seconds and allowing 55% range
                                    // This is due to the fact that the first second rate is usually 2x the
                                    // configured rate.
                                    // This problem can be handled separately.
                                    .isCloseTo(rateInMsg, Percentage.withPercentage(55));
                        }
                    });
        });
    }
}
