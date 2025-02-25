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
import static org.testng.Assert.assertNull;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.assertj.core.data.Percentage;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class PublishRateLimiterOverconsumingTest extends BrokerTestBase {
    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
    }

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    /**
     * This test verifies the broker publish rate limiting behavior with multiple concurrent publishers.
     * This reproduces the issue https://github.com/apache/pulsar/issues/23920 and prevents future regressions.
     */
    @Test
    public void testOverconsumingTokensWithBrokerPublishRateLimiter() throws Exception {
        int rateInMsg = 500;
        int durationSeconds = 5;
        int numberOfConsumers = 5;
        int numberOfProducersWithIndependentClients = 5;
        int numberOfMessagesForEachProducer = (rateInMsg * (durationSeconds + 1)) / 5;

        // configure dispatch throttling rate
        BrokerService brokerService = pulsar.getBrokerService();
        admin.brokers().updateDynamicConfiguration("brokerPublisherThrottlingMaxMessageRate", String.valueOf(rateInMsg));
        Awaitility.await().untilAsserted(() -> {
            PublishRateLimiterImpl publishRateLimiter =
                    (PublishRateLimiterImpl) brokerService.getBrokerPublishRateLimiter();
            assertEquals(publishRateLimiter.getTokenBucketOnMessage().getRate(), rateInMsg);
            assertNull(publishRateLimiter.getTokenBucketOnByte());
        });

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
                startTimeNanos.compareAndSet(0, System.nanoTime());
                startTime = startTimeNanos.get();
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
        ScheduledFuture<?> scheduledFuture = executor.scheduleAtFixedRate(rateTracker, 0, 500, TimeUnit.MILLISECONDS);

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
                                .messageListener(messageListener)
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

        // create independent clients for producers so that they don't get blocked by throttling
        List<PulsarClient> producerClients = IntStream.range(0, numberOfProducersWithIndependentClients)
                .mapToObj(i -> {
                    try {
                        return PulsarClient.builder()
                                .serviceUrl(pulsar.getBrokerServiceUrl())
                                .ioThreads(1)
                                .statsInterval(0, TimeUnit.SECONDS)
                                .connectionsPerBroker(1)
                                .build();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }).toList();
        @Cleanup
        AutoCloseable producerClientsCloser = () -> {
            producerClients.forEach(c -> {
                try {
                    c.close();
                } catch (Exception e) {
                    // ignore
                }
            });
        };

        List<Producer<Integer>> producers = IntStream.range(0, numberOfProducersWithIndependentClients)
                .mapToObj(i -> {
                    try {
                        return producerClients.get(i)
                                .newProducer(Schema.INT32).enableBatching(true)
                                .producerName("producer-" + (i + 1))
                                .batchingMaxPublishDelay(100, TimeUnit.MILLISECONDS)
                                .batchingMaxMessages(numberOfMessagesForEachProducer / 100)
                                .topic(topicName).create();
                    } catch (PulsarClientException e) {
                        throw new RuntimeException(e);
                    }
                }).toList();

        // send messages
        producers.forEach(producer -> {
            IntStream.range(0, numberOfMessagesForEachProducer).forEach(i -> {
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
            producer.flushAsync().whenComplete((r, e) -> {
                if (e != null) {
                    log.error("Failed to flush producer", e);
                } else {
                    log.info("Producer {} flushed", producer.getProducerName());
                }
            });
        });

        @Cleanup
        AutoCloseable producersClose = () -> {
            producers.forEach(p -> {
                try {
                    p.close();
                } catch (Exception e) {
                    // ignore
                }
            });
        };

        // wait for results
        Awaitility.await()
                .atMost(Duration.ofSeconds(durationSeconds * 2))
                .pollInterval(Duration.ofMillis(100))
                .untilAsserted(
                        () -> assertThat(collectedRatesForEachSecond).hasSizeGreaterThanOrEqualTo(durationSeconds));
        List<Integer> collectedRatesSnapshot = new ArrayList<>(collectedRatesForEachSecond);
        log.info("Collected rates for each second: {}", collectedRatesSnapshot);
        long avgMsgRate =
                totalMessagesReceived.get() / TimeUnit.NANOSECONDS.toSeconds(
                        lastReceivedMessageTimeNanos.get() - startTimeNanos.get());
        log.info("Average rate during the test run: {} msg/s", avgMsgRate);

        assertSoftly(softly -> {
            // check the rate during the test run
            softly.assertThat(avgMsgRate).describedAs("average rate during the test run")
                    // allow rate in 40% range
                    .isCloseTo(rateInMsg, Percentage.withPercentage(40));

            // check that rates were collected
            // skip the first element as it might contain messages for first 2 seconds
            softly.assertThat(collectedRatesSnapshot.subList(1, collectedRatesSnapshot.size() - 1))
                    .describedAs("actual rates for each second")
                    .allSatisfy(rates -> {
                        assertThat(rates).describedAs("actual rate for each second")
                                .isCloseTo(rateInMsg, Percentage.withPercentage(50));
                    });
        });
        scheduledFuture.cancel(true);
        producersClose.close();
        consumerCloser.close();
    }
}
