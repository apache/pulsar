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
import static org.testng.Assert.assertEquals;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
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
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.assertj.core.data.Percentage;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class DispatchRateLimiterOverconsumingTest extends BrokerTestBase {
    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
    }

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        // simplify testing by enabling throttling on non-backlog consumers
        conf.setDispatchThrottlingOnNonBacklogConsumerEnabled(true);
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    /**
     * This test verifies the broker dispatch rate limiting behavior with multiple concurrent consumers.
     */
    @Test
    public void testOverconsumingTokensWithBrokerDispatchRateLimiter() throws Exception {
        int rateInMsg = 50;
        int numberOfMessages = 2000;
        int numberOfConsumers = 10;
        int durationSeconds = 5;

        // configure dispatch throttling rate to 10 msg/s
        BrokerService brokerService = pulsar.getBrokerService();
        admin.brokers().updateDynamicConfiguration("dispatchThrottlingRateInMsg", String.valueOf(rateInMsg));
        Awaitility.await().untilAsserted(() ->
                assertEquals(brokerService.getBrokerDispatchRateLimiter()
                        .getAvailableDispatchRateLimitOnMsg(), rateInMsg));
        assertEquals(brokerService.getBrokerDispatchRateLimiter().getDispatchRateOnByte(), -1L);

        final String topicName = "persistent://" + newTopicName();

        // state for calculating message rate
        AtomicLong startTimeNanos = new AtomicLong();
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
                log.info("Rate for second {}: {} msg/s", elapsedFullSeconds, messagesCountForPreviousSecond);
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
                currentSecondMessagesCount.incrementAndGet();
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
                .pollInterval(Duration.ofMillis(100))
                .untilAsserted(() -> assertThat(lastCalculatedSecond).hasValue(durationSeconds));

        List<Integer> collectedRatesSnapshot = new ArrayList<>(collectedRatesForEachSecond);

        // check that rates were collected
        assertThat(collectedRatesSnapshot).size().isEqualTo(durationSeconds);

        // check the rate during the test run
        assertThat(collectedRatesSnapshot)
                .describedAs("actual rates for each second")
                // allow rate in 10% range
                .allSatisfy(rate -> assertThat(rate).isCloseTo(rateInMsg, Percentage.withPercentage(10)));
    }
}
