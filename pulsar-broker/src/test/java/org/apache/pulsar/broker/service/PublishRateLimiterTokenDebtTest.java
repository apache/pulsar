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
import static org.mockito.Mockito.doReturn;
import com.google.common.io.Closer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.qos.AsyncTokenBucket;
import org.apache.pulsar.broker.qos.MonotonicClock;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.util.FutureUtil;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Uses a dedicated broker because its rate-limiter clock must be replaced before broker startup.
 * Freezing the clock of a shared broker would affect unrelated tests.
 */
@Test(groups = "broker")
public class PublishRateLimiterTokenDebtTest extends BrokerTestBase {
    private AtomicLong manualClock;

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        manualClock = new AtomicLong(TimeUnit.SECONDS.toNanos(100));
        super.baseSetup();
    }

    @Override
    protected void beforePulsarStart(PulsarService pulsar) throws Exception {
        super.beforePulsarStart(pulsar);
        MonotonicClock clock = manualClock::get;
        doReturn(clock).when(pulsar).getMonotonicClock();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testOverconsumedTokensDelaySubsequentSocketReads() throws Exception {
        int rateInMsg = 500;
        int producerCount = 5;
        int burstSize = 600;
        String topicName = "persistent://" + newTopicName();
        List<Producer<Integer>> producers = new ArrayList<>();
        PublishRateLimiterImpl limiter =
                (PublishRateLimiterImpl) pulsar.getBrokerService().getBrokerPublishRateLimiter();
        try (Closer clientCloser = Closer.create();
             Consumer<Integer> consumer = pulsarClient.newConsumer(Schema.INT32).topic(topicName)
                .subscriptionName("sub").subscribe()) {
            try {
                for (int i = 0; i < producerCount; i++) {
                    @SuppressWarnings("deprecation")
                    PulsarClient client = PulsarClient.builder().serviceUrl(pulsar.getBrokerServiceUrl())
                            .ioThreads(1).connectionsPerBroker(1).statsInterval(0, TimeUnit.SECONDS).build();
                    clientCloser.register(client);
                    producers.add(client.newProducer(Schema.INT32).topic(topicName).producerName("producer-" + i)
                            .enableBatching(true).batchingMaxMessages(burstSize)
                            .batchingMaxPublishDelay(1, TimeUnit.HOURS).create());
                }
                Topic topic = pulsar.getBrokerService().getTopicReference(topicName).orElseThrow();
                List<ServerCnx> connections = topic.getProducers().values().stream()
                        .map(p -> (ServerCnx) p.getCnx()).toList();
                assertThat(connections).hasSize(producerCount);
                admin.brokers().updateDynamicConfiguration("brokerPublisherThrottlingMaxMessageRate",
                        String.valueOf(rateInMsg));
                Awaitility.await().untilAsserted(() -> {
                    assertThat(limiter.getTokenBucketOnMessage()).isNotNull();
                    assertThat(limiter.getTokenBucketOnMessage().getRate()).isEqualTo(rateInMsg);
                });

                // A single batch can overconsume the remaining tokens. Freeze the clock so a short-lived
                // burst cannot be mistaken for disabled throttling or hidden by token replenishment.
                List<CompletableFuture<MessageId>> firstBurst = new ArrayList<>();
                for (int i = 0; i < producerCount; i++) {
                    Producer<Integer> producer = producers.get(i);
                    for (int j = 0; j < burstSize; j++) {
                        firstBurst.add(producer.sendAsync(i * burstSize + j));
                    }
                }
                FutureUtil.waitForAll(firstBurst).get(15, TimeUnit.SECONDS);
                AsyncTokenBucket bucket = limiter.getTokenBucketOnMessage();
                assertThat(bucket.getTokens()).isEqualTo(rateInMsg - (long) producerCount * burstSize);
                assertThat(bucket.calculateThrottlingDuration()).isGreaterThan(TimeUnit.SECONDS.toNanos(5));
                for (ServerCnx connection : connections) {
                    connection.ctx().executor().submit(() -> {
                        assertThat(connection.ctx().channel().config().isAutoRead()).isFalse();
                        assertThat(connection.getThrottleTracker().throttledCount()).isPositive();
                    }).get(5, TimeUnit.SECONDS);
                }

                List<CompletableFuture<MessageId>> nextBurst = new ArrayList<>();
                for (int i = 0; i < producerCount; i++) {
                    nextBurst.add(producers.get(i).sendAsync(producerCount * burstSize + i));
                    producers.get(i).flushAsync();
                }
                // One second of replenishment cannot repay five seconds of token debt. Future socket reads
                // must remain paused even though the publishes which exhausted the budget have completed.
                manualClock.addAndGet(TimeUnit.SECONDS.toNanos(1));
                assertThat(bucket.calculateThrottlingDuration()).isGreaterThan(TimeUnit.SECONDS.toNanos(4));
                Awaitility.await().during(Duration.ofMillis(200)).atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                    assertThat(nextBurst).allMatch(future -> !future.isDone());
                    for (ServerCnx connection : connections) {
                        assertThat(connection.ctx().channel().config().isAutoRead()).isFalse();
                    }
                });

                // Repay the debt and let the existing scheduled unthrottle resume the connections.
                manualClock.addAndGet(TimeUnit.SECONDS.toNanos(10));
                FutureUtil.waitForAll(nextBurst).get(15, TimeUnit.SECONDS);
                for (ServerCnx connection : connections) {
                    Awaitility.await().untilAsserted(() ->
                            assertThat(connection.ctx().channel().config().isAutoRead()).isTrue());
                }

                Set<Integer> received = new HashSet<>();
                int messageCount = producerCount * (burstSize + 1);
                for (int i = 0; i < messageCount; i++) {
                    Message<Integer> message = consumer.receive(5, TimeUnit.SECONDS);
                    assertThat(message).isNotNull();
                    assertThat(received.add(message.getValue())).as("each published value is delivered once").isTrue();
                    consumer.acknowledge(message);
                }
                assertThat(received).hasSize(messageCount);
            } finally {
                // Failed assertions must not leave producers waiting on the deliberately frozen clock during close.
                limiter.update(new PublishRate(0, 0));
            }
        }
    }
}
